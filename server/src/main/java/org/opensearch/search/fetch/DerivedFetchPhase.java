package org.opensearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.TaskExecutor;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;import org.opensearch.search.fetch.subphase.DerivedFetchSourcePhase;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.Collections.emptyMap;

public class DerivedFetchPhase implements Fetch {

    private final FetchSubPhase[] fetchSubPhases;

    public DerivedFetchPhase(List<FetchSubPhase> fetchSubPhases) {
        List<FetchSubPhase> phases = new ArrayList<>(fetchSubPhases.size() + 1);
        for (FetchSubPhase phase : fetchSubPhases) {
            if (phase instanceof org.opensearch.search.fetch.subphase.FetchSourcePhase) {
                continue;
            }
            phases.add(phase);
        }
        phases.add(new DerivedFetchSourcePhase());
        this.fetchSubPhases = phases.toArray(new FetchSubPhase[0]);
    }

    @Override
    public void execute(SearchContext context, String profileDescription) {
        if (context.docIdsToLoadSize() == 0) {
            context.fetchResult()
                .hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
            return;
        }

        FetchPhase.DocIdToIndex[] docs = new FetchPhase.DocIdToIndex[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            docs[index] = new FetchPhase.DocIdToIndex(context.docIdsToLoad()[context.docIdsToLoadFrom() + index], index);
        }
        Arrays.sort(docs);

        List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        int[] segmentStarts = buildSegmentStarts(docs, leaves);

        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        FieldsVisitor fieldsVisitorTemplate = createStoredFieldsVisitor(context, storedToRequestedFields);

        FetchContext fetchContext = new FetchContext(context);
        SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];
        TaskExecutor taskExecutor = context.searcher().getTaskExecutor();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int seg = 0; seg < leaves.size(); seg++) {
            int start = segmentStarts[seg];
            int end = segmentStarts[seg + 1];
            if (start == end) continue;

            LeafReaderContext leafContext = leaves.get(seg);
            tasks.add(() -> {
                processSegment(context, fetchContext, leafContext, docs, start, end, fieldsVisitorTemplate, storedToRequestedFields, hits);
                return null;
            });
        }

        try {
            taskExecutor.invokeAll(tasks);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context.shardTarget(), "Error running parallel derived fetch phase", e);
        }

        context.fetchResult().hits(new SearchHits(hits, context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
    }

    private void processSegment(
        SearchContext context,
        FetchContext fetchContext,
        LeafReaderContext leafContext,
        FetchPhase.DocIdToIndex[] docs,
        int start,
        int end,
        FieldsVisitor fieldsVisitorTemplate,
        Map<String, Set<String>> storedToRequestedFields,
        SearchHit[] hits
    ) throws IOException {
        StoredFields storedFields = leafContext.reader().storedFields();
        List<Tuple<FetchSubPhaseProcessor, FetchSubPhase>> processors = getProcessors(context.shardTarget(), fetchContext);

        for (Tuple<FetchSubPhaseProcessor, FetchSubPhase> p : processors) {
            p.v1().setNextReader(leafContext);
        }

        FieldsVisitor fieldsVisitor;
        if (fieldsVisitorTemplate == null) {
            fieldsVisitor = null;
        } else if (fieldsVisitorTemplate instanceof CustomFieldsVisitor) {
            fieldsVisitor = new CustomFieldsVisitor(
                storedToRequestedFields.keySet(),
                false,
                context.hasFetchSourceContext() ? context.fetchSourceContext().includes() : null,
                context.hasFetchSourceContext() ? context.fetchSourceContext().excludes() : null
            );
        } else {
            fieldsVisitor = new FieldsVisitor(false);
        }

        if (fieldsVisitor != null) {
            storedFields.prefetch(docs[start].docId - leafContext.docBase);
        }

        for (int i = start; i < end; i++) {
            int docId = docs[i].docId;
            int subDocId = docId - leafContext.docBase;

            if (fieldsVisitor != null && i + 1 < end) {
                storedFields.prefetch(docs[i + 1].docId - leafContext.docBase);
            }

            SearchHit hit;
            if (fieldsVisitor == null) {
                hit = new SearchHit(docId, null, null, null);
            } else {
                fieldsVisitor.reset();
                storedFields.document(subDocId, fieldsVisitor);
                fieldsVisitor.postProcess(context::fieldType);

                String id = fieldsVisitor.id();
                if (fieldsVisitor.fields().isEmpty() == false) {
                    Map<String, DocumentField> docFields = new HashMap<>();
                    Map<String, DocumentField> metaFields = new HashMap<>();
                    fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
                    hit = new SearchHit(docId, id, docFields, metaFields);
                } else {
                    hit = new SearchHit(docId, id, emptyMap(), emptyMap());
                }
            }

            HitContext hitContext = new HitContext(hit, leafContext, subDocId, fetchContext.searchLookup().source());
            for (Tuple<FetchSubPhaseProcessor, FetchSubPhase> p : processors) {
                p.v1().process(hitContext);
            }
            hits[docs[i].index] = hit;
        }
    }

    private int[] buildSegmentStarts(FetchPhase.DocIdToIndex[] sortedDocs, List<LeafReaderContext> leaves) {
        int[] segmentStarts = new int[leaves.size() + 1];
        int leafIdx = 0;
        for (int docIdx = 0; docIdx < sortedDocs.length; docIdx++) {
            while (leafIdx < leaves.size() - 1 && sortedDocs[docIdx].docId >= leaves.get(leafIdx + 1).docBase) {
                leafIdx++;
                segmentStarts[leafIdx] = docIdx;
            }
        }
        for (int i = leafIdx + 1; i <= leaves.size(); i++) {
            segmentStarts[i] = sortedDocs.length;
        }
        return segmentStarts;
    }

    private static void fillDocAndMetaFields(
        SearchContext context,
        FieldsVisitor fieldsVisitor,
        Map<String, Set<String>> storedToRequestedFields,
        Map<String, DocumentField> docFields,
        Map<String, DocumentField> metaFields
    ) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            Set<String> requestedFields = storedToRequestedFields.get(storedField);
            if (requestedFields != null) {
                for (String fieldName : requestedFields) {
                    addField(context, fieldName, storedValues, docFields, metaFields);
                }
            } else {
                addField(context, storedField, storedValues, docFields, metaFields);
            }
        }
    }

    private static void addField(
        SearchContext context,
        String fieldName,
        List<Object> values,
        Map<String, DocumentField> docFields,
        Map<String, DocumentField> metaFields
    ) {
        if (context.mapperService().isMetadataField(fieldName)) {
            metaFields.put(fieldName, new DocumentField(fieldName, values));
        } else {
            docFields.put(fieldName, new DocumentField(fieldName, values));
        }
    }

    List<Tuple<FetchSubPhaseProcessor, FetchSubPhase>> getProcessors(SearchShardTarget target, FetchContext context) {
        try {
            List<Tuple<FetchSubPhaseProcessor, FetchSubPhase>> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(new Tuple<>(processor, fsp));
                }
            }
            return processors;
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(target, "Error building fetch sub-phases", e);
        }
    }

    protected FieldsVisitor createStoredFieldsVisitor(SearchContext context, Map<String, Set<String>> storedToRequestedFields) {
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                context.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
            }
            return new FieldsVisitor(false);
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            return null;
        } else {
            for (String fieldNameOrPattern : context.storedFieldsContext().fieldNames()) {
                // This is just to make sure we keep the context consistent with default fetch phase
                if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                    FetchSourceContext fetchSourceContext = context.hasFetchSourceContext()
                        ? context.fetchSourceContext()
                        : FetchSourceContext.FETCH_SOURCE;
                    context.fetchSourceContext(new FetchSourceContext(true, fetchSourceContext.includes(), fetchSourceContext.excludes()));
                    continue;
                }

                Collection<String> fieldNames = context.mapperService().simpleMatchToFullName(fieldNameOrPattern);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = context.fieldType(fieldName);
                    if (fieldType == null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        if (context.getObjectMapper(fieldName) != null) {
                            throw new IllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                        }
                    } else {
                        String storedField = fieldType.name();
                        Set<String> requestedFields = storedToRequestedFields.computeIfAbsent(storedField, key -> new HashSet<>());
                        requestedFields.add(fieldName);
                    }
                }
            }
            if (storedToRequestedFields.isEmpty()) {
                // empty list specified, default to disable _source if no explicit indication
                return new FieldsVisitor(false);
            } else {
                return new CustomFieldsVisitor(
                    storedToRequestedFields.keySet(),
                    false,
                    context.hasFetchSourceContext() ? context.fetchSourceContext().includes() : null,
                    context.hasFetchSourceContext() ? context.fetchSourceContext().excludes() : null
                );
            }
        }
    }
}
