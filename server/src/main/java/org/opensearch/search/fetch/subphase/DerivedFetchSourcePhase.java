/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.RootObjectMapper;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds _source for derived source indices by deriving only the fields matching includes/excludes.
 *
 * @opensearch.internal
 */
public final class DerivedFetchSourcePhase implements FetchSubPhase {

    private static final Logger logger = LogManager.getLogger(DerivedFetchSourcePhase.class);

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchSourceContext fetchSourceContext = fetchContext.fetchSourceContext();
        if (fetchSourceContext == null || fetchSourceContext.fetchSource() == false) {
            return null;
        }

        RootObjectMapper rootObjectMapper = fetchContext.mapperService().documentMapper().root();
        List<String> fieldsToDeriveList = resolveFields(rootObjectMapper, fetchSourceContext);
        String[] fieldsToDerive = fieldsToDeriveList.toArray(new String[0]);

        logger.debug("DerivedFetchSourcePhase: deriving {} fields out of {} total mapped fields",
            fieldsToDerive.length, countMappedFields(rootObjectMapper));

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {}

            @Override
            public void process(HitContext hitContext) throws IOException {
                SourceLookup sourceLookup = hitContext.sourceLookup();

                Map<String, Object> sourceMap = new LinkedHashMap<>(fieldsToDerive.length);
                for (String field : fieldsToDerive) {
                    Object value = sourceLookup.get(field);
                    if (value != null) {
                        sourceMap.put(field, value);
                    }
                }

                logger.trace("doc [{}]: derived {} fields for _source", hitContext.hit().docId(), sourceMap.size());

                BytesStreamOutput out = new BytesStreamOutput(1024);
                try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), out)) {
                    builder.map(sourceMap);
                }
                hitContext.hit().sourceRef(out.bytes());
            }
        };
    }

    private static int countMappedFields(RootObjectMapper rootObjectMapper) {
        int count = 0;
        Iterator<Mapper> mappers = rootObjectMapper.iterator();
        while (mappers.hasNext()) {
            mappers.next();
            count++;
        }
        return count;
    }

    private static List<String> resolveFields(RootObjectMapper rootObjectMapper, FetchSourceContext fetchSourceContext) {
        String[] includes = fetchSourceContext.includes();
        String[] excludes = fetchSourceContext.excludes();

        List<String> allFields = new ArrayList<>();
        Iterator<Mapper> mappers = rootObjectMapper.iterator();
        while (mappers.hasNext()) {
            allFields.add(mappers.next().simpleName());
        }

        if (includes.length == 0 && excludes.length == 0) {
            return allFields;
        }

        List<String> result = new ArrayList<>();
        for (String field : allFields) {
            if (includes.length > 0 && !Regex.simpleMatch(includes, field)) {
                continue;
            }
            if (excludes.length > 0 && Regex.simpleMatch(excludes, field)) {
                continue;
            }
            result.add(field);
        }
        return result;
    }
}