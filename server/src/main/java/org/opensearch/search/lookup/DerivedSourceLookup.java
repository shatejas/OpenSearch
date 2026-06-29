/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.lookup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.mapper.RootObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A SourceLookup that lazily derives field values from doc-values instead of loading stored _source.
 *
 * @opensearch.internal
 */
public class DerivedSourceLookup extends SourceLookup {

    private static final Logger log = LogManager.getLogger(DerivedSourceLookup.class);

    private final RootObjectMapper rootObjectMapper;
    private LeafReader reader;
    private int docId = -1;
    private final LazyDerivedMap derivedFields = new LazyDerivedMap();

    private class LazyDerivedMap extends HashMap<String, Object> {
        @Override
        public Object get(Object key) {
            String field = key.toString();
            return computeIfAbsent(field, f -> {
                try {
                    log.trace("deriving field [{}] for doc [{}]", f, docId);
                    return rootObjectMapper.deriveFieldValue(f, reader, docId);
                } catch (IOException e) {
                    throw new OpenSearchParseException("failed to derive field [" + f + "]", e);
                }
            });
        }
    }

    public DerivedSourceLookup(RootObjectMapper rootObjectMapper) {
        this.rootObjectMapper = rootObjectMapper;
    }

    @Override
    public void setSegmentAndDocument(LeafReaderContext context, int docId) {
        if (this.reader == context.reader() && this.docId == docId) {
            return;
        }
        this.reader = context.reader();
        this.docId = docId;
        this.derivedFields.clear();
    }

    @Override
    public Object get(Object key) {
        return derivedFields.get(key);
    }

    @Override
    public List<Object> extractRawValues(String path) {
        Object value = get(path);
        if (value == null) {
            return Collections.emptyList();
        }
        if (value instanceof List) {
            return (List<Object>) value;
        }
        return Collections.singletonList(value);
    }

    @Override
    public Object extractValue(String path, Object nullValue) {
        Object value = get(path);
        return value != null ? value : nullValue;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public Map<String, Object> loadSourceIfNeeded() {
        return derivedFields;
    }

    @Override
    public Map<String, Object> source() {
        return derivedFields;
    }

    @Override
    public BytesReference internalSourceRef() {
        throw new UnsupportedOperationException("DerivedSourceLookup does not have stored source bytes");
    }

    @Override
    public void setSource(BytesReference source) {
        throw new UnsupportedOperationException("DerivedSourceLookup does not accept stored source");
    }

    @Override
    public void setSource(Map<String, Object> source) {
        throw new UnsupportedOperationException("DerivedSourceLookup does not accept stored source");
    }
}
