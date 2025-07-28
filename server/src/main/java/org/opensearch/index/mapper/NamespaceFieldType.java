/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;

@PublicApi(since = "3.1.0")
public class NamespaceFieldType extends MappedFieldType {

    private String fieldName;

    public NamespaceFieldType(String name) {
        super(name, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return null;
    }

    @Override
    public String typeName() {
        return "";
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return null;
    }
}
