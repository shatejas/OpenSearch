/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.contextaware;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CriteriaExtractor {

    public static Set<String> extractCriteria(QueryBuilder queryBuilder, String criteria) {
        if (criteria == null) {
            return Collections.emptySet();
        }

        if (queryBuilder instanceof TermQueryBuilder) {
            final TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
            if (termQueryBuilder.fieldName().equals(criteria)) {
                return Set.of(termQueryBuilder.value().toString());
            }
            return Collections.emptySet();
        }

        if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            List<QueryBuilder> filter = boolQueryBuilder.filter();
            Set<String> filterCriterias = new HashSet<>();
            for (QueryBuilder query : filter) {
                Set<String> criterias = extractCriteria(query, criteria);
                if (filterCriterias.isEmpty()) {
                    filterCriterias.addAll(criterias);
                } else {
                   return Collections.emptySet();
                }
            }

            if (!filterCriterias.isEmpty()) {
                return filterCriterias;
            }

            final Set<String> mustCriterias = new HashSet<>();
            List<QueryBuilder> mustClauses = boolQueryBuilder.must();
            for (QueryBuilder query : mustClauses) {
                Set<String> criterias = extractCriteria(query, criteria);
                if (mustCriterias.isEmpty()) {
                    mustCriterias.addAll(criterias);
                } else {
                    if (!(criterias.isEmpty() || mustCriterias.equals(criterias))) {
                        return Collections.emptySet();
                    }
                }
            }

            if (!mustCriterias.isEmpty()) {
                return mustCriterias;
            }

            final Set<String> shouldCriterias = new HashSet<>();
            String minshouldMatchString = boolQueryBuilder.minimumShouldMatch();
            int minimumShouldMatch = minshouldMatchString == null ? 1 : Integer.parseInt(minshouldMatchString);
            if (mustCriterias.isEmpty() && minimumShouldMatch > 0) {
                final List<QueryBuilder> shouldClauses = boolQueryBuilder.should();
                for (QueryBuilder query : shouldClauses) {
                    Set<String> shouldCriteria = extractCriteria(query, criteria);
                    if (shouldCriteria.isEmpty()) {
                        return Collections.emptySet();
                    }
                    shouldCriterias.addAll(shouldCriteria);
                }
            }

            if (!shouldCriterias.isEmpty()) {
                return shouldCriterias;
            }
        }

        return Collections.emptySet();
    }
}
