/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.PublicApi;

/**
 * Indicates the type of field value
 *
 * @opensearch.api
 */
@PublicApi(since = "2.18.0")
public enum FieldValueType {
    DOC_VALUES,
    STORED
}
