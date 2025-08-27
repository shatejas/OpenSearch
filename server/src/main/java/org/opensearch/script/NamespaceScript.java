/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;

/**
 * A script used by the Ingest Script Processor.
 *
 * @opensearch.internal
 */
@PublicApi(since = "2.19.0")
public abstract class NamespaceScript {

    public static final String[] PARAMETERS = { "ctx" };

    public static final ScriptContext<NamespaceScript.Factory> CONTEXT = new ScriptContext<>(
        "namespace",
        NamespaceScript.Factory.class,
        200,
        TimeValue.timeValueMillis(10000),
        ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple()
    );

    public abstract String execute(Map<String, Object> ctx);

    public interface Factory {
        NamespaceScript newInstance();
    }
}
