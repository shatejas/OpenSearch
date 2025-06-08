/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.CriteriaBasedDocValuesWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * The default internal engine (can be overridden by plugins)
 *
 * @opensearch.internal
 */
public class CriteriaBasedDocValueFormat extends DocValuesFormat {

    private final DocValuesFormat delegate;
    static final String DATA_CODEC = "CriteriaBasedDocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "CriteriaBasedDocValuesMetadata";
    static final String META_EXTENSION = "dvm";
    private final String criteria;

    public CriteriaBasedDocValueFormat() {
        this(new Lucene90DocValuesFormat(), null);
    }

    public CriteriaBasedDocValueFormat(String criteria) {
        this(new Lucene90DocValuesFormat(), criteria);
    }

    /**
     * Creates a new docvalues format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using {@code PerFieldDocValuesFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     *
     */
    protected CriteriaBasedDocValueFormat(DocValuesFormat delegate, String criteria) {
        super(delegate.getName());
        this.delegate = delegate;
        this.criteria = criteria;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new CriteriaBasedDocValuesWriter(delegate.fieldsConsumer(state), criteria);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }
}
