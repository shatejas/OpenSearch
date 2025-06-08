/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;

import java.io.IOException;

/**
 * The default internal engine (can be overridden by plugins)
 *
 * @opensearch.internal
 */
public class CriteriaBasedCodec extends FilterCodec {

    private final String criteria;
    public CriteriaBasedCodec() {
        super("CriteriaBasedCodec", new Lucene101Codec());
        criteria = null;
    }

    public CriteriaBasedCodec(Codec delegate, String criteria) {
        super("CriteriaBasedCodec", delegate);
        this.criteria = criteria;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                if (criteria != null) {
                    info.putAttribute("criteria", criteria);
                }

                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return new CriteriaBasedDocValueFormat(criteria);
    }
}
