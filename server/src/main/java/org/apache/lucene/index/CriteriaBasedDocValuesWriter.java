/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;

import java.io.IOException;

public class CriteriaBasedDocValuesWriter extends DocValuesConsumer {

    private final DocValuesConsumer delegate;
    private final String criteria;

    public CriteriaBasedDocValuesWriter(DocValuesConsumer delegate, String criteria) throws IOException {
        this.delegate = delegate;
        this.criteria = criteria;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
        if (field.name.equals("_seq_no") && criteria != null) {
            field.putAttribute("criteria", criteria);
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addBinaryField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        super.merge(mergeState);
        mergeState.segmentInfo.putAttribute("criteria", mergeState.mergeFieldInfos.fieldInfo("_seq_no").getAttribute("criteria"));
        mergeState.segmentInfo.putAttribute("merge", "true");
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
