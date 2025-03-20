/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

public class LuceneUtils {
    public static boolean hasChangesInRam(IndexWriter writer) {
        return writer.hasChangesInRam();
    }

}
