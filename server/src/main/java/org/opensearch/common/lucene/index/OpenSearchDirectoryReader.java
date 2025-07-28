/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * OpenSearch internal per shard / index information like the shard ID.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class OpenSearchDirectoryReader extends FilterDirectoryReader {

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    private final Map<String, OpenSearchDirectoryReader> criteriaBasedReaders = new HashMap<>();

    private final DelegatingCacheHelper  delegatingCacheHelper;

    private OpenSearchDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper, ShardId shardId)
        throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.delegatingCacheHelper = new DelegatingCacheHelper(in.getReaderCacheHelper());
        if (!(in instanceof CriteriaBasedReader)) {
            warmUpCriteriaBasedReader(in.leaves());
        }
    }

    private void warmUpCriteriaBasedReader(final List<LeafReaderContext> leaves) throws IOException {
        // TODO: index setting to figure out if it we need to look for criterias?
        Map<String, List<LeafReader>> criteriaBasedLeafReaders = new HashMap<>();
        for (LeafReaderContext leafReaderContext : leaves) {
            final String criteria = Lucene.segmentReader(leafReaderContext.reader())
                .getSegmentInfo().info.getAttribute("criteria");
            if (criteria != null) {
                criteriaBasedLeafReaders.computeIfAbsent(criteria, k -> new ArrayList<>()).add(leafReaderContext.reader());
            } else {
                //TODO: not sure what to do
            }
        }

        //Important to create readers here and not at runtime to make sure cachkey does not change
        for (Map.Entry<String, List<LeafReader>> entry : criteriaBasedLeafReaders.entrySet()) {
            final String criteria = entry.getKey();
            final CriteriaBasedReader reader =
                new CriteriaBasedReader(directory, entry.getValue().toArray(new LeafReader[0]), in.getReaderCacheHelper());
            criteriaBasedReaders.put(criteria,
                new OpenSearchDirectoryReader
                    (reader, new FilterDirectoryReader.SubReaderWrapper() {
                        @Override
                        public LeafReader wrap(LeafReader reader) {
                            return reader;
                        }
                    }, shardId()));
        }
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    public OpenSearchDirectoryReader getCriteriaBasedReader(Set<String> criteria) throws IOException {

        List<LeafReader> leafReaders = new ArrayList<>();
        for (String criteriaKey : criteria) {
            OpenSearchDirectoryReader criteriaBasedReader = criteriaBasedReaders.get(criteriaKey);
            if (criteriaBasedReader != null) {
                leafReaders.addAll(criteriaBasedReader.leaves().stream()
                    .map(LeafReaderContext::reader)
                    .toList());
            }

        }
        if (leafReaders.isEmpty()) {
            // Return empty reader. need to give a constant cache key
            return new OpenSearchDirectoryReader(new CriteriaBasedReader(directory, emptyList().toArray(new LeafReader[0]), in.getReaderCacheHelper()),
                new FilterDirectoryReader.SubReaderWrapper() {
                    @Override
                    public LeafReader wrap(LeafReader reader) {
                        return reader;
                    }
                },
                shardId());
        }
        return new OpenSearchDirectoryReader(new CriteriaBasedReader(directory, leafReaders.toArray(new LeafReader[0]), in.getReaderCacheHelper()),
            wrapper,
            shardId());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return this.delegatingCacheHelper;
    }

    public DelegatingCacheHelper getDelegatingCacheHelper() {
        return this.delegatingCacheHelper;
    }

    /**
     * Wraps existing IndexReader cache helper which internally provides a way to wrap CacheKey.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.13.0")
    public class DelegatingCacheHelper implements CacheHelper {
        private final CacheHelper cacheHelper;
        private final DelegatingCacheKey serializableCacheKey;

        DelegatingCacheHelper(CacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            this.serializableCacheKey = new DelegatingCacheKey(Optional.ofNullable(cacheHelper).map(key -> getKey()).orElse(null));
        }

        @Override
        public CacheKey getKey() {
            return this.cacheHelper.getKey();
        }

        public DelegatingCacheKey getDelegatingCacheKey() {
            return this.serializableCacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            this.cacheHelper.addClosedListener(listener);
        }
    }

    /**
     *  Wraps internal IndexReader.CacheKey and attaches a uniqueId to it which can be eventually be used instead of
     *  object itself for serialization purposes.
     *
     *  @opensearch.api
     */
    @PublicApi(since = "2.13.0")
    public class DelegatingCacheKey {
        private final CacheKey cacheKey;
        private final String uniqueId;

        DelegatingCacheKey(CacheKey cacheKey) {
            this.cacheKey = cacheKey;
            this.uniqueId = UUID.randomUUID().toString();
        }

        public CacheKey getCacheKey() {
            return this.cacheKey;
        }

        public String getId() {
            return uniqueId;
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new OpenSearchDirectoryReader(in, wrapper, shardId);
    }

    /**
     * Wraps the given reader in a {@link OpenSearchDirectoryReader} as
     * well as all it's sub-readers in {@link OpenSearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the opensearch internal reader wrappers.
     */
    public static OpenSearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return new OpenSearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId);
    }

    private static final class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;

        SubReaderWrapper(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new OpenSearchLeafReader(reader, shardId);
        }
    }

    /**
     * Adds the given listener to the provided directory reader. The reader
     * must contain an {@link OpenSearchDirectoryReader} in it's hierarchy
     * otherwise we can't safely install the listener.
     *
     * @throws IllegalArgumentException if the reader doesn't contain an
     *     {@link OpenSearchDirectoryReader} in it's hierarchy
     */
    @SuppressForbidden(reason = "This is the only sane way to add a ReaderClosedListener")
    public static void addReaderCloseListener(DirectoryReader reader, IndexReader.ClosedListener listener) {
        OpenSearchDirectoryReader openSearchDirectoryReader = getOpenSearchDirectoryReader(reader);
        if (openSearchDirectoryReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an OpenSearchDirectoryReader/OpenSearchLeafReader"
            );
        }
        IndexReader.CacheHelper cacheHelper = openSearchDirectoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + openSearchDirectoryReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    /**
     * Tries to unwrap the given reader until the first
     * {@link OpenSearchDirectoryReader} instance is found or {@code null}
     * if no instance is found.
     */
    public static OpenSearchDirectoryReader getOpenSearchDirectoryReader(DirectoryReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof OpenSearchDirectoryReader) {
                return (OpenSearchDirectoryReader) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of OpenSearchLeafReader. This can cause us to miss the shardId.
                return getOpenSearchDirectoryReader(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }

    @ExperimentalApi
    public class CriteriaBasedReader extends DirectoryReader {

        private CacheHelper cacheHelper;
        /**
         * Constructs a {@code BaseCompositeReader} on the given subReaders.
         *
         * @param subReaders       the wrapped sub-readers. This array is returned by {@link
         *                         #getSequentialSubReaders} and used to resolve the correct subreader for docID-based
         *                         methods. <b>Please note:</b> This array is <b>not</b> cloned and not protected for
         *                         modification, the subclass is responsible to do this.
         */
        protected CriteriaBasedReader(Directory directory, LeafReader[] subReaders, CacheHelper cacheHelper) throws IOException {
            super(directory, subReaders, null);
            // Create a new object here to have a different cache key uuid than parent
            this.cacheHelper = cacheHelper;
        }

        @Override
        protected void doClose() throws IOException {
            //NO op - Parent director should be closed not
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return this.cacheHelper;
        }

        @Override
        protected DirectoryReader doOpenIfChanged() throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support doOpenIfChanged() operation");
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support doOpenIfChanged(IndexCommit) operation");
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) operation");
        }

        @Override
        public long getVersion() {
            throw new UnsupportedOperationException("Criteria based reader does not support getVersion() operation");
        }

        @Override
        public boolean isCurrent() throws IOException {
            // Should inherit parent directory value. If parent is not current, sub directory reader won't be current
            return OpenSearchDirectoryReader.this.isCurrent();
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support getIndexCommit() operation");
        }
    }
}
