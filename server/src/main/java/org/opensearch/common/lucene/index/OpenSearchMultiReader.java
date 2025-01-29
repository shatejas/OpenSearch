/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@PublicApi(since = "2.19.0")
public class OpenSearchMultiReader extends MultiReader {
    private final Map<String, DirectoryReader> subReadersMap;
    private final Directory directory;
    private final ShardId shardId;

    public OpenSearchMultiReader(Directory directory, Map<String, DirectoryReader> subReadersMap, ShardId shardId)  throws IOException {
        super(subReadersMap.values().stream().filter(Objects::nonNull).toArray(IndexReader[]::new));
        this.subReadersMap = subReadersMap;
        this.directory = directory;
        this.shardId = shardId;
    }

//    public OpenSearchMultiReader(List<IndexReader> subReadersList, Directory directory, Set<String> criteriaList, ShardId shardId) throws IOException {
//        super(subReadersList.stream().filter(Objects::nonNull).toArray(IndexReader[]::new));
//        final IndexReader[] filteredSubReadersList = subReadersList.stream().filter(Objects::nonNull).toArray(IndexReader[]::new);
//        this.subReadersList = Arrays.stream(filteredSubReadersList).toList();
//        this.directory = directory;
//        this.criteriaList = criteriaList;
//        this.shardId = shardId;
//    }

//    public OpenSearchMultiReader(Directory directory, Set<String> criteriaList, ShardId shardId, IndexReader... subReaders) throws IOException {
//        super(Arrays.stream(subReaders).filter(Objects::nonNull).toArray(IndexReader[]::new));
//        this.subReadersList = Arrays.stream(subReaders).filter(Objects::nonNull).toList();
//        this.directory = directory;
//        this.criteriaList = criteriaList;
//        this.shardId = shardId;
//    }

    public List<OpenSearchDirectoryReader> getSubDirectoryReaderList() {
        final List<OpenSearchDirectoryReader> sequentialReader = new ArrayList<>();
        for (IndexReader indexReader: subReadersMap.values()) {
            assert indexReader instanceof OpenSearchDirectoryReader;
            sequentialReader.add((OpenSearchDirectoryReader) indexReader);
        }

        return sequentialReader;
    }

    public SegmentInfos getSegmentInfos() throws IOException {
        final List<SegmentInfos> segmentInfos = new ArrayList<>();
        for (IndexReader indexReader: subReadersMap.values()) {
            assert indexReader instanceof StandardDirectoryReader;
            segmentInfos.add(((StandardDirectoryReader) indexReader).getSegmentInfos());
        }

        return Lucene.combineSegmentInfos(segmentInfos, subReadersMap.keySet(), directory);
    }

    public Directory getDirectory() {
        return directory;
    }

    public Set<String> getCriteriaList() {
        return subReadersMap.keySet();
    }

    public Map<String, DirectoryReader> getSubReadersMap() {
        return subReadersMap;
    }

    public ShardId shardId() {
        return shardId;
    }

    public static void addReaderCloseListener(OpenSearchMultiReader reader, IndexReader.ClosedListener listener) throws IOException {
        OpenSearchMultiReader openSearchMultiReader = getOpenSearchMultiDirectoryReader(reader);
        if (openSearchMultiReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an OpenSearchDirectoryReader/OpenSearchLeafReader"
            );
        }
        IndexReader.CacheHelper cacheHelper = openSearchMultiReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + openSearchMultiReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    public static OpenSearchMultiReader getOpenSearchMultiDirectoryReader(MultiReader reader) throws IOException {
        if (reader instanceof OpenSearchMultiReader) {
            Map<String, DirectoryReader> subReadersMap = new HashMap<>();
            OpenSearchMultiReader openSearchMultiReader = (OpenSearchMultiReader) reader;
            for (Map.Entry<String, DirectoryReader> subReaderEntry: openSearchMultiReader.getSubReadersMap().entrySet()) {
                DirectoryReader subReader = subReaderEntry.getValue();
                OpenSearchDirectoryReader openSearchDirectoryReader = OpenSearchDirectoryReader.getOpenSearchDirectoryReader(subReader);
                if (openSearchDirectoryReader != null) {
                    subReadersMap.put(subReaderEntry.getKey(), openSearchDirectoryReader);
                }
            }

            if (subReadersMap.isEmpty()) {
                return null;
            } else {
                return new OpenSearchMultiReader(openSearchMultiReader.getDirectory(), subReadersMap, openSearchMultiReader.shardId);
            }
        }

        return null;
    }

    public boolean isCurrent() throws IOException {
        for (IndexReader indexReader: subReadersMap.values()) {
            assert indexReader instanceof OpenSearchDirectoryReader;
            if(!((OpenSearchDirectoryReader) indexReader).isCurrent()) {
                return false;
            }
        }

        return true;
    }

    public static OpenSearchMultiReader unwrap(OpenSearchMultiReader reader) throws IOException {
        Map<String, DirectoryReader> localSubReaderMap = new HashMap<>();
        Map<String, DirectoryReader> subReadersMap = reader.getSubReadersMap();
        for (String criteria: reader.getCriteriaList()) {
            DirectoryReader directoryReader = OpenSearchDirectoryReader.unwrap(subReadersMap.get(criteria));
            if (directoryReader != null) {
                localSubReaderMap.put(criteria, directoryReader);
            }
        }

        if (!subReadersMap.isEmpty()) {
            return new OpenSearchMultiReader(reader.getDirectory(), localSubReaderMap, reader.shardId);
        }

        return null;
    }
}
