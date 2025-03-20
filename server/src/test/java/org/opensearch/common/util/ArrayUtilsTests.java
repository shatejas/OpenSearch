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

package org.opensearch.common.util;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class ArrayUtilsTests extends OpenSearchTestCase {
    public void testBinarySearch() throws Exception {
        for (int j = 0; j < 100; j++) {
            int index = randomIntBetween(0, 9);
            double tolerance = randomDoubleBetween(0, 0.01, true);
            double lookForValue = frequently() ? -1 : Double.NaN; // sometimes we'll look for NaN
            double[] array = new double[10];
            for (int i = 0; i < array.length; i++) {
                double value;
                if (frequently()) {
                    value = randomDoubleBetween(0, 9, true);
                    array[i] = value + ((randomBoolean() ? 1 : -1) * randomDouble() * tolerance);

                } else {                    // sometimes we'll have NaN in the array
                    value = Double.NaN;
                    array[i] = value;
                }
                if (i == index && lookForValue < 0) {
                    lookForValue = value;
                }
            }
            Arrays.sort(array);

            // pick up all the indices that fall within the range of [lookForValue - tolerance, lookForValue + tolerance]
            // we need to do this, since we choose the values randomly and we might end up having multiple values in the
            // array that will match the looked for value with the random tolerance. In such cases, the binary search will
            // return the first one that will match.
            BitSet bitSet = new BitSet(10);
            for (int i = 0; i < array.length; i++) {
                if (Double.isNaN(lookForValue) && Double.isNaN(array[i])) {
                    bitSet.set(i);
                } else if ((array[i] >= lookForValue - tolerance) && (array[i] <= lookForValue + tolerance)) {
                    bitSet.set(i);
                }
            }

            int foundIndex = ArrayUtils.binarySearch(array, lookForValue, tolerance);

            if (bitSet.cardinality() == 0) {
                assertThat(foundIndex, is(-1));
            } else {
                assertThat(bitSet.get(foundIndex), is(true));
            }
        }
    }

    public void testConcat() {
        assertArrayEquals(new String[] { "a", "b", "c", "d" }, ArrayUtils.concat(new String[] { "a", "b" }, new String[] { "c", "d" }));
        int firstSize = randomIntBetween(0, 10);
        String[] first = new String[firstSize];
        ArrayList<String> sourceOfTruth = new ArrayList<>();
        for (int i = 0; i < firstSize; i++) {
            first[i] = randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
            sourceOfTruth.add(first[i]);
        }
        int secondSize = randomIntBetween(0, 10);
        String[] second = new String[secondSize];
        for (int i = 0; i < secondSize; i++) {
            second[i] = randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
            sourceOfTruth.add(second[i]);
        }
        assertArrayEquals(sourceOfTruth.toArray(new String[0]), ArrayUtils.concat(first, second));
    }

    public void testNodeAttributeUtils() throws InterruptedException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("dp_version", "true");
        attributes.put("remote_publication.repository.cs-remote-routing-table.settings.amazon_es_kms_key_arn", "false");
        attributes.put("admission_control_resource_tracker_feature_present", "true");
        attributes.put("search_reactivation_counter_feature_present", "false");
        attributes.put("remote_publication.repository.cs-remote-routing-table.settings.amazon_es_kms_enc_ctx",
            "true");
        attributes.put("remote_publication.repository.cs-remote-routing-table.settings.amazon_es_encryption", "false");
        attributes.put("cold_enabled", "false");
        attributes.put("zone", "true");
        attributes.put("global_cpu_usage_ac_supported", "true");
        List<String> keys = new ArrayList<>(attributes.keySet());

        DiscoveryNode localNode = new DiscoveryNode(
            "node",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        Settings settings = Settings.builder().put("node.name", "test").put("cluster.name", "ClusterServiceTests").build();
        ClusterService clusterService = new ClusterService(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            new TestThreadPool(ArrayUtilsTests.class.getSimpleName()),
            new ClusterManagerMetrics(NoopMetricsRegistry.INSTANCE)
        );
        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).clusterManagerNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getClusterManagerService()
            .setClusterStatePublisher(ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getClusterManagerService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        NodeAttributesUtil nodeAttributesUtil = new NodeAttributesUtil(clusterService);

        ArrayList<Thread> threadList = new ArrayList<>();

        for (int j = 0; j < 5000; j++) {
            threadList.add(new Thread(() -> {
                for (int i = 0; i < 500; i++) {
                    int l = randomIntBetween(0, keys.size() - 1);
                    boolean ans = NodeAttributesUtil.isNodeAttributeEnabled(keys.get(l));
                    if (ans != Boolean.parseBoolean(attributes.get(keys.get(l)))) {
                        System.out.println("Boolean values of " + keys.get(l) + " is different " + attributes.get(keys.get(l)) + " and " + NodeAttributesUtil.isNodeAttributeEnabled(keys.get(l)));
                    }
                }
            }));
        }

        for (Thread thread : threadList) {
            thread.start();
        }

        for (Thread thread : threadList) {
            thread.join();
        }

        Map<String, Boolean> attributesEnabled = NodeAttributesUtil.getAttributesEnabled();
        for (String key : attributesEnabled.keySet()) {
            if (attributesEnabled.get(key) != Boolean.parseBoolean(attributes.get(key))) {
                System.out.println("Did not matched" + key + ": " + attributesEnabled.get(key));
            }
        }

//        NodeAttributesUtil.getAttributesEnabled().forEach((key, value) -> System.out.println(key + " " + value));;
        clusterService.stop();

    }
}
