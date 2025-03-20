 /*
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  */

 package org.opensearch.common.util;

 import org.opensearch.cluster.node.DiscoveryNode;
 import org.opensearch.cluster.service.ClusterService;
 import org.opensearch.common.settings.Settings;

 import java.util.HashMap;
 import java.util.Iterator;
 import java.util.Map;
 import java.util.Objects;
 import java.util.concurrent.ConcurrentHashMap;

 /**
  * Class provides helper methods to evaluate if a node attribute is enabled
  */
 public class NodeAttributesUtil {

     private static ClusterService clusterService;
     private static final Map<String, Boolean> attributesEnabled = new HashMap<>();

     public NodeAttributesUtil(ClusterService clusterService) {
         NodeAttributesUtil.clusterService = clusterService;
     }

     public static boolean isNodeAttributeEnabled(String attributeKey) {
         if (attributesEnabled.containsKey(attributeKey)) {
             return attributesEnabled.get(attributeKey);
         }

         if (Objects.isNull(clusterService)) {
             attributesEnabled.put(attributeKey, false);
             return false;
         }

         Map<String, DiscoveryNode> nodesMap = clusterService.state().getNodes().getNodes();

         if (nodesMap.isEmpty()) {
             attributesEnabled.put(attributeKey, false);
             return false;
         }

         Iterator<String> iter = nodesMap.keySet().iterator();
         while (iter.hasNext()) {
             String node = iter.next();
             DiscoveryNode nodeDiscovery = nodesMap.get(node);
             Map<String, String> nodeAttributes = nodeDiscovery.getAttributes();
             if (Boolean.parseBoolean(nodeAttributes.get(attributeKey)) == false) {
                 attributesEnabled.put(attributeKey, false);
                 return false;
             }
         }

         attributesEnabled.put(attributeKey, true);
         return true;
     }

     public static Map<String, Boolean> getAttributesEnabled() {
         return attributesEnabled;
     }

     public static Settings addNodeAttributeSettings(Settings settings, String key, boolean value) {
         if (settings.get(key) != null) {
             // Setting already exists, don't override
             return settings;
         }

         return Settings.builder()
             .put(settings)
             .put(key, String.valueOf(value))
             .build();
     }
 }
