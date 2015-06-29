/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package org.apache.james.mailbox.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

public class NodeProvider {

    private static final String GLOBAL_NETWORK_HOST_SETTING = "network.host";
    private static final String SCRIPT_DISABLE_DYNAMIC = "script.disable_dynamic";
    
    public static Node createNodeForClusterName(String clusterName, String masterHost) {
        return nodeBuilder()
                .clusterName(clusterName)
                .settings(ImmutableSettings.builder()
                        .put(GLOBAL_NETWORK_HOST_SETTING, masterHost)
                        .put(SCRIPT_DISABLE_DYNAMIC, false))
                .client(true)
                .node()
                .start();
    }
}
