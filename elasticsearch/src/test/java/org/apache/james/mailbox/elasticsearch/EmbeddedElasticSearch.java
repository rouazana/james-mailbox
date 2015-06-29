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



import static com.jayway.awaitility.Awaitility.await;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.awaitility.Duration;

public class EmbeddedElasticSearch {

    private static Logger LOGGER = LoggerFactory.getLogger(EmbeddedElasticSearch.class);

    public static Node provideNode(TemporaryFolder temporaryFolder) throws IOException {
        Node node = nodeBuilder().local(true)
            .settings(ImmutableSettings.builder()
                .put("path.data", temporaryFolder.newFolder().getAbsolutePath())
                .build())
            .node();
        node.start();
        awaitForElasticSearch(node);
        return node;
    }

    public static void shutDown(Node node) {
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        try (Client client = node.client()) {
            client.prepareDeleteByQuery(ElasticSearchIndexer.MAILBOX_INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        } catch (Exception e) {
            LOGGER.warn("Error while closing ES connection", e);
        }
        node.close();
    }

    /**
     * Sometimes, tests are too fast.
     * This method ensure that ElasticSearch service is up and indices are updated
     */
    public static void awaitForElasticSearch(Node node) {
        await().atMost(Duration.TEN_SECONDS).until(() -> flush(node));
    }

    private static boolean flush(Node node) {
        try (Client client = node.client()) {
            new FlushRequestBuilder(client.admin().indices()).setForce(true).get();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
