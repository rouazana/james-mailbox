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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ElasticSearchIndexerTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private Node node;
    private ElasticSearchIndexer testee;

    @Before
    public void setup() throws IOException {
        node = EmbeddedElasticSearch.provideNode(temporaryFolder);
        testee = new ElasticSearchIndexer(node);
    }
    
    @After
    public void tearDown() {
        EmbeddedElasticSearch.shutDown(node);
    }
    
    @Test
    public void indexMessageShouldWork() throws Exception {
        String messageId = "1";
        String content = "{\"message\": \"trying out Elasticsearch\"}";
        
        testee.indexMessage(messageId, content);
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        try (Client client = node.client()) {
            SearchResponse searchResponse = client.prepareSearch(ElasticSearchIndexer.MAILBOX_INDEX)
                    .setTypes(ElasticSearchIndexer.MESSAGE_TYPE)
                    .setQuery(QueryBuilders.matchQuery("message", "trying"))
                    .get();
            assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(1);
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void indexMessageShouldThrowWhenJsonIsNull() throws InterruptedException {
        testee.indexMessage("1", null);
    }
    
    @Test
    public void updateMessage() throws Exception {
        String messageId = "1";
        String content = "{\"message\": \"trying out Elasticsearch\",\"field\":\"Should be unchanged\"}";

        testee.indexMessage(messageId, content);
        EmbeddedElasticSearch.awaitForElasticSearch(node);

        testee.updateMessage(messageId, "{\"message\": \"mastering out Elasticsearch\"}");
        EmbeddedElasticSearch.awaitForElasticSearch(node);

        try (Client client = node.client()) {
            SearchResponse searchResponse = client.prepareSearch(ElasticSearchIndexer.MAILBOX_INDEX)
                .setTypes(ElasticSearchIndexer.MESSAGE_TYPE)
                .setQuery(QueryBuilders.matchQuery("message", "mastering"))
                .get();
            assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(1);
        }

        try (Client client = node.client()) {
            SearchResponse searchResponse = client.prepareSearch(ElasticSearchIndexer.MAILBOX_INDEX)
                .setTypes(ElasticSearchIndexer.MESSAGE_TYPE)
                .setQuery(QueryBuilders.matchQuery("field", "unchanged"))
                .get();
            assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(1);
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void updateMessageShouldThrowWhenJsonIsNull() throws InterruptedException {
        String messageId = "1:2";
        String content = "{\"message\": \"trying out Elasticsearch\"}";

        testee.indexMessage(messageId, content);
        EmbeddedElasticSearch.awaitForElasticSearch(node);

        testee.updateMessage("1", null);
    }
    
    @Test
    public void deleteAllWithIdStarting() throws Exception {
        String messageId = "1:2";
        String content = "{\"message\": \"trying out Elasticsearch\"}";

        testee.indexMessage(messageId, content);
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        testee.deleteAllWithIdStarting("1:");
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        try (Client client = node.client()) {
            SearchResponse searchResponse = client.prepareSearch(ElasticSearchIndexer.MAILBOX_INDEX)
                    .setTypes(ElasticSearchIndexer.MESSAGE_TYPE)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .get();
            assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(0);
        }
    }
    
    @Test
    public void deleteAllWithIdStartingWhenMultipleMessages() throws Exception {
        String messageId = "1:2";
        String content = "{\"message\": \"trying out Elasticsearch\"}";
        
        testee.indexMessage(messageId, content);
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        String messageId2 = "1:2";
        String content2 = "{\"message\": \"trying out Elasticsearch 2\"}";
        
        testee.indexMessage(messageId2, content2);
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        String messageId3 = "2:3";
        String content3 = "{\"message\": \"trying out Elasticsearch 3\"}";
        
        testee.indexMessage(messageId3, content3);
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        testee.deleteAllWithIdStarting("1:");
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        try (Client client = node.client()) {
            SearchResponse searchResponse = client.prepareSearch(ElasticSearchIndexer.MAILBOX_INDEX)
                    .setTypes(ElasticSearchIndexer.MESSAGE_TYPE)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .get();
            assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(1);
        }
    }
    
    @Test
    public void deleteMessage() throws Exception {
        String messageId = "1:2";
        String content = "{\"message\": \"trying out Elasticsearch\"}";

        testee.indexMessage(messageId, content);
        EmbeddedElasticSearch.awaitForElasticSearch(node);

        testee.deleteMessage(messageId);
        EmbeddedElasticSearch.awaitForElasticSearch(node);
        
        try (Client client = node.client()) {
            SearchResponse searchResponse = client.prepareSearch(ElasticSearchIndexer.MAILBOX_INDEX)
                    .setTypes(ElasticSearchIndexer.MESSAGE_TYPE)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .get();
            assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(0);
        }
    }

}
