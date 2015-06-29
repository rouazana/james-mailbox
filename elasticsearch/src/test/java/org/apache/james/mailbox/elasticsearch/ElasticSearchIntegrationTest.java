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

import java.text.SimpleDateFormat;

import javax.mail.Flags;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.acl.SimpleGroupMembershipResolver;
import org.apache.james.mailbox.acl.UnionMailboxACLResolver;
import org.apache.james.mailbox.elasticsearch.events.ElasticSearchListeningMessageSearchIndex;
import org.apache.james.mailbox.elasticsearch.json.MessageToElasticSearchJson;
import org.apache.james.mailbox.elasticsearch.query.CriterionConverter;
import org.apache.james.mailbox.elasticsearch.query.QueryConverter;
import org.apache.james.mailbox.elasticsearch.search.ElasticSearchSearcher;
import org.apache.james.mailbox.elasticsearch.utils.TestingClientProvider;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.inmemory.InMemoryId;
import org.apache.james.mailbox.inmemory.InMemoryMailboxSessionMapperFactory;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.model.SearchQuery;
import org.apache.james.mailbox.store.JVMMailboxPathLocker;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.MockAuthenticator;
import org.apache.james.mailbox.store.StoreMailboxManager;
import org.apache.james.mailbox.store.StoreMessageManager;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


public class ElasticSearchIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchIntegrationTest.class);

    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private EmbeddedElasticSearch embeddedElasticSearch = new EmbeddedElasticSearch(temporaryFolder);

    @Rule
    public RuleChain chain = RuleChain.outerRule(temporaryFolder).around(embeddedElasticSearch);
    
    private StoreMailboxManager<InMemoryId> storeMailboxManager;
    private ElasticSearchListeningMessageSearchIndex<InMemoryId> elasticSearchListeningMessageSearchIndex;
    private Mailbox<InMemoryId> mailbox;
    private SimpleDateFormat format;
    private MailboxSession session;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

        initializeMailboxManager();

        session = storeMailboxManager.createSystemSession("benwa", LOGGER);

        storeMailboxManager.createMailbox(new MailboxPath("#private", "benwa", "INBOX"), session);
        StoreMessageManager<InMemoryId> messageManager = (StoreMessageManager<InMemoryId>) storeMailboxManager.getMailbox(new MailboxPath("#private", "benwa", "INBOX"), session);
        mailbox = messageManager.getMailboxEntity();

        // sentDate: Wed, 3 Jun 2015 09:05:46 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/spamMail.eml"),
            format.parse("2014/01/02 00:00:00.000"),
            session,
            true,
            new Flags(Flags.Flag.DELETED));
        //sentDate: Thu, 4 Jun 2015 09:23:37 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/mail1.eml"),
            format.parse("2014/02/02 00:00:00.000"),
            session,
            true,
            new Flags(Flags.Flag.ANSWERED));
        //sentDate: Thu, 4 Jun 2015 09:27:37 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/mail2.eml"),
            format.parse("2014/03/02 00:00:00.000"),
            session,
            true,
            new Flags(Flags.Flag.DRAFT));
        //sentDate: Tue, 2 Jun 2015 08:16:19 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/mail3.eml"),
            format.parse("2014/05/02 00:00:00.000"),
            session,
            true,
            new Flags(Flags.Flag.RECENT));
        //sentDate: Fri, 15 May 2015 06:35:59 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/mail4.eml"),
            format.parse("2014/04/02 00:00:00.000"),
            session,
            true,
            new Flags(Flags.Flag.FLAGGED));
        //sentDate: Wed, 03 Jun 2015 19:14:32 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/pgpSignedMail.eml"),
            format.parse("2014/06/02 00:00:00.000"),
            session,
            true,
            new Flags(Flags.Flag.SEEN));
        //sentDate: Thu, 04 Jun 2015 07:36:08 +0000
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/htmlMail.eml"),
            format.parse("2014/07/02 00:00:00.000"),
            session,
            false,
            new Flags());
        //sentDate: Thu, 4 Jun 2015 06:08:41 +0200
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/mail.eml"),
            format.parse("2014/08/02 00:00:00.000"),
            session,
            true,
            new Flags("Hello"));
        //sentDate: Tue, 2 Jun 2015 12:00:55 +0200
        messageManager.appendMessage(
            ClassLoader.getSystemResourceAsStream("documents/frnog.eml"),
            format.parse("2014/09/02 00:00:00.000"),
            session,
            true,
            new Flags("Hello you"));

        embeddedElasticSearch.awaitForElasticSearch();
    }

    private void initializeMailboxManager() throws Exception {
        ClientProvider clientProvider = NodeMappingFactory.applyMapping(
            IndexCreationFactory.createIndex(new TestingClientProvider(embeddedElasticSearch.getNode()))
        );
        MailboxSessionMapperFactory<InMemoryId> mapperFactory = new InMemoryMailboxSessionMapperFactory();
        elasticSearchListeningMessageSearchIndex = new ElasticSearchListeningMessageSearchIndex<InMemoryId>(mapperFactory,
            new ElasticSearchIndexer(clientProvider),
            new ElasticSearchSearcher<InMemoryId>(clientProvider, new QueryConverter(new CriterionConverter())),
            new MessageToElasticSearchJson());
        storeMailboxManager = new StoreMailboxManager<>(
            mapperFactory,
            new MockAuthenticator(),
            new JVMMailboxPathLocker(),
            new UnionMailboxACLResolver(),
            new SimpleGroupMembershipResolver());
        storeMailboxManager.setMessageSearchIndex(elasticSearchListeningMessageSearchIndex);
        storeMailboxManager.init();
    }

    @Test
    public void allShouldReturnAllUids() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.all());
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void bodyContainsShouldReturnUidOfMessageContainingTheGivenText() throws MailboxException {
        /*
        Only mail4.eml contains word MAILET-94
         */
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.bodyContains("MAILET-94"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(5L);
    }

    @Test
    public void bodyContainsShouldReturnUidOfMessageContainingTheApproximativeText() throws MailboxException {
        /*
        mail1.eml contains words created AND summary
        mail.eml contains created and thus matches the query with a low score
         */
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.bodyContains("created summary"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(2L, 8L);
    }

    @Test
    public void flagIsSetShouldReturnUidOfMessageMarkedAsDeletedWhenUsedWithFlagDeleted() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet(Flags.Flag.DELETED));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L);
    }

    @Test
    public void flagIsSetShouldReturnUidOfMessageMarkedAsAnsweredWhenUsedWithFlagAnswered() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet(Flags.Flag.ANSWERED));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(2L);
    }

    @Test
    public void flagIsSetShouldReturnUidOfMessageMarkedAsDraftWhenUsedWithFlagDraft() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet(Flags.Flag.DRAFT));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(3L);
    }

    @Test
    public void flagIsSetShouldReturnUidOfMessageMarkedAsRecentWhenUsedWithFlagRecent() throws MailboxException {
        // Only message 7 is not marked as RECENT
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet(Flags.Flag.RECENT));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 3L, 4L, 5L, 6L, 8L, 9L);
    }

    @Test
    public void flagIsSetShouldReturnUidOfMessageMarkedAsFlaggedWhenUsedWithFlagFlagged() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet(Flags.Flag.FLAGGED));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(5L);
    }

    @Test
    public void flagIsSetShouldReturnUidOfMessageMarkedAsSeenWhenUsedWithFlagSeen() throws MailboxException {
        // Only message 6 is marked as read.
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet(Flags.Flag.SEEN));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(6L);
    }

    @Ignore("This test will fail as Memory mailbox has no support for user defined flags. This test will return two message instead of one => mapping issue")
    @Test
    public void flagIsSetShouldReturnUidsOfMessageContainingAGivenUserFlag() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet("Hello"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(8L);
    }

    @Ignore("This test will fail as Memory mailbox has no support for user defined flags. This test will return two message instead of one => mapping issue")
    @Test
    public void userFlagsShouldBeMatchedExactly() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsSet("Hello bonjour"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(8L);
    }

    @Test
    public void flagIsUnSetShouldReturnUidOfMessageNotMarkedAsDeletedWhenUsedWithFlagDeleted() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet(Flags.Flag.DELETED));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void flagIsUnSetShouldReturnUidOfMessageNotMarkedAsAnsweredWhenUsedWithFlagAnswered() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet(Flags.Flag.ANSWERED));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void flagIsUnSetShouldReturnUidOfMessageNotMarkedAsDraftWhenUsedWithFlagDraft() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet(Flags.Flag.DRAFT));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void flagIsUnSetShouldReturnUidOfMessageNotMarkedAsRecentWhenUsedWithFlagRecent() throws MailboxException {
        // Only message 7 is not marked as RECENT
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet(Flags.Flag.RECENT));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(7L);
    }

    @Test
    public void flagIsUnSetShouldReturnUidOfMessageNotMarkedAsFlaggedWhenUsedWithFlagFlagged() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet(Flags.Flag.FLAGGED));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 3L, 4L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void flagIsUnSetShouldReturnUidOfMessageNotMarkedAsSeendWhenUsedWithFlagSeen() throws MailboxException {
        // Only message 6 is marked as read.
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet(Flags.Flag.SEEN));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 3L, 4L, 5L, 7L, 8L, 9L);
    }

    @Ignore("This test will fail as Memory mailbox has no support for user defined flags. This test will return two message instead of one => mapping issue")
    @Test
    public void flagIsUnSetShouldReturnUidsOfMessageNotContainingAGivenUserFlag() throws MailboxException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.flagIsUnSet("Hello"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(8L);
    }

    @Test
    public void internalDateAfterShouldReturnMessagesAfterAGivenDate() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.internalDateAfter(format.parse("2014/07/02 00:00:00.000"), SearchQuery.DateResolution.Day));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(7L, 8L, 9L);
    }

    @Test
    public void internalDateBeforeShouldReturnMessagesBeforeAGivenDate() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.internalDateBefore(format.parse("2014/02/02 00:00:00.000"), SearchQuery.DateResolution.Day));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L);
    }

    @Test
    public void internalDateOnShouldReturnMessagesOfTheGivenDate() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.internalDateOn(format.parse("2014/03/02 00:00:00.000"), SearchQuery.DateResolution.Day));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(3L);
    }

    @Test
    public void modSeqEqualsShouldReturnUidsOfMessageHavingAGivenModSeq() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.modSeqEquals(2L));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(2L);
    }

    @Test
    public void modSeqGreaterThanShouldReturnUidsOfMessageHavingAGreaterModSeq() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.modSeqGreaterThan(7L));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(7L, 8L, 9L);
    }

    @Test
    public void modSeqLessThanShouldReturnUidsOfMessageHavingAGreaterModSeq() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.modSeqLessThan(3L));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 3L);
    }

    @Test
    public void sizeGreaterThanShouldReturnUidsOfMessageExceedingTheSpecifiedSize() throws Exception {
        // Only message 7 is over 10 KB
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.sizeGreaterThan(10000L));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(7L);
    }

    @Test
    public void sizeLessThanShouldReturnUidsOfMessageNotExceedingTheSpecifiedSize() throws Exception {
        // Only message 2 3 4 5 9 are under 5 KB
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.sizeLessThan(5000L));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(2L, 3L, 4L, 5L, 9L);
    }

    @Test
    public void headerContainsShouldReturnUidsOfMessageHavingThisHeaderWithTheSpecifiedValue() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.headerContains("Precedence", "list"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 6L, 8L, 9L);
    }

    @Test
    public void headerExistsShouldReturnUidsOfMessageHavingThisHeader() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.headerExists("Precedence"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 2L, 3L, 4L, 5L, 6L, 8L, 9L);
    }

    @Test
    public void addressShouldReturnUidHavingRightExpeditorWhenFromIsSpecified() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.address(SearchQuery.AddressType.From, "murari.ksr@gmail.com"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(8L);
    }

    @Test
    public void addressShouldReturnUidHavingRightRecipientWhenToIsSpecified() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.address(SearchQuery.AddressType.To, "root@listes.minet.net"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L);
    }

    @Test
    public void uidShouldreturnExistingUidsOnTheGivenRanges() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        SearchQuery.NumericRange[] numericRanges = {new SearchQuery.NumericRange(2L, 4L), new SearchQuery.NumericRange(6L, 7L)};
        searchQuery.andCriteria(SearchQuery.uid(numericRanges));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(2L, 3L, 4L, 6L, 7L);
    }

    @Test
    public void youShouldBeAbleToSpecifySeveralCriterionOnASingleQuery() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.headerExists("Precedence"));
        searchQuery.andCriteria(SearchQuery.modSeqGreaterThan(6L));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(6L, 8L, 9L);
    }

    @Test
    public void andShouldReturnResultsMatchingBothRequests() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(
            SearchQuery.and(
                SearchQuery.headerExists("Precedence"),
                SearchQuery.modSeqGreaterThan(6L)));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(6L, 8L, 9L);
    }

    @Test
    public void notShouldReturnResultsThatDoNotMatchAQuery() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(
            SearchQuery.not(SearchQuery.headerExists("Precedence")));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(7L);
    }

    @Test
    public void sortShouldOrderMessages() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.all());
        searchQuery.setSorts(Lists.newArrayList(new SearchQuery.Sort(SearchQuery.Sort.SortClause.Arrival)));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsExactly(1L, 2L, 3L, 5L, 4L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void revertSortingShouldReturnElementsInAReversedOrder() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.all());
        searchQuery.setSorts(Lists.newArrayList(new SearchQuery.Sort(SearchQuery.Sort.SortClause.Arrival, true)));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsExactly(9L, 8L, 7L, 6L, 4L, 5L, 3L, 2L, 1L);
    }

    @Test
    public void headerDateAfterShouldWork() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.headerDateAfter("sentDate", format.parse("2015/06/04 11:00:00.000"), SearchQuery.DateResolution.Second));
        searchQuery.setSorts(Lists.newArrayList(new SearchQuery.Sort(SearchQuery.Sort.SortClause.Arrival, true)));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(3L, 2L);
    }

    @Test
    public void headerDateBeforeShouldWork() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.headerDateBefore("sentDate", format.parse("2015/06/01 00:00:00.000"), SearchQuery.DateResolution.Day));
        searchQuery.setSorts(Lists.newArrayList(new SearchQuery.Sort(SearchQuery.Sort.SortClause.Arrival, true)));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(5L);
    }

    @Test
    public void headerDateOnShouldWork() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.headerDateOn("sentDate", format.parse("2015/06/02 08:00:00.000"), SearchQuery.DateResolution.Day));
        searchQuery.setSorts(Lists.newArrayList(new SearchQuery.Sort(SearchQuery.Sort.SortClause.Arrival, true)));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(4L, 9L);
    }

    @Test
    public void mailsContainsShouldIncludeMailHavingAttachmentsMatchingTheRequest() throws Exception {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.andCriteria(SearchQuery.mailContains("root mailing list"));
        assertThat(elasticSearchListeningMessageSearchIndex.search(session, mailbox, searchQuery))
            .containsOnly(1L, 6L);
    }

}
