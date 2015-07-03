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

package org.apache.james.mailbox.cassandra.mail;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.decr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.BODY_CONTENT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.BODY_OCTECTS;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.BODY_START_OCTET;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.FIELDS;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.FLAG_VERSION;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.FULL_CONTENT_OCTETS;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.HEADER_CONTENT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.IMAP_UID;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.INTERNAL_DATE;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.MAILBOX_ID;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.MOD_SEQ;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.PROPERTIES;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.TABLE_NAME;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.TEXTUAL_LINE_COUNT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.ANSWERED;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.DELETED;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.DRAFT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.FLAGGED;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.JAVAX_MAIL_FLAG;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.RECENT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.SEEN;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.USER;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.mail.Flags;
import javax.mail.Flags.Flag;
import javax.mail.util.SharedByteArrayInputStream;

import org.apache.james.mailbox.FlagsBuilder;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.cassandra.CassandraConstants;
import org.apache.james.mailbox.cassandra.CassandraId;
import org.apache.james.mailbox.cassandra.CassandraTypesProvider;
import org.apache.james.mailbox.cassandra.CassandraTypesProvider.TYPE;
import org.apache.james.mailbox.cassandra.table.CassandraMailboxCountersTable;
import org.apache.james.mailbox.cassandra.table.CassandraMessageTable;
import org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Properties;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.MessageMetaData;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.UpdatedFlags;
import org.apache.james.mailbox.store.FlagsUpdateCalculator;
import org.apache.james.mailbox.store.SimpleMessageMetaData;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.ModSeqProvider;
import org.apache.james.mailbox.store.mail.UidProvider;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.mail.model.impl.PropertyBuilder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;
import org.apache.james.mailbox.store.mail.model.impl.SimpleProperty;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;

public class CassandraMessageMapper implements MessageMapper<CassandraId> {

    private final Session session;
    private final ModSeqProvider<CassandraId> modSeqProvider;
    private final MailboxSession mailboxSession;
    private final UidProvider<CassandraId> uidProvider;
    private final CassandraTypesProvider typesProvider;

    private int maxRetries;

    private final static int DEFAULT_MAX_RETRIES = 10000;

    public CassandraMessageMapper(Session session, UidProvider<CassandraId> uidProvider, ModSeqProvider<CassandraId> modSeqProvider, CassandraTypesProvider typesProvider) {
        this(session, uidProvider, modSeqProvider, null, DEFAULT_MAX_RETRIES, typesProvider);
    }

    public CassandraMessageMapper(Session session, UidProvider<CassandraId> uidProvider, ModSeqProvider<CassandraId> modSeqProvider, MailboxSession mailboxSession, CassandraTypesProvider typesProvider) {
        this(session, uidProvider, modSeqProvider, mailboxSession, DEFAULT_MAX_RETRIES, typesProvider);
    }

    public CassandraMessageMapper(Session session, UidProvider<CassandraId> uidProvider, ModSeqProvider<CassandraId> modSeqProvider, MailboxSession mailboxSession, int maxRetries, CassandraTypesProvider typesProvider) {
        this.session = session;
        this.uidProvider = uidProvider;
        this.modSeqProvider = modSeqProvider;
        this.mailboxSession = mailboxSession;
        this.maxRetries = maxRetries;
        this.typesProvider = typesProvider;
    }

    @Override
    public long countMessagesInMailbox(Mailbox<CassandraId> mailbox) throws MailboxException {
        ResultSet results = session.execute(
            select(CassandraMailboxCountersTable.COUNT)
                .from(CassandraMailboxCountersTable.TABLE_NAME)
                .where(eq(CassandraMailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId().asUuid())));
        return results.isExhausted() ? 0 : results.one().getLong(CassandraMailboxCountersTable.COUNT);
    }

    @Override
    public long countUnseenMessagesInMailbox(Mailbox<CassandraId> mailbox) throws MailboxException {
        ResultSet results = session.execute(
            select(CassandraMailboxCountersTable.UNSEEN)
                .from(CassandraMailboxCountersTable.TABLE_NAME)
                .where(eq(CassandraMailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId().asUuid())));
        if (!results.isExhausted()) {
            Row row = results.one();
            if (row.getColumnDefinitions().contains(CassandraMailboxCountersTable.UNSEEN)) {
                return row.getLong(CassandraMailboxCountersTable.UNSEEN);
            }
        }
        return 0;
    }

    @Override
    public void delete(Mailbox<CassandraId> mailbox, Message<CassandraId> message) throws MailboxException {
        session.execute(
            QueryBuilder.delete()
                .from(TABLE_NAME)
                .where(eq(MAILBOX_ID, mailbox.getMailboxId().asUuid()))
                .and(eq(IMAP_UID, message.getUid())));
        decrementCount(mailbox);
        if (!message.isSeen()) {
            decrementUnseen(mailbox);
        }
    }

    private void decrementCount(Mailbox<CassandraId> mailbox) {
        updateMailbox(mailbox, decr(CassandraMailboxCountersTable.COUNT));
    }

    private void incrementCount(Mailbox<CassandraId> mailbox) {
        updateMailbox(mailbox, incr(CassandraMailboxCountersTable.COUNT));
    }

    private void decrementUnseen(Mailbox<CassandraId> mailbox) {
        updateMailbox(mailbox, decr(CassandraMailboxCountersTable.UNSEEN));
    }

    private void incrementUnseen(Mailbox<CassandraId> mailbox) {
        updateMailbox(mailbox, incr(CassandraMailboxCountersTable.UNSEEN));
    }

    private void updateMailbox(Mailbox<CassandraId> mailbox, Assignment operation) {
        session.execute(
            update(CassandraMailboxCountersTable.TABLE_NAME)
                .with(operation)
                .where(eq(CassandraMailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId().asUuid())));
    }

    @Override
    public Iterator<Message<CassandraId>> findInMailbox(Mailbox<CassandraId> mailbox, MessageRange set, FetchType ftype, int max) throws MailboxException {
        return convertToStream(session.execute(buildQuery(mailbox, set)))
            .map(this::message)
            .iterator();
    }

    private byte[] getFullContent(Row row) {
        byte[] headerContent = new byte[row.getBytes(HEADER_CONTENT).remaining()];
        byte[] bodyContent = new byte[row.getBytes(BODY_CONTENT).remaining()];
        row.getBytes(HEADER_CONTENT).get(headerContent);
        row.getBytes(BODY_CONTENT).get(bodyContent);
        return Bytes.concat(headerContent, bodyContent);
    }

    private Flags getFlags(Row row) {
        return Arrays.stream(CassandraMessageTable.Flag.ALL)
            .filter(row::getBool)
            .map(JAVAX_MAIL_FLAG::get)
            .reduce(
                new Flags(),
                (flags, flag) -> {
                    flags.add(flag);
                    return flags;
                }, (flags1, flags2) -> {
                    flags1.add(flags2);
                    return flags1;
                });
    }

    private PropertyBuilder getPropertyBuilder(Row row) {
        PropertyBuilder property = new PropertyBuilder(
            row.getList(PROPERTIES, UDTValue.class).stream()
                .map(x -> new SimpleProperty(x.getString(Properties.NAMESPACE), x.getString(Properties.NAME), x.getString(Properties.VALUE)))
                .collect(Collectors.toList()));
        property.setTextualLineCount(row.getLong(TEXTUAL_LINE_COUNT));
        return property;
    }

    private Message<CassandraId> message(Row row) {
        SimpleMessage<CassandraId> message =
            new SimpleMessage<CassandraId>(
                row.getDate(INTERNAL_DATE),
                row.getInt(FULL_CONTENT_OCTETS),
                row.getInt(BODY_START_OCTET),
                new SharedByteArrayInputStream(getFullContent(row)),
                getFlags(row),
                getPropertyBuilder(row),
                CassandraId.of(row.getUUID(MAILBOX_ID)));
        message.setUid(row.getLong(IMAP_UID));
        return message;
    }

    private Where buildQuery(Mailbox<CassandraId> mailbox, MessageRange set) {
        final MessageRange.Type type = set.getType();
        switch (type) {
        case ALL:
            return selectAll(mailbox);
        case FROM:
            return selectFrom(mailbox, set.getUidFrom());
        case RANGE:
            return selectRange(mailbox, set.getUidFrom(), set.getUidTo());
        case ONE:
            return selectMessage(mailbox, set.getUidFrom());
        }
        throw new UnsupportedOperationException();
    }

    private Where selectAll(Mailbox<CassandraId> mailbox) {
        return select(FIELDS)
            .from(TABLE_NAME)
            .where(eq(MAILBOX_ID, mailbox.getMailboxId().asUuid()));
    }

    private Where selectFrom(Mailbox<CassandraId> mailbox, long uid) {
        return select(FIELDS)
            .from(TABLE_NAME)
            .where(eq(MAILBOX_ID, mailbox.getMailboxId().asUuid()))
            .and(gte(IMAP_UID, uid));
    }

    private Where selectRange(Mailbox<CassandraId> mailbox, long from, long to) {
        return select(FIELDS)
            .from(TABLE_NAME)
            .where(eq(MAILBOX_ID, mailbox.getMailboxId().asUuid()))
            .and(gte(IMAP_UID, from))
            .and(lte(IMAP_UID, to));
    }

    private Where selectMessage(Mailbox<CassandraId> mailbox, long uid) {
        return select(FIELDS)
            .from(TABLE_NAME)
            .where(eq(MAILBOX_ID, mailbox.getMailboxId().asUuid()))
            .and(eq(IMAP_UID, uid));
    }

    @Override
    public List<Long> findRecentMessageUidsInMailbox(Mailbox<CassandraId> mailbox) throws MailboxException {
        return convertToStream(session.execute(selectAll(mailbox).and((eq(RECENT, true)))))
            .map((row) -> row.getLong(IMAP_UID))
            .sorted()
            .collect(Collectors.toList());
    }

    @Override
    public Long findFirstUnseenMessageUid(Mailbox<CassandraId> mailbox) throws MailboxException {
        ResultSet rows = session.execute(selectAll(mailbox).orderBy(asc(IMAP_UID)));
        for (Row row : rows) {
            if (!row.getBool(SEEN)) {
                return row.getLong(IMAP_UID);
            }
        }
        return null;
    }

    @Override
    public Map<Long, MessageMetaData> expungeMarkedForDeletionInMailbox(final Mailbox<CassandraId> mailbox, MessageRange set) throws MailboxException {
        ImmutableMap.Builder<Long, MessageMetaData> deletedMessages = ImmutableMap.builder();
        ResultSet messages = session.execute(buildQuery(mailbox, set));
        for (Row row : messages) {
            if (row.getBool(DELETED)) {
                Message<CassandraId> message = message(row);
                delete(mailbox, message);
                deletedMessages.put(message.getUid(), new SimpleMessageMetaData(message));
            }
        }
        return deletedMessages.build();
    }

    @Override
    public MessageMetaData move(Mailbox<CassandraId> mailbox, Message<CassandraId> original) throws MailboxException {
        throw new UnsupportedOperationException("Not implemented - see https://issues.apache.org/jira/browse/IMAP-370");
    }

    @Override
    public void endRequest() {
        // Do nothing
    }

    @Override
    public long getHighestModSeq(Mailbox<CassandraId> mailbox) throws MailboxException {
        return modSeqProvider.highestModSeq(mailboxSession, mailbox);
    }

    @Override
    public <T> T execute(Transaction<T> transaction) throws MailboxException {
        return transaction.run();
    }

    @Override
    public MessageMetaData add(Mailbox<CassandraId> mailbox, Message<CassandraId> message) throws MailboxException {
        message.setUid(uidProvider.nextUid(mailboxSession, mailbox));
        message.setModSeq(modSeqProvider.nextModSeq(mailboxSession, mailbox));
        MessageMetaData messageMetaData = save(mailbox, message);
        if (!message.isSeen()) {
            incrementUnseen(mailbox);
        }
        incrementCount(mailbox);
        return messageMetaData;
    }

    private MessageMetaData save(Mailbox<CassandraId> mailbox, Message<CassandraId> message) throws MailboxException {
        try {
            Insert query = insertInto(TABLE_NAME)
                .value(MAILBOX_ID, mailbox.getMailboxId().asUuid())
                .value(IMAP_UID, message.getUid())
                .value(MOD_SEQ, message.getModSeq())
                .value(INTERNAL_DATE, message.getInternalDate())
                .value(BODY_START_OCTET, message.getFullContentOctets() - message.getBodyOctets())
                .value(FULL_CONTENT_OCTETS, message.getFullContentOctets())
                .value(BODY_OCTECTS, message.getBodyOctets())
                .value(ANSWERED, message.isAnswered())
                .value(DELETED, message.isDeleted())
                .value(DRAFT, message.isDraft())
                .value(FLAGGED, message.isFlagged())
                .value(RECENT, message.isRecent())
                .value(SEEN, message.isSeen())
                .value(USER, message.createFlags().contains(Flag.USER))
                .value(BODY_CONTENT, bindMarker())
                .value(HEADER_CONTENT, bindMarker())
                .value(PROPERTIES, message.getProperties().stream()
                    .map(x -> typesProvider.getDefinedUserType(TYPE.Property)
                        .newValue()
                        .setString(Properties.NAMESPACE, x.getNamespace())
                        .setString(Properties.NAME, x.getLocalName())
                        .setString(Properties.VALUE, x.getValue()))
                    .collect(Collectors.toList()))
                .value(TEXTUAL_LINE_COUNT, message.getTextualLineCount())
                .value(FLAG_VERSION, 0);
            PreparedStatement preparedStatement = session.prepare(query.toString());


            BoundStatement boundStatement = preparedStatement.bind(toByteBuffer(message.getBodyContent()), toByteBuffer(message.getHeaderContent()));
            session.execute(boundStatement);
            return new SimpleMessageMetaData(message);
        } catch (IOException e) {
            throw new MailboxException("Error saving mail", e);
        }
    }

    private boolean conditionalSave(Mailbox<CassandraId> mailbox, Message<CassandraId> message, long flagVersion) throws MailboxException {
        ResultSet resultSet = session.execute(
            update(TABLE_NAME)
                .with(set(ANSWERED, message.isAnswered()))
                .and(set(DELETED, message.isDeleted()))
                .and(set(DRAFT, message.isDraft()))
                .and(set(FLAGGED, message.isFlagged()))
                .and(set(RECENT, message.isRecent()))
                .and(set(SEEN, message.isSeen()))
                .and(set(USER, message.createFlags().contains(Flag.USER)))
                .and(set(FLAG_VERSION, flagVersion + 1))
                .where(eq(IMAP_UID, message.getUid()))
                .and(eq(MAILBOX_ID, message.getMailboxId().asUuid()))
                .onlyIf(eq(FLAG_VERSION, flagVersion))
        );
        return resultSet.one().getBool(CassandraConstants.LIGHTWEIGHT_TRANSACTION_APPLIED);
    }

    private ByteBuffer toByteBuffer(InputStream stream) throws IOException {
        return ByteBuffer.wrap(ByteStreams.toByteArray(stream));
    }

    @Override
    public Iterator<UpdatedFlags> updateFlags(Mailbox<CassandraId> mailbox, FlagsUpdateCalculator flagsUpdateCalculator, MessageRange set) throws MailboxException {
        ImmutableList.Builder<UpdatedFlags> result = ImmutableList.builder();
        for (Row row : session.execute(buildQuery(mailbox, set))) {
            updateMessage(mailbox, flagsUpdateCalculator, result, row);
        }
        return result.build().iterator();
    }

    private void updateMessage(Mailbox<CassandraId> mailbox, FlagsUpdateCalculator flagsUpdateCalculator, ImmutableList.Builder<UpdatedFlags> result, Row row) throws MailboxException {
        // Get the message and basic information about it
        Message<CassandraId> message = message(row);
        long flagVersion = row.getLong(FLAG_VERSION);
        long uid = message.getUid();
        // update flags
        Flags originFlags = message.createFlags();
        Flags updatedFlags = flagsUpdateCalculator.buildNewFlags(originFlags);
        message.setFlags(updatedFlags);
        // Update the ModSeq
        long previousModSeq = message.getModSeq();
        long modSeq = modSeqProvider.nextModSeq(mailboxSession, mailbox);
        message.setModSeq(modSeq);
        // Try a first update
        if(!conditionalSave(mailbox, message, flagVersion)) {
            int tries = 0;
            // It fails. Someone updated the flag before us.
            do {
                tries++;
                // Retrieve the message from uid
                Row newRow = findMessageByUid(mailbox, uid);
                if(newRow == null) {
                    // Someone deleted this result while we were doing other stuff
                    // Skip it
                    break;
                }
                message = message(newRow);
                flagVersion = newRow.getLong(FLAG_VERSION);
                // update flags
                originFlags = message.createFlags();
                updatedFlags = flagsUpdateCalculator.buildNewFlags(originFlags);
                message.setFlags(updatedFlags);
                // Update ModSeq
                if (previousModSeq != message.getModSeq()) {
                    // Here someone updated the ModSeq, so we can not used the previously generated value
                    previousModSeq = message.getModSeq();
                    modSeq = modSeqProvider.nextModSeq(mailboxSession, mailbox);
                }
                message.setModSeq(modSeq);
                // and retry
            } while (!conditionalSave(mailbox, message, flagVersion) && tries < maxRetries);
            if(tries == maxRetries) {
                throw new MailboxException("Max retries reached when asking an update of flags on message " + uid + " for mailbox " + mailbox.getMailboxId());
            }
        }
        manageUnseenMessageCounts(mailbox, originFlags, updatedFlags);
        result.add(new UpdatedFlags(message.getUid(), message.getModSeq(), originFlags, updatedFlags));
    }

    private void manageUnseenMessageCounts(Mailbox<CassandraId> mailbox, Flags oldFlags, Flags newFlags) {
        if (oldFlags.contains(Flag.SEEN) && !newFlags.contains(Flag.SEEN)) {
            incrementUnseen(mailbox);
        }
        if (!oldFlags.contains(Flag.SEEN) && newFlags.contains(Flag.SEEN)) {
            decrementUnseen(mailbox);
        }
    }

    private Row findMessageByUid(Mailbox<CassandraId> mailbox, long uid) {
        ResultSet resultSet = session.execute(selectMessage(mailbox, uid));
        if ( resultSet.isExhausted() ) {
            return null;
        }
        return resultSet.one();
    }

    @Override
    public MessageMetaData copy(Mailbox<CassandraId> mailbox, Message<CassandraId> original) throws MailboxException {

        original.setUid(uidProvider.nextUid(mailboxSession, mailbox));
        original.setModSeq(modSeqProvider.nextModSeq(mailboxSession, mailbox));
        incrementCount(mailbox);
        if(!original.isSeen()) {
            incrementUnseen(mailbox);
        }
        original.setFlags(new FlagsBuilder().add(original.createFlags()).add(Flag.RECENT).build());
        return save(mailbox, original);
    }

    @Override
    public long getLastUid(Mailbox<CassandraId> mailbox) throws MailboxException {
        return uidProvider.lastUid(mailboxSession, mailbox);
    }

    private Stream<Row> convertToStream(ResultSet resultSet) {
        return StreamSupport.stream(resultSet.spliterator(), true);
    }


}
