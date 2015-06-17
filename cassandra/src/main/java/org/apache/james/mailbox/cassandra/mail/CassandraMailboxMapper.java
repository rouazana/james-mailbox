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

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.apache.james.mailbox.cassandra.table.CassandraMailboxTable.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import org.apache.james.mailbox.cassandra.CassandraTypesProvider;
import org.apache.james.mailbox.cassandra.CassandraTypesProvider.TYPE;
import org.apache.james.mailbox.cassandra.table.CassandraMailboxTable;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.MailboxACL;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.model.Mailbox;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMailbox;

/**
 * Data access management for mailbox.
 */
public class CassandraMailboxMapper implements MailboxMapper<UUID> {

    public static final String WILDCARD = "%";
    private final Session session;
    private final int maxRetry;
    private final CassandraTypesProvider typesProvider;

    public CassandraMailboxMapper(Session session, CassandraTypesProvider typesProvider, int maxRetry) {
        this.session = session;
        this.maxRetry = maxRetry;
        this.typesProvider = typesProvider;
    }

    @Override
    public void delete(Mailbox<UUID> mailbox) throws MailboxException {
        session.execute(QueryBuilder.delete().from(TABLE_NAME).where(eq(ID, mailbox.getMailboxId())));
    }

    @Override
    public Mailbox<UUID> findMailboxByPath(MailboxPath path) throws MailboxException {
        ResultSet resultSet = session.execute(select(FIELDS).from(TABLE_NAME).where(eq(PATH, path.toString())));
        if (resultSet.isExhausted()) {
            throw new MailboxNotFoundException(path);
        } else {
            return mailbox(resultSet.one());
        }
    }

    private SimpleMailbox<UUID> mailbox(Row row) {
        SimpleMailbox<UUID> mailbox = new SimpleMailbox<>(
            new MailboxPath(
                row.getUDTValue(MAILBOX_BASE).getString(MailboxBase.NAMESPACE),
                row.getUDTValue(MAILBOX_BASE).getString(MailboxBase.USER),
                row.getString(NAME)),
            row.getLong(UIDVALIDITY));
        mailbox.setMailboxId(row.getUUID(ID));
        mailbox.setACL(new CassandraACLMapper(mailbox, session, maxRetry).getACL());
        return mailbox;
    }

    @Override
    public List<Mailbox<UUID>> findMailboxWithPathLike(MailboxPath path) throws MailboxException {
        Pattern regex = Pattern.compile(constructEscapedRegexForMailboxNameMatching(path));
        return getMailboxFilteredByNamespaceAndUserStream(path.getNamespace(), path.getUser())
            .filter((row) -> regex.matcher(row.getString(NAME)).matches())
            .map(this::mailbox)
            .collect(Collectors.toList());
    }

    private String constructEscapedRegexForMailboxNameMatching(MailboxPath path) {
        return Collections
            .list(new StringTokenizer(path.getName(), WILDCARD, true))
            .stream()
            .map((token) -> {
                    if (token.equals(WILDCARD)) {
                        return ".*";
                    } else {
                        return Pattern.quote((String) token);
                    }
                }
            ).collect(Collectors.joining());
    }

    @Override
    public void save(Mailbox<UUID> mailbox) throws MailboxException {
        Preconditions.checkArgument(mailbox instanceof SimpleMailbox);
        SimpleMailbox<UUID> cassandraMailbox = (SimpleMailbox<UUID>) mailbox;
        if (cassandraMailbox.getMailboxId() == null) {
            cassandraMailbox.setMailboxId(UUIDs.timeBased());
        }
        upsertMailbox(cassandraMailbox);
    }

    private void upsertMailbox(SimpleMailbox<UUID> mailbox) throws MailboxException {
        session.execute(
            insertInto(TABLE_NAME)
                .value(ID, mailbox.getMailboxId())
                .value(NAME, mailbox.getName())
                .value(UIDVALIDITY, mailbox.getUidValidity())
                .value(MAILBOX_BASE, typesProvider.getDefinedUserType(TYPE.MailboxBase)
                    .newValue()
                    .setString(MailboxBase.NAMESPACE, mailbox.getNamespace())
                    .setString(MailboxBase.USER, mailbox.getUser()))
                .value(PATH, path(mailbox).toString())
        );
    }

    private MailboxPath path(Mailbox<?> mailbox) {
        return new MailboxPath(mailbox.getNamespace(), mailbox.getUser(), mailbox.getName());
    }

    @Override
    public void endRequest() {
        // Do nothing
    }

    @Override
    public boolean hasChildren(Mailbox<UUID> mailbox, char delimiter) {
        final Pattern regex = Pattern.compile(Pattern.quote( mailbox.getName() + String.valueOf(delimiter)) + ".*");
        return getMailboxFilteredByNamespaceAndUserStream(mailbox.getNamespace(), mailbox.getUser())
            .anyMatch((row) -> regex.matcher(row.getString(NAME)).matches());
    }

    private Stream<Row> getMailboxFilteredByNamespaceAndUserStream (String namespace, String user) {
        return StreamSupport.stream(session.execute(
                select(FIELDS)
                    .from(TABLE_NAME)
                    .where(eq(MAILBOX_BASE, typesProvider.getDefinedUserType(TYPE.MailboxBase)
                        .newValue()
                        .setString(MailboxBase.NAMESPACE, namespace)
                        .setString(MailboxBase.USER, user))))
                .spliterator(),
            true);
    }

    @Override
    public List<Mailbox<UUID>> list() throws MailboxException {
        Builder<Mailbox<UUID>> result = ImmutableList.<Mailbox<UUID>> builder();
        for (Row row : session.execute(select(FIELDS).from(TABLE_NAME))) {
            result.add(mailbox(row));
        }
        return result.build();
    }

    @Override
    public <T> T execute(Transaction<T> transaction) throws MailboxException {
        return transaction.run();
    }

    @Override
    public void updateACL(Mailbox<UUID> mailbox, MailboxACL.MailboxACLCommand mailboxACLCommand) throws MailboxException {
        new CassandraACLMapper(mailbox, session, maxRetry).updateACL(mailboxACLCommand);
    }

}
