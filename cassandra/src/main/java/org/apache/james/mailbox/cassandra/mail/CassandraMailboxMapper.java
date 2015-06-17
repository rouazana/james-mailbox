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

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
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

    private Session session;
    private int maxRetry;

    public CassandraMailboxMapper(Session session, int maxRetry) {
        this.session = session;
        this.maxRetry = maxRetry;
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
        SimpleMailbox<UUID> mailbox = new SimpleMailbox<UUID>(new MailboxPath(row.getString(NAMESPACE), row.getString(USER), row.getString(NAME)), row.getLong(UIDVALIDITY));
        mailbox.setMailboxId(row.getUUID(ID));
        mailbox.setACL(new CassandraACLMapper(mailbox, session, maxRetry).getACL());
        return mailbox;
    }

    @Override
    public List<Mailbox<UUID>> findMailboxWithPathLike(MailboxPath path) throws MailboxException {
        final String regexWithUser = ".*" + path.getNamespace() + ".*" + path.getUser() + ".*" + path.getName() + ".*";
        final String regexWithoutUser = ".*" + path.getNamespace() + ".*null.*" + path.getName() + ".*";
        Builder<Mailbox<UUID>> result = ImmutableList.<Mailbox<UUID>> builder();
        for (Row row : session.execute(select(FIELDS).from(TABLE_NAME))) {
            if (row.getString(PATH).matches(regexWithUser) || row.getString(PATH).matches(regexWithoutUser)) {
                result.add(mailbox(row));
            }
        }
        return result.build();
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
                .value(NAMESPACE, mailbox.getNamespace())
                .value(UIDVALIDITY, mailbox.getUidValidity())
                .value(USER, mailbox.getUser())
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
        final String regexWithUser = ".*" + mailbox.getNamespace() + ".*" + mailbox.getUser() + ".*" + mailbox.getName() + delimiter + ".*";
        final String regexWithoutUser = ".*" + mailbox.getNamespace() + ".*null.*" + mailbox.getName() + delimiter + ".*";
        for (Row row : session.execute(select(PATH).from(TABLE_NAME))) {
            if (row.getString(PATH).matches(regexWithUser) || row.getString(PATH).matches(regexWithoutUser)) {
                return true;
            }
        }
        return false;
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
