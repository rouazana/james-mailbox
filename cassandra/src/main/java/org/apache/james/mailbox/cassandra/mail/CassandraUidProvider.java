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

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageUidTable.MAILBOX_ID;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageUidTable.NEXT_UID;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageUidTable.TABLE_NAME;

import java.util.UUID;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.store.mail.UidProvider;
import org.apache.james.mailbox.store.mail.model.Mailbox;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraUidProvider implements UidProvider<UUID> {
    public final static int DEFAULT_MAX_RETRY = 100000;

    private Session session;
    private final int applied = 0;
    private int maxRetry;

    public CassandraUidProvider(Session session, int maxRetry) {
        this.session = session;
        this.maxRetry = maxRetry;
    }

    public CassandraUidProvider(Session session) {
        this(session, DEFAULT_MAX_RETRY);
    }

    @Override
    public long nextUid(MailboxSession mailboxSession, Mailbox<UUID> mailbox) throws MailboxException {
        long lastUid = lastUid(mailboxSession, mailbox);
        if (lastUid == 0) {
            ResultSet result = session.execute(insertInto(TABLE_NAME).value(NEXT_UID, ++lastUid).value(MAILBOX_ID, mailbox.getMailboxId()).ifNotExists());
            if(result.one().getBool(applied)) {
                return lastUid;
            }
        }
        int tries = 0;
        boolean isApplied;
        do {
            tries++;
            lastUid = lastUid(mailboxSession, mailbox);
            ResultSet result = session.execute(update(TABLE_NAME).onlyIf(eq(NEXT_UID, lastUid)).with(set(NEXT_UID, ++lastUid)).where(eq(MAILBOX_ID, mailbox.getMailboxId())));
            isApplied = result.one().getBool(applied);
        } while (! isApplied && tries < maxRetry);
        if( ! isApplied ) {
            throw new MailboxException("Can not obtain rights to manage UID");
        }
        return lastUid;
    }

    @Override
    public long lastUid(MailboxSession mailboxSession, Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet result = session.execute(select(NEXT_UID).from(TABLE_NAME).where(eq(MAILBOX_ID, mailbox.getMailboxId())));
        return result.isExhausted() ? 0 : result.one().getLong(NEXT_UID);
    }

}
