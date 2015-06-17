package org.apache.james.mailbox.cassandra.mail;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.james.mailbox.cassandra.CassandraClusterSingleton;
import org.apache.james.mailbox.cassandra.CassandraMailboxSessionMapperFactory;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.mock.MockMailboxSession;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.model.MapperProvider;

import java.util.UUID;

public class CassandraMapperProvider implements MapperProvider<UUID> {

    private static final CassandraClusterSingleton cassandra = CassandraClusterSingleton.build();

    @Override
    public MailboxMapper<UUID> createMailboxMapper() throws MailboxException {
        return new CassandraMailboxSessionMapperFactory(
            new CassandraUidProvider(cassandra.getConf()),
            new CassandraModSeqProvider(cassandra.getConf()),
            cassandra.getConf()
        ).getMailboxMapper(new MockMailboxSession("benwa"));
    }

    @Override
    public MessageMapper<UUID> createMessageMapper() throws MailboxException {
        return new CassandraMailboxSessionMapperFactory(
            new CassandraUidProvider(cassandra.getConf()),
            new CassandraModSeqProvider(cassandra.getConf()),
            cassandra.getConf()
        ).getMessageMapper(new MockMailboxSession("benwa"));
    }

    @Override
    public UUID generateId() {
        return UUIDs.timeBased();
    }

    @Override
    public void clearMapper() throws MailboxException {
        cassandra.clearAllTables();
    }

    @Override
    public void ensureMapperPrepared() throws MailboxException {
        cassandra.ensureAllTables();
    }
}
