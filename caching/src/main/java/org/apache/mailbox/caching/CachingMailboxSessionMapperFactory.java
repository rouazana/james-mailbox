package org.apache.mailbox.caching;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.SubscriptionException;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.user.SubscriptionMapper;

public class CachingMailboxSessionMapperFactory<Id> extends
		MailboxSessionMapperFactory<Id> {

	private MailboxSessionMapperFactory<Id> underlying;
	private MailboxByPathCache<Id> mailboxByPathCache;
	private MailboxMetadataCache<Id> mailboxMetadataCache;

	public CachingMailboxSessionMapperFactory(MailboxSessionMapperFactory<Id> underlying, MailboxByPathCache<Id> mailboxByPathCache, MailboxMetadataCache<Id> mailboxMetadataCache) {
		this.underlying = underlying;
		this.mailboxByPathCache = mailboxByPathCache;
		this.mailboxMetadataCache = mailboxMetadataCache;
	}
	
	@Override
	public MessageMapper<Id> createMessageMapper(MailboxSession session)
			throws MailboxException {
		return new CachingMessageMapper<Id>(underlying.createMessageMapper(session), mailboxByPathCache);
	}

	@Override
	public MailboxMapper<Id> createMailboxMapper(MailboxSession session)
			throws MailboxException {
	    //TODO(eric) this cast will not work!!! Temporary adding it to compile the project...
		return (MailboxMapper<Id>) new CachingMessageMapper<Id>(underlying.createMailboxMapper(session), mailboxMetadataCache);
	}

	@Override
	public SubscriptionMapper createSubscriptionMapper(MailboxSession session)
			throws SubscriptionException {
		return underlying.createSubscriptionMapper(session);
	}

}
