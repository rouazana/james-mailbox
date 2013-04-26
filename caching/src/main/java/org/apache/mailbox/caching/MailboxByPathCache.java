package org.apache.mailbox.caching;

import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.model.Mailbox;

public interface MailboxByPathCache<Id> {

	public abstract Mailbox<Id> findMailboxByPath(MailboxPath mailboxName,
			MailboxMapper<Id> underlying) throws MailboxNotFoundException,
			MailboxException;

	public abstract void invalidate(Mailbox<Id> mailbox);
	
	public abstract void invalidate(MailboxPath mailboxPath);

	// for the purpose of cascading the invalidations; does it make sense? 
	//public void connectTo(MailboxMetadataCache<Id> mailboxMetadataCache);

}