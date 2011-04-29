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

package org.apache.james.mailbox;

import java.util.Arrays;
import java.util.Iterator;

import javax.mail.Flags;

/**
 * Represent a Flag update for a message
 * 
 *
 */
public class UpdatedFlags {

    private final long uid;
    private final Flags oldFlags;
    private final Flags newFlags;
    private final Flags modifiedFlags;

    public UpdatedFlags(long uid, Flags oldFlags, Flags newFlags) {
       this.uid = uid;
       this.oldFlags = oldFlags;
       this.newFlags = newFlags;
       this.modifiedFlags = new Flags();
       addModifiedSystemFlags();
       addModifiedUserFlags();
    }
    
    private void addModifiedSystemFlags() {
        if (isChanged(oldFlags, newFlags, Flags.Flag.ANSWERED)) {
            modifiedFlags.add(Flags.Flag.ANSWERED);
        }
        if(isChanged(oldFlags, newFlags, Flags.Flag.DELETED)) {
            modifiedFlags.add(Flags.Flag.DELETED);
        }
        if (isChanged(oldFlags, newFlags, Flags.Flag.DRAFT)) {
            modifiedFlags.add(Flags.Flag.DRAFT);
        }
        if (isChanged(oldFlags, newFlags, Flags.Flag.FLAGGED)) {
            modifiedFlags.add(Flags.Flag.FLAGGED);
        }
        if (isChanged(oldFlags, newFlags, Flags.Flag.RECENT)) {
            modifiedFlags.add(Flags.Flag.RECENT);
        }
        if (isChanged(oldFlags, newFlags, Flags.Flag.SEEN)) {
            modifiedFlags.add(Flags.Flag.SEEN);
        }
    }
    
    private void addModifiedUserFlags() {
        addModifiedUserFlags(oldFlags.getUserFlags());
        addModifiedUserFlags(newFlags.getUserFlags());

    }
    

    private void addModifiedUserFlags(String[] userflags) {
        for (int i = 0; i < userflags.length; i++) {
            String userFlag = userflags[i];
            if (isChanged(oldFlags, newFlags, userFlag)) {
                modifiedFlags.add(userFlag);

            }
        }
    }
    private static boolean isChanged(final Flags original, final Flags updated, Flags.Flag flag) {
        return original != null && updated != null && (original.contains(flag) ^ updated.contains(flag));
    }

    private static boolean isChanged(final Flags original, final Flags updated, String userFlag) {
        return original != null && updated != null && (original.contains(userFlag) ^ updated.contains(userFlag));
    }

    
    /**
     * Return the old {@link Flags} for the message
     * 
     * @return oldFlags
     */
    public Flags getOldFlags() {
        return oldFlags;
    }
    
    /**
     * Return the new {@link Flags} for the message
     * 
     * @return newFlags
     */
    public Flags getNewFlags() {
        return newFlags;
    }
    
    /**
     * Return the uid for the message whichs {@link Flags} was updated
     * 
     * @return uid
     */
    public long getUid() {
        return uid;
    }
   

    /**
     * Gets an iterator for the system flags changed.
     * 
     * @return <code>Flags.Flag</code> <code>Iterator</code>, not null
     */

    public Iterator<Flags.Flag> systemFlagIterator() {
        return Arrays.asList(modifiedFlags.getSystemFlags()).iterator();
    }
    
    /**
     * Gets an iterator for the users flags changed.
     * 
     * @return <code>String</code> <code>Iterator</code>, not null
     */

    public Iterator<String> userFlagIterator() {
        return Arrays.asList(modifiedFlags.getUserFlags()).iterator();
    }
}