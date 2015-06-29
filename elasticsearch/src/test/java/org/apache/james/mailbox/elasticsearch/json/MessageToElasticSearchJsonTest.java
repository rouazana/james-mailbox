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

package org.apache.james.mailbox.elasticsearch.json;

import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.james.mailbox.FlagsBuilder;
import org.apache.james.mailbox.store.TestId;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.mail.model.impl.PropertyBuilder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;
import org.junit.Before;
import org.junit.Test;

import javax.mail.Flags;
import javax.mail.util.SharedByteArrayInputStream;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static net.javacrumbs.jsonunit.core.Option.IGNORING_ARRAY_ORDER;
import static net.javacrumbs.jsonunit.core.Option.IGNORING_VALUES;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;


public class MessageToElasticSearchJsonTest {

    public static final int SIZE = 25;
    public static final int BODY_START_OCTET = 100;
    public static final TestId MAILBOX_ID = TestId.of(18L);
    public static final long MOD_SEQ = 42L;
    public static final long UID = 25L;

    private SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");

    private Date date;
    private PropertyBuilder propertyBuilder;
    private MessageToElasticSearchJson messageToElasticSearchJson;

    @Before
    public void setUp() throws Exception {
        messageToElasticSearchJson = new MessageToElasticSearchJson();
        date = formatter.parse("07-06-2015");
        propertyBuilder = new PropertyBuilder();
        propertyBuilder.setMediaType("plain");
        propertyBuilder.setSubType("text");
        propertyBuilder.setTextualLineCount(18L);
        propertyBuilder.setContentDescription("An e-mail");
    }

    @Test
    public void spamEmailShouldBeWellConvertedToJson() throws IOException {
        Message<TestId> spamMail = new SimpleMessage<>(date,
            SIZE,
            BODY_START_OCTET,
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/spamMail.eml"))),
            new Flags(),
            propertyBuilder,
            MAILBOX_ID);
        spamMail.setModSeq(MOD_SEQ);
        assertThatJson(messageToElasticSearchJson.convertToJson(spamMail))
            .when(IGNORING_ARRAY_ORDER)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/spamMail.json")));
    }

    @Test
    public void htmlEmailShouldBeWellConvertedToJson() throws IOException {
        Message<TestId> htmlMail = new SimpleMessage<>(date,
            SIZE,
            BODY_START_OCTET,
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/htmlMail.eml"))),
            new FlagsBuilder().add(Flags.Flag.DELETED, Flags.Flag.SEEN).add("social", "pocket-money").build(),
            propertyBuilder,
            MAILBOX_ID);
        htmlMail.setModSeq(MOD_SEQ);
        htmlMail.setUid(UID);
        assertThatJson(messageToElasticSearchJson.convertToJson(htmlMail))
            .when(IGNORING_ARRAY_ORDER)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/htmlMail.json")));
    }

    @Test
    public void pgpSignedEmailShouldBeWellConvertedToJson() throws IOException {
        Message<TestId> pgpSignedMail = new SimpleMessage<>(date,
            SIZE,
            BODY_START_OCTET,
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/pgpSignedMail.eml"))),
            new FlagsBuilder().add(Flags.Flag.DELETED, Flags.Flag.SEEN).add("debian", "security").build(),
            propertyBuilder,
            MAILBOX_ID);
        pgpSignedMail.setModSeq(MOD_SEQ);
        pgpSignedMail.setUid(UID);
        assertThatJson(messageToElasticSearchJson.convertToJson(pgpSignedMail))
            .when(IGNORING_ARRAY_ORDER)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/pgpSignedMail.json")));
    }

    @Test
    public void simpleEmailShouldBeWellConvertedToJson() throws IOException {
        Message<TestId> mail = new SimpleMessage<>(date,
            SIZE,
            BODY_START_OCTET,
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/mail.eml"))),
            new FlagsBuilder().add(Flags.Flag.DELETED, Flags.Flag.SEEN).add("debian", "security").build(),
            propertyBuilder,
            MAILBOX_ID);
        mail.setModSeq(MOD_SEQ);
        mail.setUid(UID);
        assertThatJson(messageToElasticSearchJson.convertToJson(mail))
            .when(IGNORING_ARRAY_ORDER).when(IGNORING_VALUES)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/mail.json")));
    }

    @Test
    public void recursiveEmailShouldBeWellConvertedToJson() throws IOException {
        Message<TestId> recursiveMail = new SimpleMessage<>(date,
            SIZE,
            BODY_START_OCTET,
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/recursiveMail.eml"))),
            new FlagsBuilder().add(Flags.Flag.DELETED, Flags.Flag.SEEN).add("debian", "security").build(),
            propertyBuilder,
            MAILBOX_ID);
        recursiveMail.setModSeq(MOD_SEQ);
        recursiveMail.setUid(UID);
        assertThatJson(messageToElasticSearchJson.convertToJson(recursiveMail))
            .when(IGNORING_ARRAY_ORDER).when(IGNORING_VALUES)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/recursiveMail.json")));
    }

    @Test
    public void emailWithNoInternalDateShouldUseNowDate() throws IOException {
        Message<TestId> mailWithNoInternalDate = new SimpleMessage<>(null,
            SIZE,
            BODY_START_OCTET,
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/recursiveMail.eml"))),
            new FlagsBuilder().add(Flags.Flag.DELETED, Flags.Flag.SEEN).add("debian", "security").build(),
            propertyBuilder,
            MAILBOX_ID);
        mailWithNoInternalDate.setModSeq(MOD_SEQ);
        mailWithNoInternalDate.setUid(UID);
        assertThatJson(messageToElasticSearchJson.convertToJson(mailWithNoInternalDate))
            .when(IGNORING_ARRAY_ORDER)
            .when(IGNORING_VALUES)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/recursiveMail.json")));
    }

    @Test(expected = NullPointerException.class)
    public void emailWithNoMailboxIdShouldThrow() throws IOException {
        Message<TestId> mailWithNoMailboxId;
        try {
            mailWithNoMailboxId = new SimpleMessage<>(date,
                SIZE,
                BODY_START_OCTET,
                new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/recursiveMail.eml"))),
                new FlagsBuilder().add(Flags.Flag.DELETED, Flags.Flag.SEEN).add("debian", "security").build(),
                propertyBuilder,
                null);
            mailWithNoMailboxId.setModSeq(MOD_SEQ);
            mailWithNoMailboxId.setUid(UID);
        } catch(Exception exception) {
            throw Throwables.propagate(exception);
        }
        messageToElasticSearchJson.convertToJson(mailWithNoMailboxId);
    }

}
