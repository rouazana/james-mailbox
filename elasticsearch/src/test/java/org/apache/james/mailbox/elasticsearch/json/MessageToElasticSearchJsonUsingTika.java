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

import javax.mail.Flags;
import javax.mail.util.SharedByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.james.mailbox.elasticsearch.json.extractor.TikaTextExtractor;
import org.apache.james.mailbox.store.TestId;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.mail.model.impl.PropertyBuilder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;
import org.junit.Before;
import org.junit.Test;
import static net.javacrumbs.jsonunit.core.Option.IGNORING_ARRAY_ORDER;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;

public class MessageToElasticSearchJsonUsingTika {

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
        messageToElasticSearchJson = new MessageToElasticSearchJson(new TikaTextExtractor());
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
            new SharedByteArrayInputStream(IOUtils.toByteArray(ClassLoader.getSystemResourceAsStream("documents/nonTextual.eml"))),
            new Flags(),
            propertyBuilder,
            MAILBOX_ID);
        spamMail.setModSeq(MOD_SEQ);
        assertThatJson(messageToElasticSearchJson.convertToJson(spamMail))
            .when(IGNORING_ARRAY_ORDER)
            .isEqualTo(IOUtils.toString(ClassLoader.getSystemResource("documents/nonTextual.json")));
    }

}
