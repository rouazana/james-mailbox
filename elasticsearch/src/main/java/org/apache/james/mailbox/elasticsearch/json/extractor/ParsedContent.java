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

package org.apache.james.mailbox.elasticsearch.json.extractor;

import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

public class ParsedContent {

    private final Optional<String> textualContent;
    private final ImmutableMultimap<String, String> metadata;

    public ParsedContent(Optional<String> textualContent, Multimap<String, String> metadata) {
        this.textualContent = textualContent;
        this.metadata = ImmutableMultimap.copyOf(metadata);
    }

    public Optional<String> getTextualContent() {
        return textualContent;
    }

    public  Multimap<String, String> getMetadata() {
        return metadata;
    }

    @Override public boolean equals(Object o) {
        if (o instanceof ParsedContent) {
            ParsedContent other = (ParsedContent) o;
            return Objects.equals(textualContent, other.textualContent)
                && Objects.equals(metadata, other.metadata);
        }
        return false;
    }

    @Override public int hashCode() {
        return Objects.hash(textualContent, metadata);
    }
}
