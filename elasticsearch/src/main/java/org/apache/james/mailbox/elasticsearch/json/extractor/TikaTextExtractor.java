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

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;

public class TikaTextExtractor implements TextExtractor {

    private static class MetadataEntry {

        private final String name;
        private final ImmutableList<String> entries;

        public MetadataEntry(String name, List<String> entries) {
            this.name = name;
            this.entries = ImmutableList.copyOf(entries);
        }

        public String getName() {
            return name;
        }

        public List<String> getEntries() {
            return entries;
        }
    }

    private final Parser parser;
    
    public TikaTextExtractor() {
        parser = new AutoDetectParser();
    }

    public ParsedContent extractContent(InputStream inputStream, Optional<String> contentType, Optional<String> fileName) throws Exception {
        Metadata metadata = new Metadata();
        fileName.ifPresent(x -> metadata.set(Metadata.RESOURCE_NAME_KEY, x));
        contentType.ifPresent(x -> metadata.set(Metadata.CONTENT_TYPE, x));

        StringWriter stringWriter = new StringWriter();
        BodyContentHandler bodyContentHandler = new BodyContentHandler(stringWriter);
        parser.parse(inputStream, bodyContentHandler, metadata, new ParseContext());

        return new ParsedContent(Optional.of(stringWriter.toString()), convertMetadataToMultimap(metadata));
    }

    private Multimap<String, String> convertMetadataToMultimap(Metadata metadata) {
        return Arrays.stream(metadata.names())
            .map(name -> new MetadataEntry(name, Arrays.asList(metadata.getValues(name))))
            .reduce(ArrayListMultimap.create(), (metadataMultiMap, metadataEntry) -> {
                    metadataMultiMap.putAll(metadataEntry.getName(), metadataEntry.getEntries());
                    return metadataMultiMap;
                }, (metadataMultimap1, metadataMultimap2) -> {
                    metadataMultimap1.putAll(metadataMultimap2);
                    return metadataMultimap1;
                });
    }

}
