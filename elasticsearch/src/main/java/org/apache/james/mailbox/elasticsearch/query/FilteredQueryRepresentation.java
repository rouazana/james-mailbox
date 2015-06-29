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

package org.apache.james.mailbox.elasticsearch.query;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Optional;

public  class FilteredQueryRepresentation {

    public static FilteredQueryRepresentation fromQuery(QueryBuilder query) {
        return new FilteredQueryRepresentation(Optional.of(query), Optional.empty());
    }

    public static FilteredQueryRepresentation fromFilter(FilterBuilder filter) {
        return new FilteredQueryRepresentation(Optional.empty(), Optional.of(filter));
    }

    public static FilteredQueryRepresentation empty() {
        return new FilteredQueryRepresentation(Optional.empty(), Optional.empty());
    }

    private Optional<FilterBuilder> filter;
    private Optional<QueryBuilder> query;

    private FilteredQueryRepresentation(Optional<QueryBuilder> query, Optional<FilterBuilder> filter) {
        this.query = query;
        this.filter = filter;
    }

    public Optional<FilterBuilder> getFilter() {
        return filter;
    }

    public Optional<QueryBuilder> getQuery() {
        return query;
    }

    public void setFilter(Optional<FilterBuilder> filter) {
        this.filter = filter;
    }

    public void setQuery(Optional<QueryBuilder> query) {
        this.query = query;
    }
}
