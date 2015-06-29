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

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.FilterBuilders.boolFilter;

import org.apache.james.mailbox.model.SearchQuery;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class FilteredQueryCollector implements Collector<FilteredQueryRepresentation, FilteredQueryRepresentation, FilteredQueryRepresentation> {

    private final SearchQuery.Conjunction type;

    public FilteredQueryCollector(SearchQuery.Conjunction type) {
        this.type = type;
    }

    @Override
    public Supplier<FilteredQueryRepresentation> supplier() {
        return FilteredQueryRepresentation::empty;
    }

    @Override
    public BiConsumer<FilteredQueryRepresentation, FilteredQueryRepresentation> accumulator() {
        return this::apply;
    }

    @Override
    public BinaryOperator<FilteredQueryRepresentation> combiner() {
        return this::apply;
    }

    @Override
    public Function<FilteredQueryRepresentation, FilteredQueryRepresentation> finisher() {
        return (accumulator)->accumulator;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
    }

    private FilteredQueryRepresentation apply(FilteredQueryRepresentation accumulator, FilteredQueryRepresentation collected) {
        initializeAccumulatorWhenNeeded(accumulator, collected);
        switch (type) {
        case OR:
            return applyOr(accumulator, collected);
        case AND:
            return applyAnd(accumulator, collected);
        case NOR:
            return applyNor(accumulator, collected);
        }
        return accumulator;
    }

    private void initializeAccumulatorWhenNeeded(FilteredQueryRepresentation accumulator, FilteredQueryRepresentation collected) {
        // This method is compulsory because elasticSearch refuses to build empty BoolQuery and empty BoolFilter
        if (!accumulator.getQuery().isPresent() && collected.getQuery().isPresent()) {
            accumulator.setQuery(Optional.of(boolQuery()));
        }
        if (!accumulator.getFilter().isPresent() && collected.getFilter().isPresent()) {
            accumulator.setFilter(Optional.of(boolFilter()));
        }
    }

    private FilteredQueryRepresentation applyNor(FilteredQueryRepresentation accumulator, FilteredQueryRepresentation collected) {
        if (collected.getQuery().isPresent()) {
            ((BoolQueryBuilder) accumulator.getQuery().get()).mustNot(collected.getQuery().get());
        }
        if (collected.getFilter().isPresent()) {
            ((BoolFilterBuilder) accumulator.getFilter().get()).mustNot(collected.getFilter().get());
        }
        return accumulator;
    }

    private FilteredQueryRepresentation applyAnd(FilteredQueryRepresentation accumulator, FilteredQueryRepresentation collected) {
        if (collected.getQuery().isPresent()) {
            ((BoolQueryBuilder) accumulator.getQuery().get()).must(collected.getQuery().get());
        }
        if (collected.getFilter().isPresent()) {
            ((BoolFilterBuilder) accumulator.getFilter().get()).must(collected.getFilter().get());
        }
        return accumulator;
    }

    private FilteredQueryRepresentation applyOr(FilteredQueryRepresentation accumulator, FilteredQueryRepresentation collected) {
        if (collected.getQuery().isPresent()) {
            ((BoolQueryBuilder) accumulator.getQuery().get()).should(collected.getQuery().get());
        }
        if (collected.getFilter().isPresent()) {
            ((BoolFilterBuilder) accumulator.getFilter().get()).should(collected.getFilter().get());
        }
        return accumulator;
    }
}
