// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::dialect::Dialect;

/// A [`Dialect`] for Cypher query language used in Neo4j
#[derive(Debug)]
pub struct CypherDialect;

impl Dialect for CypherDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '$'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' // Cypher uses backticks for delimited identifiers
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        false
    }

    fn supports_in_empty_list(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_connect_by(&self) -> bool {
        false
    }

    fn supports_match_recognize(&self) -> bool {
        false
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        false
    }

    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        false
    }

    fn supports_dictionary_syntax(&self) -> bool {
        true // Cypher supports map syntax {key: value}
    }

    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn support_map_literal_syntax(&self) -> bool {
        true
    }

    fn supports_parenthesized_set_variables(&self) -> bool {
        false
    }

    fn supports_select_wildcard_except(&self) -> bool {
        false
    }
}