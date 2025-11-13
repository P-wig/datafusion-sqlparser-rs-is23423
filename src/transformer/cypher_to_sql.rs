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

//! Cypher to SQL transformation implementation

use crate::ast::cypher::*;
use crate::ast::*;

/// Transformer that converts Cypher statements to SQL statements
pub struct CypherToSqlTransformer {
    /// Graph-to-relational schema mapping configuration
    pub config: TransformConfig,
}

/// Configuration for the transformation process
#[derive(Debug, Clone)]
pub struct TransformConfig {
    /// Table name for storing nodes
    pub node_table: String,
    /// Table name for storing relationships
    pub relationship_table: String,
    /// Whether to use node labels as separate tables
    pub use_label_tables: bool,
}

impl Default for TransformConfig {
    fn default() -> Self {
        Self {
            node_table: "nodes".to_string(),
            relationship_table: "relationships".to_string(),
            use_label_tables: true,
        }
    }
}

impl CypherToSqlTransformer {
    /// Create a new transformer with default configuration
    pub fn new() -> Self {
        Self {
            config: TransformConfig::default(),
        }
    }

    /// Create a new transformer with custom configuration
    pub fn with_config(config: TransformConfig) -> Self {
        Self { config }
    }

    /// Transform a Cypher statement to SQL statements
    pub fn transform(&self, cypher_stmt: &CypherStatement) -> Result<Vec<Statement>, String> {
        match cypher_stmt {
            CypherStatement::Match {
                optional,
                patterns,
                r#where,
                r#return,
            } => self.transform_match(*optional, patterns, r#where, r#return),
            CypherStatement::Create { patterns } => self.transform_create(patterns),
            CypherStatement::Merge {
                patterns,
                on_create,
                on_match,
            } => self.transform_merge(patterns, on_create, on_match),
            CypherStatement::Delete {
                detach,
                what,
                r#where,
            } => self.transform_delete(*detach, what, r#where),
        }
    }

    /// Transform MATCH statement to SELECT
    fn transform_match(
        &self,
        optional: bool,
        patterns: &[Pattern],
        where_clause: &Option<Expr>,
        return_clause: &Option<ReturnClause>,
    ) -> Result<Vec<Statement>, String> {
        let mut from_tables = vec![];
        let mut joins = vec![];
        let mut where_conditions = vec![];

        // Process patterns to generate table references and joins
        for pattern in patterns {
            self.process_pattern_for_match(pattern, &mut from_tables, &mut joins, &mut where_conditions)?;
        }

        // Add WHERE clause conditions
        if let Some(where_expr) = where_clause {
            where_conditions.push(where_expr.clone());
        }

        // Build the final WHERE clause
        let final_where = if where_conditions.is_empty() {
            None
        } else {
            Some(self.combine_conditions(where_conditions))
        };

        // Build projection
        let projection = if let Some(ret_clause) = return_clause {
            ret_clause.items.clone()
        } else {
            vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())]
        };

        // Build ORDER BY
        let order_by = return_clause
            .as_ref()
            .map(|ret| ret.order_by.clone())
            .unwrap_or_default();

        // Build LIMIT and OFFSET
        let (limit, offset) = if let Some(ret_clause) = return_clause {
            let limit = ret_clause.limit.clone();
            let offset = ret_clause.skip.clone().map(|skip| Offset {
                value: skip,
                rows: OffsetRows::None,
            });
            (limit, offset)
        } else {
            (None, None)
        };

        // Create the SELECT statement
        let mut body = SetExpr::Select(Box::new(Select {
            distinct: return_clause
                .as_ref()
                .map(|ret| if ret.distinct { Some(Distinct::Distinct) } else { None })
                .flatten(),
            top: None,
            projection,
            into: None,
            from: from_tables,
            lateral_views: vec![],
            prewhere: None,
            selection: final_where,
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
        }));

        // Add joins to the SELECT
        if !joins.is_empty() {
            if let SetExpr::Select(ref mut select) = body {
                for join in joins {
                    if let Some(last_table) = select.from.last_mut() {
                        last_table.joins.push(join);
                    }
                }
            }
        }

        let query = Query {
            with: None,
            body: Box::new(body),
            order_by,
            limit,
            limit_by: vec![],
            offset,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        };

        Ok(vec![Statement::Query(Box::new(query))])
    }

    /// Process a pattern for MATCH statement
    fn process_pattern_for_match(
        &self,
        pattern: &Pattern,
        from_tables: &mut Vec<TableWithJoins>,
        joins: &mut Vec<Join>,
        where_conditions: &mut Vec<Expr>,
    ) -> Result<(), String> {
        for (i, element) in pattern.elements.iter().enumerate() {
            match element {
                PatternElement::Node { variable, labels, properties } => {
                    self.process_node_for_match(variable, labels, properties, from_tables, where_conditions)?;
                }
                PatternElement::Relationship { variable, types, properties, direction, length } => {
                    self.process_relationship_for_match(
                        variable, types, properties, direction, length,
                        joins, where_conditions, i
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Process a node in MATCH pattern
    fn process_node_for_match(
        &self,
        variable: &Option<Ident>,
        labels: &[Ident],
        properties: &Option<Expr>,
        from_tables: &mut Vec<TableWithJoins>,
        where_conditions: &mut Vec<Expr>,
    ) -> Result<(), String> {
        // Choose table based on configuration and labels
        let table_name = if self.config.use_label_tables && !labels.is_empty() {
            // Use the first label as the table name (simplified approach)
            labels[0].value.clone()
        } else {
            self.config.node_table.clone()
        };

        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new(table_name)]),
            alias: variable.as_ref().map(|v| TableAlias {
                name: v.clone(),
                columns: vec![],
            }),
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
        };

        from_tables.push(TableWithJoins {
            relation: table_factor,
            joins: vec![],
        });

        // Add label conditions if using single node table
        if !self.config.use_label_tables && !labels.is_empty() {
            let table_ref = variable
                .as_ref()
                .map(|v| v.clone())
                .unwrap_or_else(|| Ident::new(&self.config.node_table));

            for label in labels {
                let label_condition = Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        table_ref.clone(),
                        Ident::new("label"),
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString(label.value.clone()))),
                };
                where_conditions.push(label_condition);
            }
        }

        // Add property conditions
        if let Some(_props) = properties {
            // TODO: Implement property matching
            // This would require parsing the map literal and creating appropriate conditions
        }

        Ok(())
    }

    /// Process a relationship in MATCH pattern
    fn process_relationship_for_match(
        &self,
        variable: &Option<Ident>,
        types: &[Ident],
        properties: &Option<Expr>,
        direction: &RelationshipDirection,
        length: &Option<RelationshipLength>,
        joins: &mut Vec<Join>,
        where_conditions: &mut Vec<Expr>,
        _element_index: usize,
    ) -> Result<(), String> {
        // Create join to relationships table
        let join = Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new(self.config.relationship_table.clone())]),
                alias: variable.as_ref().map(|v| TableAlias {
                    name: v.clone(),
                    columns: vec![],
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            global: false,
            join_operator: JoinOperator::Inner(JoinConstraint::On(
                // This is simplified - in practice you'd need to join on node IDs
                Expr::Value(Value::Boolean(true))
            )),
        };

        joins.push(join);

        // Add type conditions
        if !types.is_empty() {
            let table_ref = variable
                .as_ref()
                .map(|v| v.clone())
                .unwrap_or_else(|| Ident::new(&self.config.relationship_table));

            let type_conditions: Vec<Expr> = types.iter().map(|rel_type| {
                Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        table_ref.clone(),
                        Ident::new("type"),
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString(rel_type.value.clone()))),
                }
            }).collect();

            if type_conditions.len() == 1 {
                where_conditions.push(type_conditions.into_iter().next().unwrap());
            } else if type_conditions.len() > 1 {
                where_conditions.push(self.combine_conditions_with_or(type_conditions));
            }
        }

        // TODO: Handle direction, length, and properties

        Ok(())
    }

    /// Transform CREATE statement to INSERT
    fn transform_create(&self, patterns: &[Pattern]) -> Result<Vec<Statement>, String> {
        let mut statements = vec![];

        for pattern in patterns {
            for element in &pattern.elements {
                match element {
                    PatternElement::Node { variable: _, labels, properties } => {
                        let table_name = if self.config.use_label_tables && !labels.is_empty() {
                            labels[0].value.clone()
                        } else {
                            self.config.node_table.clone()
                        };

                        let insert = Statement::Insert(Insert {
                            or: None,
                            into: true,
                            table_name: ObjectName(vec![Ident::new(table_name)]),
                            table_alias: None,
                            columns: vec![], // TODO: Extract from properties
                            overwrite: false,
                            source: None, // TODO: Create VALUES from properties
                            partitioned: None,
                            after_columns: vec![],
                            table: false,
                            on: None,
                            returning: None,
                            replace_into: false,
                            priority: None,
                            insert_alias: None,
                            ignore: false,
                        });

                        statements.push(insert);
                    }
                    PatternElement::Relationship { .. } => {
                        // TODO: Implement relationship creation
                    }
                }
            }
        }

        Ok(statements)
    }

    /// Transform MERGE statement
    fn transform_merge(
        &self,
        _patterns: &[Pattern],
        _on_create: &Option<Vec<SetClause>>,
        _on_match: &Option<Vec<SetClause>>,
    ) -> Result<Vec<Statement>, String> {
        // TODO: Implement MERGE transformation (complex - requires UPSERT logic)
        Err("MERGE transformation not yet implemented".to_string())
    }

    /// Transform DELETE statement
    fn transform_delete(
        &self,
        _detach: bool,
        _what: &[Expr],
        _where_clause: &Option<Expr>,
    ) -> Result<Vec<Statement>, String> {
        // TODO: Implement DELETE transformation
        Err("DELETE transformation not yet implemented".to_string())
    }

    /// Combine multiple conditions with AND
    fn combine_conditions(&self, conditions: Vec<Expr>) -> Expr {
        conditions.into_iter().reduce(|acc, expr| {
            Expr::BinaryOp {
                left: Box::new(acc),
                op: BinaryOperator::And,
                right: Box::new(expr),
            }
        }).unwrap_or(Expr::Value(Value::Boolean(true)))
    }

    /// Combine multiple conditions with OR
    fn combine_conditions_with_or(&self, conditions: Vec<Expr>) -> Expr {
        conditions.into_iter().reduce(|acc, expr| {
            Expr::BinaryOp {
                left: Box::new(acc),
                op: BinaryOperator::Or,
                right: Box::new(expr),
            }
        }).unwrap_or(Expr::Value(Value::Boolean(false)))
    }
}

impl Default for CypherToSqlTransformer {
    fn default() -> Self {
        Self::new()
    }
}