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

//! Cypher to SQL transformer CLI binary

use sqlparser::dialect::CypherDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
// use sqlparser::transformer::CypherToSqlTransformer;  // Uncomment when transformer is enabled
use std::env;
use std::io::{self, Read};
use std::process;

fn main() {
    let result = run();
    
    match result {
        Ok(()) => process::exit(0),
        Err(err) => {
            eprintln!("Error: {}", err);
            process::exit(1);
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    // Get Cypher query from command line argument or stdin
    let cypher_query = if args.len() > 1 {
        args[1].clone()
    } else {
        // Read from stdin
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        input.trim().to_string()
    };

    if cypher_query.is_empty() {
        return Err("No Cypher query provided".into());
    }

    // Parse the Cypher query
    let dialect = CypherDialect;
    let tokens = Tokenizer::new(&dialect, &cypher_query).tokenize()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    // Parse as Cypher statement
    let cypher_stmt = match parser.parse_cypher_statement() {
        Ok(stmt) => stmt,
        Err(e) => {
            return Err(format!("Failed to parse Cypher query: {}", e).into());
        }
    };

    // For now, implement basic transformation logic directly here
    // TODO: Uncomment below when transformer module is enabled
    /*
    let transformer = CypherToSqlTransformer::new();
    let sql_statements = transformer.transform(&cypher_stmt)?;
    
    for stmt in sql_statements {
        println!("{}", stmt);
    }
    */

    // Basic transformation for immediate testing
    let sql_query = transform_cypher_to_sql_basic(&cypher_stmt)?;
    println!("{}", sql_query);

    Ok(())
}

/// Basic Cypher to SQL transformation for immediate testing
/// Uses generic schema: nodes(id, label, properties), relationships(from_id, to_id, type, properties)
fn transform_cypher_to_sql_basic(cypher_stmt: &sqlparser::ast::cypher::CypherStatement) -> Result<String, Box<dyn std::error::Error>> {
    use sqlparser::ast::cypher::*;
    
    match cypher_stmt {
        CypherStatement::Match { optional: _, patterns, r#where, r#return } => {
            let mut sql = String::new();
            let mut node_counter = 0;
            let mut rel_counter = 0;
            
            // Start with SELECT
            sql.push_str("SELECT ");
            
            // Handle return clause
            if let Some(ret_clause) = r#return {
                if ret_clause.distinct {
                    sql.push_str("DISTINCT ");
                }
                
                // Add projection items
                let items: Vec<String> = ret_clause.items.iter().map(|item| {
                    match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                            // Transform property access like n.name to json_extract
                            let expr_str = format!("{}", expr);
                            if expr_str.contains('.') {
                                let parts: Vec<&str> = expr_str.split('.').collect();
                                if parts.len() == 2 {
                                    let var_name = parts[0];
                                    let property_name = parts[1];
                                    return format!("json_extract({}.properties, '$.{}') as {}", var_name, property_name, property_name);
                                }
                            }
                            expr_str
                        },
                        sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                            let expr_str = format!("{}", expr);
                            if expr_str.contains('.') {
                                let parts: Vec<&str> = expr_str.split('.').collect();
                                if parts.len() == 2 {
                                    let var_name = parts[0];
                                    let property_name = parts[1];
                                    return format!("json_extract({}.properties, '$.{}') AS {}", var_name, property_name, alias);
                                }
                            }
                            format!("{} AS {}", expr, alias)
                        },
                        sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                        _ => "*".to_string(),
                    }
                }).collect();
                
                if items.is_empty() {
                    sql.push_str("*");
                } else {
                    sql.push_str(&items.join(", "));
                }
            } else {
                sql.push_str("*");
            }
            
            // Add FROM clause
            sql.push_str(" FROM ");
            
            // Process patterns to determine tables and joins
            let mut from_clauses = Vec::new();
            let mut join_clauses = Vec::new();
            let mut where_conditions = Vec::new();
            
            for pattern in patterns {
                let mut prev_node_alias: Option<String> = None;
                
                for element in &pattern.elements {
                    match element {
                        PatternElement::Node { variable, labels, properties: _ } => {
                            node_counter += 1;
                            let alias = if let Some(ref var) = variable {
                                var.value.clone()
                            } else {
                                format!("n{}", node_counter)
                            };
                            
                            if from_clauses.is_empty() {
                                from_clauses.push(format!("nodes {}", alias));
                            } else {
                                join_clauses.push(format!("JOIN nodes {} ON TRUE", alias));
                            }
                            
                            // Add label condition
                            if !labels.is_empty() {
                                where_conditions.push(format!("{}.label = '{}'", alias, labels[0].value));
                            }
                            
                            prev_node_alias = Some(alias);
                        }
                        PatternElement::Relationship { variable, types, properties: _, direction: _, length: _ } => {
                            rel_counter += 1;
                            let rel_alias = if let Some(ref var) = variable {
                                var.value.clone()
                            } else {
                                format!("r{}", rel_counter)
                            };
                            
                            // For relationships, we need to join the relationship table
                            if let Some(ref from_node) = prev_node_alias {
                                join_clauses.push(format!("JOIN relationships {} ON {}.id = {}.from_id", rel_alias, from_node, rel_alias));
                                
                                // Add relationship type condition - types is already a Vec<Ident>
                                if !types.is_empty() {
                                    where_conditions.push(format!("{}.type = '{}'", rel_alias, types[0].value));
                                }
                            }
                        }
                    }
                }
            }
            
            // Combine FROM and JOINs
            sql.push_str(&from_clauses.join(", "));
            if !join_clauses.is_empty() {
                sql.push(' ');
                sql.push_str(&join_clauses.join(" "));
            }
            
            // Add WHERE clause
            if let Some(where_expr) = r#where {
                where_conditions.push(format!("{}", where_expr));
            }
            
            if !where_conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_conditions.join(" AND "));
            }
            
            // Add ORDER BY, LIMIT etc. from return clause
            if let Some(ret_clause) = r#return {
                if !ret_clause.order_by.is_empty() {
                    sql.push_str(" ORDER BY ");
                    let order_items: Vec<String> = ret_clause.order_by.iter().map(|item| {
                        // Transform property access in ORDER BY as well
                        let expr_str = format!("{}", item.expr);
                        if expr_str.contains('.') {
                            let parts: Vec<&str> = expr_str.split('.').collect();
                            if parts.len() == 2 {
                                let var_name = parts[0];
                                let property_name = parts[1];
                                return format!("json_extract({}.properties, '$.{}') ASC", var_name, property_name);
                            }
                        }
                        format!("{} ASC", item.expr)
                    }).collect();
                    sql.push_str(&order_items.join(", "));
                }
                
                if let Some(limit) = &ret_clause.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }
                
                if let Some(skip) = &ret_clause.skip {
                    sql.push_str(&format!(" OFFSET {}", skip));
                }
            }
            
            Ok(sql)
        }
        
        CypherStatement::Create { patterns } => {
            let mut sql = String::new();
            
            for pattern in patterns {
                for element in &pattern.elements {
                    match element {
                        PatternElement::Node { variable: _, labels, properties } => {
                            sql.push_str("INSERT INTO nodes (label, properties) VALUES (");
                            
                            // Add label
                            if !labels.is_empty() {
                                sql.push_str(&format!("'{}'", labels[0].value));
                            } else {
                                sql.push_str("NULL");
                            }
                            
                            sql.push_str(", ");
                            
                            // Add properties as JSON
                            if let Some(ref _props) = properties {
                                sql.push_str("'{}')");  // Simplified - would need proper JSON construction
                            } else {
                                sql.push_str("'{}')");
                            }
                            
                            sql.push_str(";\n");
                        }
                        PatternElement::Relationship { variable: _, types, properties, direction: _, length: _ } => {
                            sql.push_str("INSERT INTO relationships (from_id, to_id, type, properties) VALUES (");
                            sql.push_str("?, ?, ");  // Placeholder for node IDs
                            
                            // Add relationship type - types is already a Vec<Ident>
                            if !types.is_empty() {
                                sql.push_str(&format!("'{}'", types[0].value));
                            } else {
                                sql.push_str("NULL");
                            }
                            
                            sql.push_str(", ");
                            
                            // Add properties as JSON
                            if let Some(ref _props) = properties {
                                sql.push_str("'{}')");  // Simplified
                            } else {
                                sql.push_str("'{}')");
                            }
                            
                            sql.push_str(";\n");
                        }
                    }
                }
            }
            
            Ok(sql.trim_end_matches(";\n").to_string())
        }
        
        _ => {
            Ok("-- Transformation not yet implemented for this Cypher statement type".to_string())
        }
    }
}