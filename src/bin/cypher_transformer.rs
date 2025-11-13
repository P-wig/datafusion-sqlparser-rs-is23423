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
//! 
//! This binary can be called by the Go server to transform Cypher queries to SQL

use sqlparser::dialect::CypherDialect;
use sqlparser::parser::Parser;
use sqlparser::transformer::CypherToSqlTransformer;
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

    // Parse the Cypher query using our custom dialect
    let dialect = CypherDialect;
    
    // For now, we'll parse it as SQL and then try to extract Cypher patterns
    // This is a simplified approach - in a full implementation, you'd modify
    // the main parser to recognize Cypher statements
    let mut parser = Parser::new(&dialect).try_with_sql(&cypher_query)?;
    
    // Try to parse as a Cypher statement
    let cypher_stmt = match parser.parse_cypher_statement() {
        Ok(stmt) => stmt,
        Err(e) => {
            return Err(format!("Failed to parse Cypher query: {}", e).into());
        }
    };

    // Transform to SQL
    let transformer = CypherToSqlTransformer::new();
    let sql_statements = transformer.transform(&cypher_stmt)?;

    // Output the SQL statements
    for (i, stmt) in sql_statements.iter().enumerate() {
        if i > 0 {
            println!(";\n");
        }
        print!("{}", stmt);
    }
    println!(";");

    Ok(())
}