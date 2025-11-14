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

//! Cypher language parser implementation

use crate::ast::cypher::*;
use crate::ast::*;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

impl<'a> Parser<'a> {
    /// Parse a Cypher statement - entry point for Cypher parsing
    pub fn parse_cypher_statement(&mut self) -> Result<CypherStatement, ParserError> {
        match self.peek_token().token {
            Token::Word(ref w) if w.keyword == Keyword::MATCH => {
                self.parse_cypher_match()
            }
            Token::Word(ref w) if w.keyword == Keyword::OPTIONAL => {
                // OPTIONAL MATCH
                self.next_token(); // consume OPTIONAL
                self.expect_keyword(Keyword::MATCH)?;
                self.parse_cypher_match_with_optional(true)
            }
            Token::Word(ref w) if w.keyword == Keyword::CREATE => {
                self.parse_cypher_create()
            }
            Token::Word(ref w) if w.keyword == Keyword::MERGE => {
                self.parse_cypher_merge()
            }
            Token::Word(ref w) if w.keyword == Keyword::DELETE => {
                self.parse_cypher_delete()
            }
            Token::Word(ref w) if w.keyword == Keyword::DETACH => {
                self.parse_cypher_detach_delete()
            }
            _ => self.expected("MATCH, CREATE, MERGE, or DELETE", self.peek_token()),
        }
    }

    /// Parse a MATCH statement
    fn parse_cypher_match(&mut self) -> Result<CypherStatement, ParserError> {
        self.parse_cypher_match_with_optional(false)
    }

    /// Parse a MATCH statement with optional flag
    fn parse_cypher_match_with_optional(&mut self, optional: bool) -> Result<CypherStatement, ParserError> {
        if !optional {
            self.expect_keyword(Keyword::MATCH)?;
        }
        
        let patterns = self.parse_cypher_patterns()?;
        
        let r#where = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        let r#return = if self.parse_keyword(Keyword::RETURN) {
            Some(self.parse_cypher_return_clause()?)
        } else {
            None
        };

        Ok(CypherStatement::Match {
            optional,
            patterns,
            r#where,
            r#return,
        })
    }

    /// Parse a CREATE statement
    fn parse_cypher_create(&mut self) -> Result<CypherStatement, ParserError> {
        self.expect_keyword(Keyword::CREATE)?;
        let patterns = self.parse_cypher_patterns()?;
        
        Ok(CypherStatement::Create { patterns })
    }

    /// Parse a MERGE statement
    fn parse_cypher_merge(&mut self) -> Result<CypherStatement, ParserError> {
        self.expect_keyword(Keyword::MERGE)?;
        let patterns = self.parse_cypher_patterns()?;
        
        let mut on_create = None;
        let mut on_match = None;
        
        // Parse ON CREATE SET and ON MATCH SET clauses
        while self.parse_keyword(Keyword::ON) {
            if self.parse_keyword(Keyword::CREATE) {
                self.expect_keyword(Keyword::SET)?;
                on_create = Some(self.parse_cypher_set_clauses()?);
            } else if self.parse_keyword(Keyword::MATCH) {
                self.expect_keyword(Keyword::SET)?;
                on_match = Some(self.parse_cypher_set_clauses()?);
            } else {
                return self.expected("CREATE or MATCH after ON", self.peek_token());
            }
        }
        
        Ok(CypherStatement::Merge {
            patterns,
            on_create,
            on_match,
        })
    }

    /// Parse a DELETE statement
    fn parse_cypher_delete(&mut self) -> Result<CypherStatement, ParserError> {
        self.expect_keyword(Keyword::DELETE)?;
        let what = self.parse_comma_separated(|parser| parser.parse_expr())?;
        
        let r#where = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        Ok(CypherStatement::Delete {
            detach: false,
            what,
            r#where,
        })
    }

    /// Parse a DETACH DELETE statement
    fn parse_cypher_detach_delete(&mut self) -> Result<CypherStatement, ParserError> {
        self.expect_keyword(Keyword::DETACH)?;
        self.expect_keyword(Keyword::DELETE)?;
        let what = self.parse_comma_separated(|parser| parser.parse_expr())?;
        
        let r#where = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        Ok(CypherStatement::Delete {
            detach: true,
            what,
            r#where,
        })
    }

    /// Parse comma-separated patterns
    fn parse_cypher_patterns(&mut self) -> Result<Vec<Pattern>, ParserError> {
        self.parse_comma_separated(|parser| parser.parse_cypher_pattern())
    }

    /// Parse a single pattern (nodes and relationships)
    fn parse_cypher_pattern(&mut self) -> Result<Pattern, ParserError> {
        let mut elements = vec![];
        
        // A pattern starts with a node
        if self.consume_token(&Token::LParen) {
            let node = self.parse_cypher_node()?;
            elements.push(node);
            
            // Parse relationships and connected nodes
            while self.peek_token().token == Token::Minus || 
                  matches!(self.peek_token().token, Token::Lt) {
                
                let relationship = self.parse_cypher_relationship()?;
                elements.push(relationship);
                
                // After a relationship, expect another node
                if self.consume_token(&Token::LParen) {
                    let node = self.parse_cypher_node()?;
                    elements.push(node);
                } else {
                    return self.expected("node after relationship", self.peek_token());
                }
            }
        } else {
            return self.expected("pattern starting with '('", self.peek_token());
        }
        
        Ok(Pattern { elements })
    }

    /// Parse a node pattern: (variable:Label {properties})
    fn parse_cypher_node(&mut self) -> Result<PatternElement, ParserError> {
        // Already consumed the opening parenthesis
    
        let mut variable = None;
        let mut labels = vec![];
    
        // Check what we have: variable, label, or empty
        if matches!(self.peek_token().token, Token::Word(_)) {
            if self.peek_nth_token(1).token == Token::Colon {
                // This is either "variable:Label" or ":Label"
                variable = Some(self.parse_identifier()?);
            } else if self.peek_nth_token(1).token == Token::RParen {
                // This is just "variable" with no labels
                variable = Some(self.parse_identifier()?);
            } else {
                // This might be a variable followed by something else
                variable = Some(self.parse_identifier()?);
            }
        }
    
        // Parse labels
        while self.consume_token(&Token::Colon) {
            labels.push(self.parse_identifier()?);
        }
        
        let properties = if self.consume_token(&Token::LBrace) {
            let props = self.parse_map_literal()?;
            self.expect_token(&Token::RBrace)?;
            Some(props)
        } else {
            None
        };
        
        self.expect_token(&Token::RParen)?;
        
        Ok(PatternElement::Node {
            variable,
            labels,
            properties,
        })
    }

    /// Parse a relationship pattern: -[variable:TYPE*length {properties}]->
    fn parse_cypher_relationship(&mut self) -> Result<PatternElement, ParserError> {
        let direction_left = self.consume_token(&Token::Lt);
        self.expect_token(&Token::Minus)?;
        
        let mut variable = None;
        let mut types = vec![];
        let mut length = None;
        let mut properties = None;
        
        // Parse relationship details if present
        if self.consume_token(&Token::LBracket) {
            // Parse variable name - if we see a word followed by colon, it's "variable:type"
            if matches!(self.peek_token().token, Token::Word(_)) {
                if matches!(self.peek_nth_token(1).token, Token::Colon) {
                    // This is "variable:TYPE" pattern
                    variable = Some(self.parse_identifier()?);
                } else if matches!(self.peek_nth_token(1).token, Token::RBracket) {
                    // This is just "[variable]" with no type
                    variable = Some(self.parse_identifier()?);
                }
                // Note: We don't parse the variable if it's followed by something else
            }
            
            // Parse relationship types
            while self.consume_token(&Token::Colon) {
                types.push(self.parse_identifier()?);
                // Handle multiple types with |
                while self.consume_token(&Token::Pipe) {
                    types.push(self.parse_identifier()?);
                }
            }
            
            // Parse length specification
            if self.consume_token(&Token::Mul) {
                length = Some(self.parse_cypher_relationship_length()?);
            }
            
            // Parse properties
            if self.consume_token(&Token::LBrace) {
                properties = Some(self.parse_map_literal()?);
                self.expect_token(&Token::RBrace)?;
            }
            
            self.expect_token(&Token::RBracket)?;
        }
        
        let direction_right = if self.consume_token(&Token::Arrow) {
            // This handles -> which becomes Token::Arrow
            true
        } else if self.consume_token(&Token::Minus) {
            // This handles plain - followed by optional >
            self.consume_token(&Token::Gt)
        } else {
            return self.expected("relationship direction (- or ->)", self.peek_token());
        };
        
        let direction = match (direction_left, direction_right) {
            (true, true) => RelationshipDirection::Both,
            (true, false) => RelationshipDirection::Left,
            (false, true) => RelationshipDirection::Right,
            (false, false) => RelationshipDirection::None,
        };
        
        Ok(PatternElement::Relationship {
            variable,
            types,
            properties,
            direction,
            length,
        })
    }

    /// Parse relationship length specification: *n or *n..m or *..m or *n.. or *
    fn parse_cypher_relationship_length(&mut self) -> Result<RelationshipLength, ParserError> {
        if let Token::Number(n, _) = &self.peek_token().token {
            let min_val = n.parse::<u64>().map_err(|_| {
                ParserError::ParserError("Invalid number in relationship length".to_string())
            })?;
            self.next_token();
            
            if self.consume_token(&Token::Period) {
                self.expect_token(&Token::Period)?; // ..
                
                if let Token::Number(m, _) = &self.peek_token().token {
                    let max_val = m.parse::<u64>().map_err(|_| {
                        ParserError::ParserError("Invalid number in relationship length".to_string())
                    })?;
                    self.next_token();
                    Ok(RelationshipLength::Range(Some(min_val), Some(max_val)))
                } else {
                    Ok(RelationshipLength::Range(Some(min_val), None))
                }
            } else {
                Ok(RelationshipLength::Exact(min_val))
            }
        } else if self.consume_token(&Token::Period) {
            self.expect_token(&Token::Period)?; // ..
            
            if let Token::Number(m, _) = &self.peek_token().token {
                let max_val = m.parse::<u64>().map_err(|_| {
                    ParserError::ParserError("Invalid number in relationship length".to_string())
                })?;
                self.next_token();
                Ok(RelationshipLength::Range(None, Some(max_val)))
            } else {
                Ok(RelationshipLength::Variable)
            }
        } else {
            Ok(RelationshipLength::Variable)
        }
    }

    /// Parse RETURN clause
    fn parse_cypher_return_clause(&mut self) -> Result<ReturnClause, ParserError> {
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        let items = self.parse_projection()?;
        
        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            vec![self.parse_order_by_expr()?]
        } else {
            vec![]
        };
        
        let skip = if self.parse_keyword(Keyword::SKIP) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        let limit = if self.parse_keyword(Keyword::LIMIT) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(ReturnClause {
            distinct,
            items,
            order_by,
            limit,
            skip,
        })
    }

    /// Parse SET clauses
    fn parse_cypher_set_clauses(&mut self) -> Result<Vec<SetClause>, ParserError> {
        self.parse_comma_separated(|parser| parser.parse_cypher_set_clause())
    }

    /// Parse a single SET clause
    fn parse_cypher_set_clause(&mut self) -> Result<SetClause, ParserError> {
        let target = self.parse_cypher_set_target()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        
        Ok(SetClause { target, value })
    }

    /// Parse SET target (variable.property or variable:Label)
    fn parse_cypher_set_target(&mut self) -> Result<SetTarget, ParserError> {
        let variable = self.parse_identifier()?;
        
        if self.consume_token(&Token::Period) {
            let property = self.parse_identifier()?;
            Ok(SetTarget::Property { variable, property })
        } else if self.consume_token(&Token::Colon) {
            let label = self.parse_identifier()?;
            Ok(SetTarget::Label { variable, label })
        } else {
            Ok(SetTarget::Variable(variable))
        }
    }

    /// Parse a map literal {key: value, ...} - simplified implementation
    fn parse_map_literal(&mut self) -> Result<Expr, ParserError> {
        // For now, treat map literals as function calls with named arguments
        // This is a simplified approach - a full implementation would need
        // proper AST support for maps
        let mut args = vec![];
        
        if !self.consume_token(&Token::RBrace) {
            loop {
                let key = self.parse_identifier()?;
                self.expect_token(&Token::Colon)?;
                let value = self.parse_expr()?;
                
                // Convert to a named argument for now
                args.push(FunctionArg::Named {
                    name: key,
                    arg: FunctionArgExpr::Expr(value),
                    operator: FunctionArgOperator::Colon,
                });
                
                if !self.consume_token(&Token::Comma) {
                    break;
                }
            }
            self.expect_token(&Token::RBrace)?;
        }
        
        // Return as a function call to represent the map
        Ok(Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("MAP"))]),
            parameters: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            args: FunctionArguments::None, // Add this missing field
            uses_odbc_syntax: false, // Add this missing field
        }))
    }
}