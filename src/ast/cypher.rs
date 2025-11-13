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

//! Cypher language AST nodes for graph query support

#[cfg(not(feature = "std"))]
use alloc::{vec, vec::Vec};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{display_comma_separated, Expr, Ident, OrderByExpr, SelectItem};

/// A Cypher statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CypherStatement {
    /// MATCH pattern [WHERE condition] [RETURN items]
    Match {
        optional: bool,
        patterns: Vec<Pattern>,
        r#where: Option<Expr>,
        r#return: Option<ReturnClause>,
    },
    /// CREATE pattern
    Create {
        patterns: Vec<Pattern>,
    },
    /// MERGE pattern [ON CREATE SET] [ON MATCH SET]
    Merge {
        patterns: Vec<Pattern>,
        on_create: Option<Vec<SetClause>>,
        on_match: Option<Vec<SetClause>>,
    },
    /// DELETE nodes/relationships [WHERE condition]
    Delete {
        detach: bool,
        what: Vec<Expr>,
        r#where: Option<Expr>,
    },
}

impl fmt::Display for CypherStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CypherStatement::Match {
                optional,
                patterns,
                r#where,
                r#return,
            } => {
                if *optional {
                    write!(f, "OPTIONAL ")?;
                }
                write!(f, "MATCH {}", display_comma_separated(patterns))?;
                if let Some(condition) = r#where {
                    write!(f, " WHERE {condition}")?;
                }
                if let Some(ret) = r#return {
                    write!(f, " {ret}")?;
                }
                Ok(())
            }
            CypherStatement::Create { patterns } => {
                write!(f, "CREATE {}", display_comma_separated(patterns))
            }
            CypherStatement::Merge {
                patterns,
                on_create,
                on_match,
            } => {
                write!(f, "MERGE {}", display_comma_separated(patterns))?;
                if let Some(create_clauses) = on_create {
                    write!(f, " ON CREATE SET {}", display_comma_separated(create_clauses))?;
                }
                if let Some(match_clauses) = on_match {
                    write!(f, " ON MATCH SET {}", display_comma_separated(match_clauses))?;
                }
                Ok(())
            }
            CypherStatement::Delete {
                detach,
                what,
                r#where,
            } => {
                if *detach {
                    write!(f, "DETACH ")?;
                }
                write!(f, "DELETE {}", display_comma_separated(what))?;
                if let Some(condition) = r#where {
                    write!(f, " WHERE {condition}")?;
                }
                Ok(())
            }
        }
    }
}

/// A pattern describes nodes and relationships in a graph
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, element) in self.elements.iter().enumerate() {
            if i > 0 {
                write!(f, "-")?;
            }
            write!(f, "{element}")?;
        }
        Ok(())
    }
}

/// Elements that can appear in a pattern
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PatternElement {
    Node {
        variable: Option<Ident>,
        labels: Vec<Ident>,
        properties: Option<Expr>,
    },
    Relationship {
        variable: Option<Ident>,
        types: Vec<Ident>,
        properties: Option<Expr>,
        direction: RelationshipDirection,
        length: Option<RelationshipLength>,
    },
}

impl fmt::Display for PatternElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PatternElement::Node {
                variable,
                labels,
                properties,
            } => {
                write!(f, "(")?;
                if let Some(var) = variable {
                    write!(f, "{var}")?;
                }
                for label in labels {
                    write!(f, ":{label}")?;
                }
                if let Some(props) = properties {
                    write!(f, " {props}")?;
                }
                write!(f, ")")
            }
            PatternElement::Relationship {
                variable,
                types,
                properties,
                direction,
                length,
            } => {
                match direction {
                    RelationshipDirection::Left => write!(f, "<")?,
                    RelationshipDirection::Both => write!(f, "<")?,
                    _ => {}
                }
                write!(f, "[")?;
                if let Some(var) = variable {
                    write!(f, "{var}")?;
                }
                for (i, rel_type) in types.iter().enumerate() {
                    if i == 0 {
                        write!(f, ":")?;
                    } else {
                        write!(f, "|")?;
                    }
                    write!(f, "{rel_type}")?;
                }
                if let Some(len) = length {
                    write!(f, "{len}")?;
                }
                if let Some(props) = properties {
                    write!(f, " {props}")?;
                }
                write!(f, "]")?;
                match direction {
                    RelationshipDirection::Right => write!(f, ">")?,
                    RelationshipDirection::Both => write!(f, ">")?,
                    _ => {}
                }
                Ok(())
            }
        }
    }
}

/// Direction of a relationship
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RelationshipDirection {
    Left,    // <-[:TYPE]-
    Right,   // -[:TYPE]->
    Both,    // <-[:TYPE]->
    None,    // -[:TYPE]-
}

/// Relationship path length specification
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RelationshipLength {
    /// Exactly n steps: *n
    Exact(u64),
    /// From min to max steps: *min..max
    Range(Option<u64>, Option<u64>),
    /// Any length: *
    Variable,
}

impl fmt::Display for RelationshipLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RelationshipLength::Exact(n) => write!(f, "*{n}"),
            RelationshipLength::Range(min, max) => {
                write!(f, "*")?;
                if let Some(min_val) = min {
                    write!(f, "{min_val}")?;
                }
                write!(f, "..")?;
                if let Some(max_val) = max {
                    write!(f, "{max_val}")?;
                }
                Ok(())
            }
            RelationshipLength::Variable => write!(f, "*"),
        }
    }
}

/// RETURN clause in Cypher queries
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub skip: Option<Expr>,
}

impl fmt::Display for ReturnClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RETURN")?;
        if self.distinct {
            write!(f, " DISTINCT")?;
        }
        write!(f, " {}", display_comma_separated(&self.items))?;
        
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        
        if let Some(skip) = &self.skip {
            write!(f, " SKIP {skip}")?;
        }
        
        if let Some(limit) = &self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        
        Ok(())
    }
}

/// SET clause for updating properties
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetClause {
    pub target: SetTarget,
    pub value: Expr,
}

impl fmt::Display for SetClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.target, self.value)
    }
}

/// Target for SET operations
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetTarget {
    /// Set a property: variable.property
    Property {
        variable: Ident,
        property: Ident,
    },
    /// Set entire node/relationship: variable
    Variable(Ident),
    /// Add label: variable:Label
    Label {
        variable: Ident,
        label: Ident,
    },
}

impl fmt::Display for SetTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetTarget::Property { variable, property } => {
                write!(f, "{variable}.{property}")
            }
            SetTarget::Variable(var) => write!(f, "{var}"),
            SetTarget::Label { variable, label } => {
                write!(f, "{variable}:{label}")
            }
        }
    }
}