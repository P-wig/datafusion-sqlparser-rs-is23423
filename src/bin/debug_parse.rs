use sqlparser::dialect::CypherDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

fn main() {
    let cypher_query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name";
    println!("Testing: {}", cypher_query);
    
    let dialect = CypherDialect;
    let tokens = Tokenizer::new(&dialect, cypher_query).tokenize().unwrap();
    
    println!("Tokens:");
    for (i, token) in tokens.iter().enumerate() {
        println!("  {}: {:?}", i, token);
    }
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    match parser.parse_cypher_statement() {
        Ok(stmt) => println!("Parsed successfully: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}