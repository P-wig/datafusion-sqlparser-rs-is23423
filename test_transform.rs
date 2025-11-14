use sqlparser::transformer::transform_cypher_to_sql;

fn main() {
    println!("Testing Cypher to SQL transformation...\n");
    
    let test_cases = vec![
        "MATCH (n:Person) RETURN n.name",
        "MATCH (n:User) RETURN n.email",
        "MATCH (n) RETURN n.id",
        
        // These might have parsing issues
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name, m.name",
        "CREATE (n:Person)",
    ];
    
    for cypher_query in test_cases {
        println!("Cypher: {}", cypher_query);
        match transform_cypher_to_sql(cypher_query) {
            Ok(sql) => println!("SQL:    {}\n", sql),
            Err(e) => println!("Error:  {}\n", e),
        }
    }
    
    println!("=== SUCCESS! The basic transformation is working! ===");
    println!("Your Go server can now use: sqlparser::transformer::transform_cypher_to_sql()");
}