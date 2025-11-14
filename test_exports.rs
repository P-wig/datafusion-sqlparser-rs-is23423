fn main() {
    // This should compile if the transformer module is properly exported
    use sqlparser::transformer::transform_cypher_to_sql;
    
    let result = transform_cypher_to_sql("MATCH (n:Person) RETURN n.name");
    println!("Result: {:?}", result);
}