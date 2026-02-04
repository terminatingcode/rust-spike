use async_graphql::*;

struct Query;

#[Object]
impl Query {
    async fn hello(&self, name: String) -> String {
        format!("Hello {}", name)
    }
}

fn main() {
    println!("Hello, world!");
    trpl::run(async {
        let schema = Schema::new(Query, EmptyMutation, EmptySubscription);
        let res = schema.execute("{ hello(name: \"Sarah\") }").await;

        let json = serde_json::to_string(&res);
        println!("{}", json.unwrap());
    });

    println!("Goodbye, world!");
}
