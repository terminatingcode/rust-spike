use async_graphql::*;
use graphql::Query;

mod graphql;

fn main() {
    println!("Hello, world!");
    trpl::run(async {
        let schema = Schema::new(Query, EmptyMutation, EmptySubscription);
        let res = schema.execute("{ merchants { id name description} }").await;

        let json = serde_json::to_string(&res);
        println!("{}", json.unwrap());
    });

    println!("Goodbye, world!");
}
