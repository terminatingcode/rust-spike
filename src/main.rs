use crate::dynamo::{init_merchants, init_transactions};
use actix_web::{App, HttpResponse, HttpServer, Result, guard, web};
use async_graphql::{EmptyMutation, EmptySubscription, Schema, http::GraphiQLSource};
use async_graphql_actix_web::GraphQL;
use aws_config::Region;
use models::Query;

mod dynamo;
mod models;

async fn index_graphiql() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(GraphiQLSource::build().endpoint("/").finish()))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .test_credentials()
        .region(Region::new("eu-west-1"))
        .endpoint_url("http://localhost:8000")
        .load()
        .await;
    let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config).build();

    let client = aws_sdk_dynamodb::Client::from_conf(dynamodb_local_config);

    let list_resp = client.list_tables().send().await;
    match list_resp {
        Ok(resp) => {
            println!("Found {} tables", resp.table_names().len());
            for name in resp.table_names() {
                println!("  {}", name);
            }
            if resp.table_names().is_empty() {
                println!("No tables found, initializing db...");
                init_merchants(&client).await;
                init_transactions(&client).await;
            }
        }
        Err(err) => eprintln!("Failed to list local dynamodb tables: {err:?}"),
    }

    println!("GraphiQL IDE: http://localhost:8080");

    HttpServer::new(move || {
        let schema = Schema::build(Query, EmptyMutation, EmptySubscription)
            .data(client.clone())
            .finish();
        App::new()
            .service(
                web::resource("/")
                    .guard(guard::Post())
                    .to(GraphQL::new(schema.clone())),
            )
            .service(web::resource("/").guard(guard::Get()).to(index_graphiql))
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
