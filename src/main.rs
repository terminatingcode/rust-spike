use async_graphql::{Schema, EmptyMutation, EmptySubscription, http::GraphiQLSource};
use models::Query;
use async_graphql_actix_web::GraphQL;
use actix_web::{web, App, HttpServer, HttpResponse, HttpRequest, guard, Result};

mod models;

async fn index_graphiql() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(GraphiQLSource::build().endpoint("/").finish()))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("GraphiQL IDE: http://localhost:8000");

    HttpServer::new(move || {
        let schema = Schema::build(Query, EmptyMutation, EmptySubscription)
            .finish();
        App::new()
            .service(
                web::resource("/")
                .guard(guard::Post())
                .to(GraphQL::new(schema.clone()))
            )
            .service(web::resource("/").guard(guard::Get()).to(index_graphiql))
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}
