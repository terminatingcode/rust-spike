use actix_web::{App, HttpResponse, HttpServer, Result, guard, web};
use async_graphql::{EmptyMutation, EmptySubscription, Schema, http::GraphiQLSource};
use async_graphql_actix_web::GraphQL;
use models::Query;
use sqlx::postgres::PgPoolOptions;

mod models;
mod postgres;

async fn index_graphiql() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(GraphiQLSource::build().endpoint("/").finish()))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://testuser:testpass@db:5432/merchants")
        .await
        .map_err(|e| {
            eprintln!("Failed to connect to Postgres: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to connect to Postgres")
        })?;
    sqlx::migrate!().run(&pool).await.map_err(|e| {
        eprintln!("Failed to run database migrations: {}", e);
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to run database migrations",
        )
    })?;

    postgres::init_db(&pool).await.map_err(|e| {
        eprintln!("Failed to initialize database: {}", e);
        std::io::Error::new(std::io::ErrorKind::Other, "Failed to initialize database")
    })?;

    println!("GraphiQL IDE: http://localhost:8080");

    HttpServer::new(move || {
        let schema = Schema::build(Query, EmptyMutation, EmptySubscription)
            .data(pool.clone())
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
