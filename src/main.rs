use std::time::SystemTime;

use actix_web::{App, HttpResponse, HttpServer, Result, guard, web};
use async_graphql::{EmptyMutation, EmptySubscription, Schema, http::GraphiQLSource};
use async_graphql_actix_web::GraphQL;
use aws_config::Region;
use aws_sdk_dynamodb::{Error, types::AttributeValue};
use models::{Merchant, Query};

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
                println!("No tables found, creating 'merchants' table...");
                create_merchant_table(&client).await;
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

async fn create_merchant_table(client: &aws_sdk_dynamodb::Client) {
    let create_resp = client
        .create_table()
        .table_name("merchants")
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .expect("Failed to build KeySchemaElement"),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .expect("Failed to build AttributeDefinition"),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await;
    match create_resp {
        Ok(_) => println!("Created table 'merchants'"),
        Err(err) => eprintln!("Failed to create table 'merchants': {err:?}"),
    }

    let _add_resp = add_merchant(
        client,
        Merchant {
            id: "merchant-123".to_string(),
            name: "Uniqlo".to_string(),
            founded_date: "2020-01-01".to_string(),
            industry: "Retail".to_string(),
            num_employees: 100,
            vat_number: "VAT123456".to_string(),
            description: "A sample merchant 3".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        },
        &"merchants".to_string(),
    )
    .await
    .expect("Failed to add merchant");

    let _add_resp = add_merchant(
        client,
        Merchant {
            id: "merchant-456".to_string(),
            name: "Asos".to_string(),
            founded_date: "2021-01-01".to_string(),
            industry: "Retail".to_string(),
            num_employees: 100,
            vat_number: "VAT654321".to_string(),
            description: "A sample merchant 2".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        },
        &"merchants".to_string(),
    )
    .await
    .expect("Failed to add merchant");

    let _add_resp = add_merchant(
        client,
        Merchant {
            id: "merchant-789".to_string(),
            name: "Docker".to_string(),
            founded_date: "2022-01-01".to_string(),
            industry: "Software".to_string(),
            num_employees: 100,
            vat_number: "VAT234567".to_string(),
            description: "A sample merchant 3".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        },
        &"merchants".to_string(),
    )
    .await
    .expect("Failed to add merchant");
}

async fn add_merchant(
    client: &aws_sdk_dynamodb::Client,
    merchant: Merchant,
    table: &String,
) -> Result<(), Error> {
    let id_av = AttributeValue::S(merchant.id);
    let name_av = AttributeValue::S(merchant.name);
    let founded_date_av = AttributeValue::S(merchant.founded_date);
    let industry_av = AttributeValue::S(merchant.industry);
    let num_employees_av = AttributeValue::N(merchant.num_employees.to_string());
    let vat_number_av = AttributeValue::S(merchant.vat_number);
    let description_av = AttributeValue::S(merchant.description);
    let created_at_av = AttributeValue::N(merchant.created_at.to_string());
    let request = client
        .put_item()
        .table_name(table)
        .item("id", id_av)
        .item("name", name_av)
        .item("founded_date", founded_date_av)
        .item("industry", industry_av)
        .item("num_employees", num_employees_av)
        .item("vat_number", vat_number_av)
        .item("description", description_av)
        .item("created_at", created_at_av);
    println!("Executing request [{request:?}] to add item...");

    request.send().await?;
    Ok(())
}
