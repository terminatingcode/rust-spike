use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};

#[derive(SimpleObject, Deserialize, Serialize)]
pub struct Merchant {
    pub id: String,
    pub name: String,
    pub founded_date: String,
    pub industry: String,
    pub num_employees: i32,
    pub vat_number: String,
    pub description: String,
}

impl Merchant {
    pub async fn read_all (client: &aws_sdk_dynamodb::Client) -> Result<Vec<Merchant>, aws_sdk_dynamodb::Error> {
        let items: Result<Vec<_>, _> = client
        .scan()
        .table_name("merchants")
        .limit(10)
        .into_paginator()
        .items()
        .send()
        .collect()
        .await;

        println!("Received items from DynamoDB: {items:?}");

        let merchants = items?
            .into_iter()
            .map(|item| Merchant {
                id: item.get("id")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                name: item.get("name")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                founded_date: item.get("founded_date")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                industry: item.get("industry")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                num_employees: item.get("num_employees")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<i32>().ok())
                    .unwrap_or(0),
                vat_number: item.get("vat_number")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                description: item.get("description")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
            })
            .collect();
        Ok(merchants)
    }
}

pub struct Query;

#[async_graphql::Object]
impl Query {
    async fn merchants(&self, ctx: &async_graphql::Context<'_>) -> Vec<Merchant> {
        let client = ctx.data::<aws_sdk_dynamodb::Client>().unwrap();
        Merchant::read_all(client)
            .await
            .unwrap_or_default()
    }
}
