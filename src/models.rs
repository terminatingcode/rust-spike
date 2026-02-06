use std::time::SystemTime;

use async_graphql::types::connection::{Connection, Edge, EmptyFields, OpaqueCursor, query};
use async_graphql::{Object, SimpleObject};
use aws_sdk_dynamodb::types::AttributeValue;
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
    pub created_at: i64,
}

impl Merchant {
    pub async fn read_all(
        client: &aws_sdk_dynamodb::Client,
        after: i64,
        before: i64,
        limit: i32,
    ) -> (Result<Vec<Merchant>, aws_sdk_dynamodb::Error>, bool) {
        println!(
            "Reading merchants with created_at > {after} and created_at < {before}, limit {limit}"
        );
        let items_resp = client
            .scan()
            .table_name("merchants")
            .limit(limit)
            .filter_expression(
                "#created_at > :created_at_after AND #created_at < :created_at_before",
            )
            .expression_attribute_names("#created_at", "created_at")
            .expression_attribute_values(":created_at_after", AttributeValue::N(after.to_string()))
            .expression_attribute_values(
                ":created_at_before",
                AttributeValue::N(before.to_string()),
            )
            .send()
            .await
            .map_err(|err| {
                println!("Error scanning DynamoDB: {err}");
                err
            });

        let items_resp = items_resp.unwrap();
        println!(
            "Received response from DynamoDB: {0:?}",
            items_resp.last_evaluated_key
        );
        let items = items_resp.items.unwrap_or_else(|| Vec::new());
        println!("Received items from DynamoDB: {items:?}");

        let merchants = items
            .into_iter()
            .map(|item| Merchant {
                id: item
                    .get("id")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                name: item
                    .get("name")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                founded_date: item
                    .get("founded_date")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                industry: item
                    .get("industry")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                num_employees: item
                    .get("num_employees")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<i32>().ok())
                    .unwrap_or(0),
                vat_number: item
                    .get("vat_number")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                description: item
                    .get("description")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                created_at: item
                    .get("created_at")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<i64>().ok())
                    .unwrap_or(0),
            })
            .collect();
        (Ok(merchants), items_resp.last_evaluated_key.is_some())
    }
}

pub struct Query;

#[Object]
impl Query {
    async fn merchants(
        &self,
        ctx: &async_graphql::Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<
        Connection<OpaqueCursor<(String, i64)>, Merchant, EmptyFields, EmptyFields>,
        async_graphql::Error,
    > {
        query(
            after,
            before,
            first,
            last,
            |after: Option<OpaqueCursor<(String, i64)>>,
             before: Option<OpaqueCursor<(String, i64)>>,
             first: Option<usize>,
             last: Option<usize>| async move {
                let has_prev_page = after.is_some();
                let after = after.map(|c| c.0).unwrap_or((String::new(), 0));
                let before = before.map(|c| c.0).unwrap_or((
                    String::new(),
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                ));
                let limit = first.unwrap_or(last.unwrap_or(10)) as i32;
                println!("Received pagination parameters: after={after:?}, before={before:?}, first={limit}, last={last:?}");
                let client: &aws_sdk_dynamodb::Client =
                    ctx.data::<aws_sdk_dynamodb::Client>().unwrap();
                let (merchants_result, has_more) = Merchant::read_all(client, after.1, before.1, limit)
                    .await;
                let merchants = merchants_result.unwrap_or_default();
                let mut connection = Connection::new(has_prev_page, has_more);
                connection.edges = merchants
                    .into_iter()
                    .map(|merchant| {
                        Edge::new(
                            OpaqueCursor((merchant.id.clone(), merchant.created_at.clone())),
                            merchant,
                        )
                    })
                    .collect();
                Ok::<_, async_graphql::Error>(connection)
            },
        )
        .await
    }
}
