use anyhow::{Context, Error};
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use std::env;
use std::time::SystemTime;

use async_graphql::types::connection::{Connection, Edge, EmptyFields, OpaqueCursor, query};
use async_graphql::{Enum, Guard, Object, SimpleObject};
use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(SimpleObject, Deserialize, Serialize)]
pub struct Merchant {
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub id: String,
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub name: String,
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub founded_date: String,
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub industry: String,
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub num_employees: i32,
    #[graphql(guard = "RoleGuard::new(Role::Admin)")]
    pub vat_number: String,
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub description: String,
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    pub created_at: i64,
}

impl Merchant {
    pub async fn read_all(
        client: &aws_sdk_dynamodb::Client,
        after: i64,
        before: i64,
        limit: i32,
    ) -> Result<(Vec<Merchant>, bool), Error> {
        let items_resp = client
            .scan()
            .table_name("merchants")
            .limit(limit)
            .filter_expression("#created_at BETWEEN :created_at_after AND :created_at_before")
            .expression_attribute_names("#created_at", "created_at")
            .expression_attribute_values(":created_at_after", AttributeValue::N(after.to_string()))
            .expression_attribute_values(
                ":created_at_before",
                AttributeValue::N(before.to_string()),
            )
            .send()
            .await
            .context("failed to query merchants")?;

        if let Some(items) = items_resp.items {
            let merchants: Vec<Merchant> = from_items(items.to_vec())?;
            return Ok((merchants, items_resp.last_evaluated_key.is_some()));
        }

        Ok((Vec::new(), false))
    }
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum TransactionType {
    Online,
    Pos,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum TransactionStatus {
    Pending,
    Successful,
    Chargeback,
    PaidOut,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum CardBrand {
    Visa,
    Mastercard,
    Amex,
    Discover,
}

#[derive(SimpleObject, Deserialize, Serialize)]
pub struct Transaction {
    pub merchant_id: String,
    pub id: String,
    pub transaction_type: TransactionType,
    pub status: TransactionStatus,
    pub amount: f64,
    pub fees: f64,
    pub pan: i64,
    pub card_brand: CardBrand,
    pub created_at: i64,
}

impl Transaction {
    pub async fn read_all(
        client: &aws_sdk_dynamodb::Client,
        merchant_id: String,
        after: i64,
        before: i64,
        limit: i32,
    ) -> Result<(Vec<Transaction>, bool), Error> {
        let items_resp = client
            .query()
            .table_name("transactions")
            .limit(limit)
            .key_condition_expression("#merchant_id = :merchant_id AND #created_at BETWEEN :created_at_after AND :created_at_before")
            .expression_attribute_names("#merchant_id", "merchant_id")
            .expression_attribute_names("#created_at", "created_at")
            .expression_attribute_values(":merchant_id", AttributeValue::S(merchant_id))
            .expression_attribute_values(":created_at_after", AttributeValue::N(after.to_string()))
            .expression_attribute_values(":created_at_before", AttributeValue::N(before.to_string()))
            .send()
            .await
            .context("failed to query transactions")?;

        if let Some(items) = items_resp.items {
            let transactions: Vec<Transaction> = from_items(items.to_vec())?;
            return Ok((transactions, items_resp.last_evaluated_key.is_some()));
        }

        Ok((Vec::new(), false))
    }
}

pub struct Query;

#[Object]
impl Query {
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
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
                let (merchants, has_more): (Vec<Merchant>, bool) = Merchant::read_all(client, after.1, before.1, limit)
                    .await?;
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

    async fn transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        merchant_id: String,
    ) -> Result<
        Connection<OpaqueCursor<(String, i64)>, Transaction, EmptyFields, EmptyFields>,
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
                let (transactions, has_more) = Transaction::read_all(client, merchant_id, after.1, before.1, limit)
                    .await?;
                let mut connection = Connection::new(has_prev_page, has_more);
                connection.edges = transactions
                    .into_iter()
                    .map(|transaction| {
                        Edge::new(
                            OpaqueCursor((transaction.id.clone(), transaction.created_at.clone())),
                            transaction,
                        )
                    })
                    .collect();
                Ok::<_, async_graphql::Error>(connection)
            },
        )
        .await
    }
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum Role {
    Admin,
    Reader,
}

struct RoleGuard {
    role: Role,
}

impl RoleGuard {
    fn new(role: Role) -> Self {
        Self { role }
    }
}

impl Guard for RoleGuard {
    async fn check(&self, ctx: &async_graphql::Context<'_>) -> Result<(), async_graphql::Error> {
        let env_role = env::var("ROLE").ok();
        let env_role_parsed = env_role.as_deref().and_then(|role_str| match role_str {
            "Admin" => Some(Role::Admin),
            "Reader" => Some(Role::Reader),
            _ => None,
        });

        if ctx.data_opt::<Role>() == Some(&self.role) || env_role_parsed == Some(self.role) {
            Ok(())
        } else {
            Err("Forbidden".into())
        }
    }
}
