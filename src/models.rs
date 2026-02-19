use crate::dynamo::{get_merchant, get_transactions, get_transactions_for_settlement_merchant};
use anyhow::Error;
use std::env;

use async_graphql::types::connection::{Connection, Edge, EmptyFields, OpaqueCursor, query};
use async_graphql::{Enum, Guard, Object, SimpleObject};
use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum MerchantLevel {
    Group,
    Chain,
    Outlet,
}

#[derive(SimpleObject, Deserialize, Serialize)]
pub struct Merchant {
    pub id: String,
    pub name: String,
    pub founded_date: String,
    pub industry: String,
    pub vat_number: String,
    pub created_at: i64,
    pub merchant_level: MerchantLevel,
    pub sub_merchants: Vec<String>,
    pub has_settlement_permissions: bool,
    pub has_billing_permissions: bool,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum TransactionType {
    Purchase,
    Refund,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum TransactionStatus {
    Processed,
    Cleared,
    Chargebacked,
    Paid,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display)]
pub enum CardBrand {
    Visa,
    Mastercard,
}

#[derive(SimpleObject, Deserialize, Serialize)]
pub struct Transaction {
    pub id: String,
    pub merchant_id: String,
    pub date_transaction: String,
    pub date_settlement: String,
    pub transaction_type: TransactionType,
    pub status: TransactionStatus,
    pub amount: f64,
    pub currency: String,
    pub pan: String,
    pub card_brand: CardBrand,
    pub payout_id: String,
    pub settlement_merchant_id: String,
}

#[derive(SimpleObject, Deserialize, Serialize)]
pub struct Payout {
    pub id: String,
    pub merchant_id: String,
    pub date_transaction: String,
    pub date_settlement: String,
    pub status: TransactionStatus,
    pub amount: f64,
    pub currency: String,
    pub bank_account: String,
    pub bank_name: String,
}

impl Transaction {
    pub async fn read_all(
        client: &aws_sdk_dynamodb::Client,
        merchant_id: String,
        year: Option<String>,
        month: Option<String>,
        day: Option<String>,
        card_brand: Option<CardBrand>,
        after: Option<String>,
        before: Option<String>,
        limit: i32,
    ) -> Result<(Vec<Transaction>, bool), Error> {
        get_transactions(
            client,
            merchant_id,
            year,
            month,
            day,
            card_brand,
            after,
            before,
            limit,
        )
        .await
    }

    pub async fn read_all_for_settlement_merchant(
        client: &aws_sdk_dynamodb::Client,
        settlement_merchant_id: String,
        after: Option<String>,
        before: Option<String>,
        limit: i32,
    ) -> Result<(Vec<Transaction>, bool), Error> {
        get_transactions_for_settlement_merchant(
            client,
            settlement_merchant_id,
            after,
            before,
            limit,
        )
        .await
    }
}

pub struct Query;

#[Object]
impl Query {
    #[graphql(guard = "RoleGuard::new(Role::Admin).or(RoleGuard::new(Role::Reader))")]
    async fn merchant(
        &self,
        ctx: &async_graphql::Context<'_>,
        merchant_id: String,
    ) -> Result<Merchant, async_graphql::Error> {
        let client: &aws_sdk_dynamodb::Client = ctx.data::<aws_sdk_dynamodb::Client>().unwrap();
        Ok(get_merchant(client, merchant_id).await?)
    }

    async fn transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        merchant_id: String,
        year: Option<String>,
        month: Option<String>,
        day: Option<String>,
        card_brand: Option<CardBrand>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<
        Connection<OpaqueCursor<String>, Transaction, EmptyFields, EmptyFields>,
        async_graphql::Error,
    > {
        query(
            after,
            before,
            first,
            last,
            |after: Option<OpaqueCursor<String>>,
             before: Option<OpaqueCursor<String>>,
             first: Option<usize>,
             last: Option<usize>| async move {
                let has_prev_page = after.is_some();
                let after: Option<String> = after.map(|c| c.0);
                let before = before.map(|c| c.0);
                let limit = first.unwrap_or(last.unwrap_or(10)) as i32;

                let client: &aws_sdk_dynamodb::Client =
                    ctx.data::<aws_sdk_dynamodb::Client>().unwrap();
                let (transactions, has_more) = Transaction::read_all(
                    client,
                    merchant_id,
                    year,
                    month,
                    day,
                    card_brand,
                    after,
                    before,
                    limit,
                )
                .await?;
                let mut connection = Connection::new(has_prev_page, has_more);
                connection.edges = transactions
                    .into_iter()
                    .map(|transaction| Edge::new(OpaqueCursor(transaction.id.clone()), transaction))
                    .collect();
                Ok::<_, async_graphql::Error>(connection)
            },
        )
        .await
    }

    async fn transactions_for_settlement_merchant(
        &self,
        ctx: &async_graphql::Context<'_>,
        settlement_merchant_id: String,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<
        Connection<OpaqueCursor<String>, Transaction, EmptyFields, EmptyFields>,
        async_graphql::Error,
    > {
        query(
            after,
            before,
            first,
            last,
            |after: Option<OpaqueCursor<String>>,
             before: Option<OpaqueCursor<String>>,
             first: Option<usize>,
             last: Option<usize>| async move {
                let has_prev_page = after.is_some();
                let after: Option<String> = after.map(|c| c.0);
                let before = before.map(|c| c.0);
                let limit = first.unwrap_or(last.unwrap_or(10)) as i32;

                let client: &aws_sdk_dynamodb::Client =
                    ctx.data::<aws_sdk_dynamodb::Client>().unwrap();
                let (transactions, has_more) = Transaction::read_all_for_settlement_merchant(
                    client,
                    settlement_merchant_id,
                    after,
                    before,
                    limit,
                )
                .await?;
                let mut connection = Connection::new(has_prev_page, has_more);
                connection.edges = transactions
                    .into_iter()
                    .map(|transaction| Edge::new(OpaqueCursor(transaction.id.clone()), transaction))
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
