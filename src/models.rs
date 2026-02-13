use anyhow::Error;
use chrono::{TimeZone, Utc};
use std::env;

use async_graphql::types::connection::{Connection, Edge, EmptyFields, OpaqueCursor, query};
use async_graphql::{Enum, Guard, Object, SimpleObject};
use serde::{Deserialize, Serialize};
use strum_macros::Display;

use crate::postgres::{get_merchant_by_id, get_transactions_for_merchant};

#[derive(SimpleObject, Deserialize, Serialize, sqlx::FromRow)]
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

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display, sqlx::Type)]
#[sqlx(type_name = "transaction_type_enum", rename_all = "snake_case")]
pub enum TransactionType {
    Online,
    Pos,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display, sqlx::Type)]
#[sqlx(type_name = "transaction_status_enum", rename_all = "snake_case")]
pub enum TransactionStatus {
    Pending,
    Successful,
    Chargeback,
    PaidOut,
}

#[derive(Enum, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Display, sqlx::Type)]
#[sqlx(type_name = "card_brand_enum", rename_all = "snake_case")]
pub enum CardBrand {
    Visa,
    Mastercard,
    Amex,
    Discover,
}

#[derive(SimpleObject, Deserialize, Serialize, sqlx::FromRow)]
pub struct Transaction {
    pub merchant_id: String,
    pub id: String,
    pub transaction_type: TransactionType,
    pub status: TransactionStatus,
    pub amount: f64,
    pub fees: f64,
    pub pan: String,
    pub card_brand: CardBrand,
    pub created_at: i64,
}

impl Transaction {
    pub async fn read_all(
        pool: &sqlx::PgPool,
        merchant_id: String,
        after: i64,
        after_id: String,
        before: i64,
        before_id: String,
        limit: i64,
    ) -> Result<(Vec<Transaction>, bool), Error> {
        let (transactions, has_more) = get_transactions_for_merchant(
            pool,
            merchant_id,
            after,
            after_id,
            before,
            before_id,
            limit,
        )
        .await?;
        Ok((transactions, has_more))
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
        get_merchant_by_id(ctx.data::<sqlx::PgPool>().unwrap(), merchant_id)
            .await
            .map_err(|e| {
                eprintln!("Failed to fetch merchant: {}", e);
                "Failed to fetch merchant".into()
            })
    }

    async fn transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        merchant_id: String,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
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
                let before = before.map(|c| c.0).unwrap_or((String::new(), 0));
                let after = after.map(|c| c.0).unwrap_or((
                    String::new(),
                    Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).single().unwrap().timestamp_millis(),
                ));
                let limit = first.unwrap_or(last.unwrap_or(10)) as i64;
                println!("Received pagination parameters: after={after:?}, before={before:?}, first={limit}, last={last:?}");
                let pool: &sqlx::PgPool = ctx.data::<sqlx::PgPool>().unwrap();
                let (transactions, has_more) =
                    Transaction::read_all(pool, merchant_id, after.1, after.0, before.1, before.0, limit).await?;
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
