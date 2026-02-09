use std::env;
use std::time::SystemTime;

use async_graphql::types::connection::{Connection, Edge, EmptyFields, OpaqueCursor, query};
use async_graphql::{Context, Enum, Guard, Object, SimpleObject};
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
    ) -> (Result<Vec<Merchant>, aws_sdk_dynamodb::Error>, bool) {
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
        let items = items_resp.items.unwrap_or_else(|| Vec::new());

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
    ) -> (Result<Vec<Transaction>, aws_sdk_dynamodb::Error>, bool) {
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
            .map_err(|err| {
                println!("Error scanning DynamoDB: {err}");
                err
            });

        let items_resp = items_resp.unwrap();
        let items = items_resp.items.unwrap_or_else(|| Vec::new());

        let transactions = items
            .into_iter()
            .map(|item| Transaction {
                merchant_id: item
                    .get("merchant_id")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                id: item
                    .get("id")
                    .and_then(|attr| attr.as_s().ok())
                    .unwrap_or(&String::new())
                    .to_string(),
                transaction_type: item
                    .get("transaction_type")
                    .and_then(|attr| attr.as_s().ok())
                    .and_then(|type_str| match type_str.as_str() {
                        "Online" => Some(TransactionType::Online),
                        "Pos" => Some(TransactionType::Pos),
                        _ => None,
                    })
                    .unwrap_or(TransactionType::Online),
                status: item
                    .get("status")
                    .and_then(|attr| attr.as_s().ok())
                    .and_then(|status_str| match status_str.as_str() {
                        "Pending" => Some(TransactionStatus::Pending),
                        "Successful" => Some(TransactionStatus::Successful),
                        "Chargeback" => Some(TransactionStatus::Chargeback),
                        "PaidOut" => Some(TransactionStatus::PaidOut),
                        _ => None,
                    })
                    .unwrap_or(TransactionStatus::Pending),
                amount: item
                    .get("amount")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<f64>().ok())
                    .unwrap_or(0.0),
                fees: item
                    .get("fees")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<f64>().ok())
                    .unwrap_or(0.0),
                pan: item
                    .get("pan")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<i64>().ok())
                    .unwrap_or(0),
                card_brand: item
                    .get("card_brand")
                    .and_then(|attr| attr.as_s().ok())
                    .and_then(|brand_str| match brand_str.as_str() {
                        "Visa" => Some(CardBrand::Visa),
                        "Mastercard" => Some(CardBrand::Mastercard),
                        "Amex" => Some(CardBrand::Amex),
                        "Discover" => Some(CardBrand::Discover),
                        _ => None,
                    })
                    .unwrap_or(CardBrand::Visa),
                created_at: item
                    .get("created_at")
                    .and_then(|attr| attr.as_n().ok())
                    .and_then(|num_str| num_str.parse::<i64>().ok())
                    .unwrap_or(0),
            })
            .collect();
        (Ok(transactions), items_resp.last_evaluated_key.is_some())
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
                let (transaction_result, has_more) = Transaction::read_all(client, merchant_id, after.1, before.1, limit)
                    .await;
                let transactions = transaction_result.unwrap_or_default();
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
    async fn check(&self, ctx: &Context<'_>) -> Result<(), async_graphql::Error> {
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
