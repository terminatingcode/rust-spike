use crate::models::{CardBrand, Merchant, Transaction, TransactionStatus, TransactionType};
use anyhow::{Context, Error};
use aws_sdk_dynamodb::types::{AttributeValue, ScalarAttributeType};
use chrono::{TimeZone, Utc};
use rand::Rng;
use serde_dynamo::aws_sdk_dynamodb_1::{from_item, from_items};
use std::{collections::HashMap, time::SystemTime};

const TABLE_NAME: &str = "merchants";
const PARTITION_KEY: &str = "partition_key";
const SORT_KEY: &str = "sort_key";
const TRANSACTION_PREFIX: &str = "TRANSACTION";
const MERCHANT_PREFIX: &str = "MERCHANT";

pub async fn init_db(client: &aws_sdk_dynamodb::Client) {
    create_table(client, &TABLE_NAME.to_string()).await;

    let mut merchants: Vec<Merchant> = Vec::new();
    let mut rng = rand::thread_rng();

    for i in 1..5 {
        merchants.push(Merchant {
            id: format!("{}#{}", MERCHANT_PREFIX, i),
            name: format!("Merchant {}", i),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            num_employees: 100,
            vat_number: format!("VAT{}", i),
            description: format!("A sample merchant {}", i),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        });
    }

    for merchant in merchants {
        add_merchant(client, &merchant, &TABLE_NAME.to_string())
            .await
            .expect("Failed to add merchant");

        for i in 1..=5 {
            let date = Utc.with_ymd_and_hms(2026, i, i, 0, 0, 0).unwrap();
            add_transaction(
                client,
                Transaction {
                    id: format!("{}#{}#{}", TRANSACTION_PREFIX, date.to_rfc3339(), i),
                    merchant_id: merchant.id.clone(),
                    transaction_type: random_transaction_type(&mut rng),
                    status: random_transaction_status(&mut rng),
                    amount: rng.gen_range(10.0..100.0),
                    fees: rng.gen_range(0.5..5.0),
                    pan: rng.gen_range(4000000000000000i64..5000000000000000i64),
                    card_brand: random_card_brand(&mut rng),
                    created_at: date.timestamp_millis(),
                },
            )
            .await
            .expect("Failed to add transaction");
        }
    }
}

async fn add_merchant(
    client: &aws_sdk_dynamodb::Client,
    merchant: &Merchant,
    table: &String,
) -> Result<(), Error> {
    let id_av = AttributeValue::S(merchant.id.clone());
    let name_av = AttributeValue::S(merchant.name.clone());
    let founded_date_av = AttributeValue::S(merchant.founded_date.clone());
    let industry_av = AttributeValue::S(merchant.industry.clone());
    let num_employees_av = AttributeValue::N(merchant.num_employees.to_string());
    let vat_number_av = AttributeValue::S(merchant.vat_number.clone());
    let description_av = AttributeValue::S(merchant.description.clone());
    let created_at_av = AttributeValue::N(merchant.created_at.to_string());
    let request = client
        .put_item()
        .table_name(table)
        .item(PARTITION_KEY, id_av.clone())
        .item(SORT_KEY, id_av)
        .item("name", name_av)
        .item("founded_date", founded_date_av)
        .item("industry", industry_av)
        .item("num_employees", num_employees_av)
        .item("vat_number", vat_number_av)
        .item("description", description_av)
        .item("created_at", created_at_av);
    println!("👍Adding merchant {0}", merchant.id);

    request.send().await?;
    Ok(())
}

async fn create_table(client: &aws_sdk_dynamodb::Client, table_name: &String) {
    println!("Creating table '{table_name}'...");

    let create_resp = client
        .create_table()
        .table_name(table_name)
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name(PARTITION_KEY)
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .expect("Failed to build partition key KeySchemaElement"),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name(SORT_KEY)
                .key_type(aws_sdk_dynamodb::types::KeyType::Range)
                .build()
                .expect("Failed to build sort key KeySchemaElement"),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(PARTITION_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("Failed to build partition key AttributeDefinition"),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(SORT_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("Failed to build sort key AttributeDefinition"),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await;
    match create_resp {
        Ok(_) => println!("Created table '{table_name}'"),
        Err(err) => eprintln!("Failed to create table {table_name}: {err:?}"),
    }
}

fn random_transaction_type(rng: &mut impl Rng) -> TransactionType {
    match rng.gen_range(0..2) {
        0 => TransactionType::Online,
        _ => TransactionType::Pos,
    }
}

fn random_transaction_status(rng: &mut impl Rng) -> TransactionStatus {
    match rng.gen_range(0..3) {
        0 => TransactionStatus::Pending,
        1 => TransactionStatus::Successful,
        _ => TransactionStatus::Chargeback,
    }
}

fn random_card_brand(rng: &mut impl Rng) -> CardBrand {
    match rng.gen_range(0..4) {
        0 => CardBrand::Visa,
        1 => CardBrand::Mastercard,
        2 => CardBrand::Amex,
        _ => CardBrand::Discover,
    }
}

async fn add_transaction(
    client: &aws_sdk_dynamodb::Client,
    transaction: Transaction,
) -> Result<(), Error> {
    let merchant_id_av = AttributeValue::S(transaction.merchant_id.to_string());
    let id_av = AttributeValue::S(transaction.id);
    let transaction_type_av = AttributeValue::S(transaction.transaction_type.to_string());
    let status_av = AttributeValue::S(transaction.status.to_string());
    let amount_av = AttributeValue::N(transaction.amount.to_string());
    let fees_av = AttributeValue::N(transaction.fees.to_string());
    let pan_av = AttributeValue::N(transaction.pan.to_string());
    let card_brand_av = AttributeValue::S(transaction.card_brand.to_string());
    let created_at_av = AttributeValue::N(transaction.created_at.to_string());
    let request = client
        .put_item()
        .table_name(TABLE_NAME)
        .item(PARTITION_KEY, merchant_id_av)
        .item(SORT_KEY, id_av)
        .item("transaction_type", transaction_type_av)
        .item("status", status_av)
        .item("amount", amount_av)
        .item("fees", fees_av)
        .item("pan", pan_av)
        .item("card_brand", card_brand_av)
        .item("created_at", created_at_av);
    println!("Executing request [{request:?}] to add item...");

    request.send().await?;
    Ok(())
}

pub async fn get_transactions(
    client: &aws_sdk_dynamodb::Client,
    merchant_id: String,
    year: String,
    month: String,
    after: i64,
    before: i64,
    limit: i32,
) -> Result<(Vec<Transaction>, bool), anyhow::Error> {
    let items_resp = client
        .query()
        .table_name(TABLE_NAME)
        .limit(limit)
        .key_condition_expression(
            "#partition_key = :merchant_id AND begins_with(#sort_key, :transaction_prefix)",
        )
        .expression_attribute_names("#partition_key", PARTITION_KEY)
        .expression_attribute_names("#sort_key", SORT_KEY)
        .expression_attribute_values(":merchant_id", AttributeValue::S(merchant_id))
        .expression_attribute_values(
            ":transaction_prefix",
            AttributeValue::S(format!("{}#{}-{}", TRANSACTION_PREFIX, year, month)),
        )
        .filter_expression("#created_at BETWEEN :created_at_after AND :created_at_before")
        .expression_attribute_names("#created_at", "created_at")
        .expression_attribute_values(":created_at_after", AttributeValue::N(after.to_string()))
        .expression_attribute_values(":created_at_before", AttributeValue::N(before.to_string()))
        .send()
        .await
        .context("Failed to get transaction")?;

    if let Some(items) = items_resp.items {
        let mut modified_items = items.clone();
        replace_key_names(&mut modified_items, "merchant_id", "id");
        let transactions: Vec<Transaction> = from_items(modified_items)?;
        return Ok((transactions, items_resp.last_evaluated_key.is_some()));
    }

    Ok((Vec::new(), false))
}

pub async fn get_merchant(
    client: &aws_sdk_dynamodb::Client,
    merchant_id: String,
) -> Result<Merchant, anyhow::Error> {
    let item_resp = client
        .get_item()
        .table_name(TABLE_NAME)
        .key(PARTITION_KEY, AttributeValue::S(merchant_id.clone()))
        .key(SORT_KEY, AttributeValue::S(merchant_id.clone()))
        .send()
        .await
        .context("Failed to get merchant")?;

    return Ok(item_resp
        .item
        .map(|item| {
            let mut modified_items = vec![item.clone()];
            replace_key_names(&mut modified_items, "id", "name");
            let modified_item = modified_items.remove(0);
            from_item(modified_item).context("failed to deserialise merchant")
        })
        .transpose()?
        .context("Merchant not found")?);
}

fn replace_key_names(
    items: &mut Vec<HashMap<String, AttributeValue>>,
    partition_key: &str,
    sort_key: &str,
) {
    for item in items.iter_mut() {
        if let Some(value) = item.remove("partition_key") {
            item.insert(partition_key.to_string(), value);
        }
        if let Some(value) = item.remove("sort_key") {
            item.insert(sort_key.to_string(), value);
        }
    }
}
