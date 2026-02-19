use crate::models::{
    CardBrand, Merchant, MerchantLevel, Transaction, TransactionStatus, TransactionType,
};
use anyhow::{Context, Error};
use aws_sdk_dynamodb::types::{AttributeValue, ScalarAttributeType};
use chrono::{Datelike, TimeZone, Utc};
use rand::Rng;
use serde_dynamo::aws_sdk_dynamodb_1::{from_item, from_items};
use std::{collections::HashMap, time::SystemTime, vec};

const TABLE_NAME: &str = "merchants";
const PARTITION_KEY: &str = "pk";
const SORT_KEY: &str = "sk";
const GSI1_PARTITION_KEY: &str = "gsi1_pk";
const GSI1_SORT_KEY: &str = "gsi1_sk";
const TRANSACTION_PREFIX: &str = "TRANSACTION";
const MERCHANT_PREFIX: &str = "MERCHANT";
const PAYOUT_PREFIX: &str = "PAYOUT";

/*
A merchant_a_outlet_sb (outlet) S,B

B                      merchant_b_group_s (group) S
merchant_b_outlet1_b (outlet) B,       merchant_b_outlet2_b (outlet) B

C                                                       merchant_c_group_b (group) B
               merchant_c_chain1_s (chain) S                                             merchant_c_chain2 (chain)
merchant_c_outlet1 (outlet),     merchant_c_outlet2 (outlet),       merchant_c_outlet3_s (outlet) S      merchant_c_outlet4_s (outlet) S
*/

pub async fn init_db(client: &aws_sdk_dynamodb::Client) {
    create_table(client, &TABLE_NAME.to_string()).await;

    let mut merchants: Vec<Merchant> = Vec::new();
    let mut rng = rand::thread_rng();

    let a_outlet = Merchant {
        id: format!("{}#merchant_a_outlet_sb", MERCHANT_PREFIX),
        name: "Merchant A_outlet_sb".to_string(),
        founded_date: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        industry: "Retail".to_string(),
        vat_number: "VAT123".to_string(),
        created_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        merchant_level: MerchantLevel::Outlet,
        sub_merchants: Vec::new(),
        has_settlement_permissions: true,
        has_billing_permissions: true,
    };

    merchants.push(Merchant {
        id: format!("{}#merchant_b_group_s", MERCHANT_PREFIX),
        name: "Merchant B_group_s".to_string(),
        founded_date: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        industry: "Retail".to_string(),
        vat_number: "VAT123".to_string(),
        created_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        merchant_level: MerchantLevel::Group,
        sub_merchants: vec![
            format!("{}#merchant_b_outlet1_b", MERCHANT_PREFIX),
            format!("{}#merchant_b_outlet2_b", MERCHANT_PREFIX),
        ],
        has_settlement_permissions: true,
        has_billing_permissions: false,
    });

    let b_outlets = vec![
        Merchant {
            id: format!("{}#merchant_b_outlet1_s", MERCHANT_PREFIX),
            name: "Merchant B_outlet1_s".to_string(),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            vat_number: "VAT123".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            merchant_level: MerchantLevel::Outlet,
            sub_merchants: Vec::new(),
            has_settlement_permissions: false,
            has_billing_permissions: true,
        },
        Merchant {
            id: format!("{}#merchant_b_outlet2_s", MERCHANT_PREFIX),
            name: "Merchant B_outlet2_s".to_string(),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            vat_number: "VAT123".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            merchant_level: MerchantLevel::Outlet,
            sub_merchants: Vec::new(),
            has_settlement_permissions: false,
            has_billing_permissions: true,
        },
    ];

    merchants.push(Merchant {
        id: format!("{}#merchant_c_group_b", MERCHANT_PREFIX),
        name: "Merchant C_group_b".to_string(),
        founded_date: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        industry: "Retail".to_string(),
        vat_number: "VAT123".to_string(),
        created_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        merchant_level: MerchantLevel::Group,
        sub_merchants: vec![
            format!("{}#merchant_c_chain1_s", MERCHANT_PREFIX),
            format!("{}#merchant_c_chain2_s", MERCHANT_PREFIX),
        ],
        has_settlement_permissions: false,
        has_billing_permissions: true,
    });

    merchants.push(Merchant {
        id: format!("{}#merchant_c_chain1_s", MERCHANT_PREFIX),
        name: "Merchant C_chain1_s".to_string(),
        founded_date: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        industry: "Retail".to_string(),
        vat_number: "VAT123".to_string(),
        created_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        merchant_level: MerchantLevel::Chain,
        sub_merchants: vec![
            format!("{}#merchant_c_outlet1", MERCHANT_PREFIX),
            format!("{}#merchant_c_outlet2", MERCHANT_PREFIX),
        ],
        has_settlement_permissions: true,
        has_billing_permissions: false,
    });

    merchants.push(Merchant {
        id: format!("{}#merchant_c_chain2", MERCHANT_PREFIX),
        name: "Merchant C_chain2".to_string(),
        founded_date: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        industry: "Retail".to_string(),
        vat_number: "VAT123".to_string(),
        created_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        merchant_level: MerchantLevel::Chain,
        sub_merchants: vec![
            format!("{}#merchant_c_outlet3_s", MERCHANT_PREFIX),
            format!("{}#merchant_c_outlet4_s", MERCHANT_PREFIX),
        ],
        has_settlement_permissions: false,
        has_billing_permissions: false,
    });

    let c_outlets = vec![
        Merchant {
            id: format!("{}#merchant_c_outlet1", MERCHANT_PREFIX),
            name: "Merchant C_outlet1".to_string(),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            vat_number: "VAT123".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            merchant_level: MerchantLevel::Outlet,
            sub_merchants: vec![],
            has_settlement_permissions: false,
            has_billing_permissions: false,
        },
        Merchant {
            id: format!("{}#merchant_c_outlet2", MERCHANT_PREFIX),
            name: "Merchant C_outlet2".to_string(),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            vat_number: "VAT123".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            merchant_level: MerchantLevel::Outlet,
            sub_merchants: vec![],
            has_settlement_permissions: false,
            has_billing_permissions: false,
        },
    ];
    let c_outlets_settled = vec![
        Merchant {
            id: format!("{}#merchant_c_outlet3_s", MERCHANT_PREFIX),
            name: "Merchant C_outlet3_s".to_string(),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            vat_number: "VAT123".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            merchant_level: MerchantLevel::Outlet,
            sub_merchants: vec![],
            has_settlement_permissions: true,
            has_billing_permissions: false,
        },
        Merchant {
            id: format!("{}#merchant_c_outlet4_s", MERCHANT_PREFIX),
            name: "Merchant C_outlet4_s".to_string(),
            founded_date: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            industry: "Retail".to_string(),
            vat_number: "VAT123".to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            merchant_level: MerchantLevel::Outlet,
            sub_merchants: vec![],
            has_settlement_permissions: true,
            has_billing_permissions: false,
        },
    ];

    for merchant in merchants {
        add_merchant(client, &merchant, &TABLE_NAME.to_string())
            .await
            .expect("Failed to add merchant");
    }

    add_merchant(client, &a_outlet, &TABLE_NAME.to_string())
        .await
        .expect("Failed to add merchant");

    for i in 1..=5 {
        let transaction_date = Utc.with_ymd_and_hms(2025, 1, i, 0, 0, 0).unwrap();
        let settled_date = Utc
            .with_ymd_and_hms(2025, 3 % i + 1, i + 1, 0, 0, 0)
            .unwrap();
        add_transaction(
            client,
            Transaction {
                id: format!(
                    "{}#{}#{}",
                    TRANSACTION_PREFIX,
                    transaction_date.to_rfc3339(),
                    i
                ),
                merchant_id: a_outlet.id.clone(),
                transaction_type: random_transaction_type(&mut rng),
                status: random_transaction_status(&mut rng),
                amount: rng.gen_range(10.0..100.0),
                currency: "GBP".to_string(),
                pan: rng.gen_range(1000i64..9999i64).to_string(),
                card_brand: random_card_brand(&mut rng),
                date_transaction: transaction_date.to_rfc3339(),
                date_settlement: settled_date.to_rfc3339(),
                settlement_merchant_id: a_outlet.id.clone(),
                payout_id: format!("{}#{}#{}", PAYOUT_PREFIX, settled_date.to_rfc3339(), "1"),
            },
        )
        .await
        .expect("Failed to add transaction");
    }

    for merchant in b_outlets.into_iter() {
        for i in 1..=5 {
            let transaction_date = Utc.with_ymd_and_hms(2025, 3 % i + 1, i, 0, 0, 0).unwrap();
            let settled_date = Utc
                .with_ymd_and_hms(2025, 3 % i + 1, i + 1, 0, 0, 0)
                .unwrap();
            add_transaction(
                client,
                Transaction {
                    id: format!(
                        "{}#{}#{}",
                        TRANSACTION_PREFIX,
                        transaction_date.to_rfc3339(),
                        i
                    ),
                    merchant_id: merchant.id.clone(),
                    transaction_type: random_transaction_type(&mut rng),
                    status: random_transaction_status(&mut rng),
                    amount: rng.gen_range(10.0..100.0),
                    currency: "GBP".to_string(),
                    pan: rng.gen_range(1000i64..9999i64).to_string(),
                    card_brand: random_card_brand(&mut rng),
                    date_transaction: transaction_date.to_rfc3339(),
                    date_settlement: settled_date.to_rfc3339(),
                    settlement_merchant_id: "merchant_b_group_s".to_string(),
                    payout_id: format!("{}#{}#{}", PAYOUT_PREFIX, settled_date.to_rfc3339(), "1"),
                },
            )
            .await
            .expect("Failed to add transaction");
        }
    }

    for merchant in c_outlets.into_iter() {
        for i in 1..=5 {
            let transaction_date = Utc.with_ymd_and_hms(2025, 1, i, 0, 0, 0).unwrap();
            let settled_date = Utc
                .with_ymd_and_hms(2025, 3 % i + 1, i + 1, 0, 0, 0)
                .unwrap();
            add_transaction(
                client,
                Transaction {
                    id: format!(
                        "{}#{}#{}",
                        TRANSACTION_PREFIX,
                        transaction_date.to_rfc3339(),
                        i
                    ),
                    merchant_id: merchant.id.clone(),
                    transaction_type: random_transaction_type(&mut rng),
                    status: random_transaction_status(&mut rng),
                    amount: rng.gen_range(10.0..100.0),
                    currency: "GBP".to_string(),
                    pan: rng.gen_range(1000i64..9999i64).to_string(),
                    card_brand: random_card_brand(&mut rng),
                    date_transaction: transaction_date.to_rfc3339(),
                    date_settlement: settled_date.to_rfc3339(),
                    settlement_merchant_id: "merchant_c_chain1_s".to_string(),
                    payout_id: format!("{}#{}#{}", PAYOUT_PREFIX, settled_date.to_rfc3339(), "1"),
                },
            )
            .await
            .expect("Failed to add transaction");
        }
    }

    for merchant in c_outlets_settled.into_iter() {
        for i in 1..=5 {
            let transaction_date = Utc.with_ymd_and_hms(2025, 1, i, 0, 0, 0).unwrap();
            let settled_date = Utc
                .with_ymd_and_hms(2025, 3 % i + 1, i + 1, 0, 0, 0)
                .unwrap();
            add_transaction(
                client,
                Transaction {
                    id: format!(
                        "{}#{}#{}",
                        TRANSACTION_PREFIX,
                        transaction_date.to_rfc3339(),
                        i
                    ),
                    merchant_id: merchant.id.clone(),
                    transaction_type: random_transaction_type(&mut rng),
                    status: random_transaction_status(&mut rng),
                    amount: rng.gen_range(10.0..100.0),
                    currency: "GBP".to_string(),
                    pan: rng.gen_range(1000i64..9999i64).to_string(),
                    card_brand: random_card_brand(&mut rng),
                    date_transaction: transaction_date.to_rfc3339(),
                    date_settlement: settled_date.to_rfc3339(),
                    settlement_merchant_id: merchant.id.clone(),
                    payout_id: format!("{}#{}#{}", PAYOUT_PREFIX, settled_date.to_rfc3339(), "1"),
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
    let vat_number_av = AttributeValue::S(merchant.vat_number.clone());
    let created_at_av = AttributeValue::N(merchant.created_at.to_string());
    let merchant_level_av = AttributeValue::S(merchant.merchant_level.to_string());
    let sub_merchants_av = AttributeValue::L(
        merchant
            .sub_merchants
            .iter()
            .map(|sub_merchant| AttributeValue::S(sub_merchant.clone()))
            .collect(),
    );
    let has_settlement_permissions_av = AttributeValue::Bool(merchant.has_settlement_permissions);
    let has_billing_permissions_av = AttributeValue::Bool(merchant.has_billing_permissions);
    let request = client
        .put_item()
        .table_name(table)
        .item(PARTITION_KEY, id_av.clone())
        .item(SORT_KEY, id_av)
        .item("name", name_av)
        .item("founded_date", founded_date_av)
        .item("industry", industry_av)
        .item("vat_number", vat_number_av)
        .item("created_at", created_at_av)
        .item("merchant_level", merchant_level_av)
        .item("sub_merchants", sub_merchants_av)
        .item("has_settlement_permissions", has_settlement_permissions_av)
        .item("has_billing_permissions", has_billing_permissions_av);
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
        .global_secondary_indexes(
            aws_sdk_dynamodb::types::GlobalSecondaryIndex::builder()
                .index_name("gsi1")
                .key_schema(
                    aws_sdk_dynamodb::types::KeySchemaElement::builder()
                        .attribute_name(GSI1_PARTITION_KEY)
                        .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                        .build()
                        .expect("Failed to build GSI1 partition key KeySchemaElement"),
                )
                .key_schema(
                    aws_sdk_dynamodb::types::KeySchemaElement::builder()
                        .attribute_name(GSI1_SORT_KEY)
                        .key_type(aws_sdk_dynamodb::types::KeyType::Range)
                        .build()
                        .expect("Failed to build GSI1 sort key KeySchemaElement"),
                )
                .projection(
                    aws_sdk_dynamodb::types::Projection::builder()
                        .projection_type(aws_sdk_dynamodb::types::ProjectionType::All)
                        .build(),
                )
                .build()
                .expect("Failed to build GSI1 GlobalSecondaryIndex"),
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
                .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(GSI1_PARTITION_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("Failed to build GSI1 partition key AttributeDefinition"),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(GSI1_SORT_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("Failed to build GSI1 sort key AttributeDefinition"),
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
        0 => TransactionType::Purchase,
        _ => TransactionType::Refund,
    }
}

fn random_transaction_status(rng: &mut impl Rng) -> TransactionStatus {
    match rng.gen_range(0..4) {
        0 => TransactionStatus::Processed,
        1 => TransactionStatus::Cleared,
        2 => TransactionStatus::Chargebacked,
        _ => TransactionStatus::Paid,
    }
}

fn random_card_brand(rng: &mut impl Rng) -> CardBrand {
    match rng.gen_range(0..4) {
        0 => CardBrand::Visa,
        _ => CardBrand::Mastercard,
    }
}

async fn add_transaction(
    client: &aws_sdk_dynamodb::Client,
    transaction: Transaction,
) -> Result<(), Error> {
    let merchant_id_av = AttributeValue::S(transaction.merchant_id.to_string());
    let id_av = AttributeValue::S(transaction.id);
    let settlement_merchant_id_av =
        AttributeValue::S(transaction.settlement_merchant_id.to_string());
    let gsi1_partition_key_av = settlement_merchant_id_av.clone();
    let gsi1_sort_key_av = id_av.clone();
    let transaction_type_av = AttributeValue::S(transaction.transaction_type.to_string());
    let status_av = AttributeValue::S(transaction.status.to_string());
    let amount_av = AttributeValue::N(transaction.amount.to_string());
    let currency_av = AttributeValue::S(transaction.currency.to_string());
    let pan_av = AttributeValue::S(transaction.pan.to_string());
    let card_brand_av = AttributeValue::S(transaction.card_brand.to_string());
    let date_transaction_av = AttributeValue::S(transaction.date_transaction.to_string());
    let date_settlement_av = AttributeValue::S(transaction.date_settlement.to_string());
    let payout_id_av = AttributeValue::S(transaction.payout_id.to_string());
    let request = client
        .put_item()
        .table_name(TABLE_NAME)
        .item(PARTITION_KEY, merchant_id_av)
        .item(SORT_KEY, id_av)
        .item(GSI1_PARTITION_KEY, gsi1_partition_key_av)
        .item(GSI1_SORT_KEY, gsi1_sort_key_av)
        .item("transaction_type", transaction_type_av)
        .item("status", status_av)
        .item("amount", amount_av)
        .item("currency", currency_av)
        .item("pan", pan_av)
        .item("card_brand", card_brand_av)
        .item("date_transaction", date_transaction_av)
        .item("date_settlement", date_settlement_av)
        .item("settlement_merchant_id", settlement_merchant_id_av)
        .item("payout_id", payout_id_av);
    println!("Executing request [{request:?}] to add item...");

    request.send().await?;
    Ok(())
}

pub async fn get_transactions(
    client: &aws_sdk_dynamodb::Client,
    merchant_id: String,
    year: Option<String>,
    month: Option<String>,
    day: Option<String>,
    card_brand: Option<CardBrand>,
    after_pagination: Option<String>,
    before_pagination: Option<String>,
    limit: i32,
) -> Result<(Vec<Transaction>, bool), anyhow::Error> {
    println!(
        "Getting transactions for merchant_id={merchant_id}, after={after_pagination:?}, before={before_pagination:?}, limit={limit}..."
    );

    let (mut earlier_transaction, mut later_transaction) = match (year, month, day) {
        // daily aggregate
        (Some(year), Some(month), Some(day)) => (
            format!("{}#{}-{}-{}", TRANSACTION_PREFIX, year, month, day),
            format!("{}#{}-{}-{}T99", TRANSACTION_PREFIX, year, month, day),
        ),
        (_, _, Some(day)) => (
            format!(
                "{}#{}-{}-{}",
                TRANSACTION_PREFIX,
                Utc::now().year().to_string(),
                Utc::now().month().to_string(),
                day
            ),
            format!(
                "{}#{}-{}-{}T99",
                TRANSACTION_PREFIX,
                Utc::now().year().to_string(),
                Utc::now().month().to_string(),
                day
            ),
        ),
        // monthly aggregate
        (Some(year), Some(month), _) => (
            format!("{}#{}-{}", TRANSACTION_PREFIX, year, month),
            format!("{}#{}-{}-{}", TRANSACTION_PREFIX, year, month, "99"),
        ),
        (_, Some(month), _) => (
            format!(
                "{}#{}-{}",
                TRANSACTION_PREFIX,
                Utc::now().year().to_string(),
                month
            ),
            format!(
                "{}#{}-{}-99",
                TRANSACTION_PREFIX,
                Utc::now().year().to_string(),
                month
            ),
        ),
        // yearly aggregate
        (Some(year), None, None) => (
            format!("{}#{}", TRANSACTION_PREFIX, year),
            format!("{}#{}-99-99", TRANSACTION_PREFIX, year),
        ),
        (None, None, None) => (
            format!("{}#", TRANSACTION_PREFIX),
            format!("{}#{}-", TRANSACTION_PREFIX, Utc::now().to_rfc3339()),
        ),
    };

    if after_pagination.is_some() && after_pagination.as_ref().unwrap() <= &later_transaction {
        later_transaction = after_pagination.unwrap();
    }
    if before_pagination.is_some() && before_pagination.as_ref().unwrap() >= &earlier_transaction {
        earlier_transaction = before_pagination.unwrap();
    }

    println!(
        "Querying transactions with partition key '{merchant_id}' and sort key between '{earlier_transaction}' and '{later_transaction}'..."
    );
    let query = client
        .query()
        .table_name(TABLE_NAME)
        .limit(limit)
        .key_condition_expression(
            "#partition_key = :merchant_id AND #sort_key BETWEEN :earlier_transaction AND :later_transaction",
        )
        .expression_attribute_names("#partition_key", PARTITION_KEY)
        .expression_attribute_names("#sort_key", SORT_KEY)
        .expression_attribute_values(":merchant_id", AttributeValue::S(merchant_id))
        .expression_attribute_values(":earlier_transaction", AttributeValue::S(earlier_transaction))
        .expression_attribute_values(":later_transaction", AttributeValue::S(later_transaction))
        .scan_index_forward(false);
    let query = if let Some(card_brand) = card_brand {
        query
            .filter_expression("#card_brand = :card_brand")
            .expression_attribute_names("#card_brand", "card_brand")
            .expression_attribute_values(":card_brand", AttributeValue::S(card_brand.to_string()))
    } else {
        query
    };

    let items_resp = query.send().await.context("Failed to get transaction")?;

    if let Some(items) = items_resp.items {
        let mut modified_items = items.clone();
        replace_key_names(&mut modified_items, "merchant_id", "id");
        let transactions: Vec<Transaction> = from_items(modified_items)?;
        return Ok((transactions, items_resp.last_evaluated_key.is_some()));
    }

    Ok((Vec::new(), false))
}

pub async fn get_transactions_for_settlement_merchant(
    client: &aws_sdk_dynamodb::Client,
    settlement_merchant_id: String,
    after: Option<String>,
    before: Option<String>,
    limit: i32,
) -> Result<(Vec<Transaction>, bool), anyhow::Error> {
    println!(
        "Getting transactions for settlement_merchant_id={settlement_merchant_id}, after={after:?}, before={before:?}, limit={limit}..."
    );
    
     let (mut earlier_transaction, mut later_transaction) = (
            format!("{}#", TRANSACTION_PREFIX),
            format!("{}#9999", TRANSACTION_PREFIX),
        );

    if after.is_some() && after.as_ref().unwrap() <= &later_transaction {
        later_transaction = after.unwrap();
    }
    if before.is_some() && before.as_ref().unwrap() >= &earlier_transaction {
        earlier_transaction = before.unwrap();
    }

    println!(
        "Querying transactions with partition key '{settlement_merchant_id}' and sort key between '{earlier_transaction}' and '{later_transaction}'..."
    );
    let query = client
        .query()
        .table_name(TABLE_NAME)
        .index_name("gsi1")
        .limit(limit)
        .key_condition_expression(
            "#gsi1_partition_key = :settlement_merchant_id AND #gsi1_sort_key BETWEEN :earlier_transaction AND :later_transaction",
        )
        .expression_attribute_names("#gsi1_partition_key", GSI1_PARTITION_KEY)
        .expression_attribute_names("#gsi1_sort_key", GSI1_SORT_KEY)
        .expression_attribute_values(
            ":settlement_merchant_id",
            AttributeValue::S(settlement_merchant_id),
        )
        .expression_attribute_values(":earlier_transaction", AttributeValue::S(earlier_transaction))
        .expression_attribute_values(":later_transaction", AttributeValue::S(later_transaction))
        .scan_index_forward(false);

    let items_resp = query.send().await.map_err(|err| anyhow::anyhow!("Failed to get transactions: {}", err))?;

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
        if let Some(value) = item.remove(PARTITION_KEY) {
            item.insert(partition_key.to_string(), value);
        }
        if let Some(value) = item.remove(SORT_KEY) {
            item.insert(sort_key.to_string(), value);
        }
    }
}
