use crate::models::{CardBrand, Merchant, Transaction, TransactionStatus, TransactionType};
use aws_sdk_dynamodb::{
    Error,
    types::{AttributeValue, ScalarAttributeType},
};
use rand::Rng;
use std::time::SystemTime;

pub async fn init_merchants(client: &aws_sdk_dynamodb::Client) {
    create_table(
        client,
        &"merchants".to_string(),
        &"id".to_string(),
        ScalarAttributeType::S,
        Some(&"created_at".to_string()),
        Some(ScalarAttributeType::N),
    )
    .await;

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

async fn create_table(
    client: &aws_sdk_dynamodb::Client,
    table_name: &String,
    partition_key_name: &String,
    partition_key_type: ScalarAttributeType,
    sort_key_name: Option<&String>,
    sort_key_type: Option<ScalarAttributeType>,
) {
    println!("Creating table '{table_name}'...");

    let create_resp = client
        .create_table()
        .table_name(table_name)
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name(partition_key_name)
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .expect("Failed to build partition key KeySchemaElement"),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name(sort_key_name.unwrap_or(&"".to_string()))
                .key_type(aws_sdk_dynamodb::types::KeyType::Range)
                .build()
                .expect("Failed to build sort key KeySchemaElement"),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(partition_key_name)
                .attribute_type(partition_key_type)
                .build()
                .expect("Failed to build partition key AttributeDefinition"),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(sort_key_name.unwrap_or(&"".to_string()))
                .attribute_type(sort_key_type.unwrap_or(ScalarAttributeType::S))
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

pub async fn init_transactions(client: &aws_sdk_dynamodb::Client) {
    create_table(
        client,
        &"transactions".to_string(),
        &"merchant_id".to_string(),
        ScalarAttributeType::S,
        Some(&"created_at".to_string()),
        Some(ScalarAttributeType::N),
    )
    .await;

    let mut rng = rand::thread_rng();

    for i in 1..=5 {
        let _add_resp = add_transaction(
            client,
            Transaction {
                merchant_id: "merchant-123".to_string(),
                id: format!("transaction-{}", i),
                transaction_type: random_transaction_type(&mut rng),
                status: random_transaction_status(&mut rng),
                amount: rng.gen_range(10.0..100.0),
                fees: rng.gen_range(0.5..5.0),
                pan: rng.gen_range(4000000000000000i64..5000000000000000i64),
                card_brand: random_card_brand(&mut rng),
                created_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            },
        )
        .await
        .expect("Failed to add transaction");
    }

    for i in 1..=5 {
        let _add_resp = add_transaction(
            client,
            Transaction {
                merchant_id: "merchant-456".to_string(),
                id: format!("transaction-{}", i),
                transaction_type: random_transaction_type(&mut rng),
                status: random_transaction_status(&mut rng),
                amount: rng.gen_range(10.0..100.0),
                fees: rng.gen_range(0.5..5.0),
                pan: rng.gen_range(4000000000000000i64..5000000000000000i64),
                card_brand: random_card_brand(&mut rng),
                created_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            },
        )
        .await
        .expect("Failed to add transaction");
    }

    for i in 1..=5 {
        let _add_resp = add_transaction(
            client,
            Transaction {
                merchant_id: "merchant-789".to_string(),
                id: format!("transaction-{}", i),
                transaction_type: random_transaction_type(&mut rng),
                status: random_transaction_status(&mut rng),
                amount: rng.gen_range(10.0..100.0),
                fees: rng.gen_range(0.5..5.0),
                pan: rng.gen_range(4000000000000000i64..5000000000000000i64),
                card_brand: random_card_brand(&mut rng),
                created_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            },
        )
        .await
        .expect("Failed to add transaction");
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
        .table_name("transactions")
        .item("merchant_id", merchant_id_av)
        .item("id", id_av)
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
