use crate::models::{CardBrand, Merchant, Transaction, TransactionStatus, TransactionType};
use chrono::{TimeZone, Utc};
use rand::Rng;

pub async fn init_db(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    let mut merchants: Vec<Merchant> = Vec::new();
    let mut rng = rand::thread_rng();

    for i in 1..=5 {
        merchants.push(Merchant {
            id: format!("merchant-{}", i),
            name: format!("Merchant {}", i),
            description: "description".to_string(),
            founded_date: chrono::Utc::now().to_rfc2822(),
            industry: "industry".to_string(),
            num_employees: i,
            vat_number: format!("VAT-{}", i),
            created_at: chrono::Utc::now().timestamp_millis(),
        });
    }

    for merchant in merchants {
        sqlx::query!(
            r#"
            INSERT INTO merchants (
                id, name, description, founded_date, industry, num_employees, vat_number, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING;
            "#,
            merchant.id,
            merchant.name,
            merchant.description,
            merchant.founded_date,
            merchant.industry,
            merchant.num_employees,
            merchant.vat_number,
            merchant.created_at
        )
        .execute(pool)
        .await
        .expect("Failed to insert merchant");

        for i in 0..10 {
            let date = Utc.with_ymd_and_hms(2026, i % 3 + 1, 1, 0, 0, 0).unwrap();
            sqlx::query!(
                r#"
                INSERT INTO transactions (
                    merchant_id, id, transaction_type, status, amount, fees, pan, card_brand, created_at, description
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO NOTHING;
                "#,
                merchant.id,
                format!("txn-{}-{}", merchant.id, i),
                random_transaction_type(&mut rng) as TransactionType,
                random_transaction_status(&mut rng) as TransactionStatus,
                rng.gen_range(10.0..100.0),
                rng.gen_range(0.5..5.0),
                rng.gen_range(4000000000i64..5000000000i64).to_string(),
                random_card_brand(&mut rng) as CardBrand,
                date.timestamp_millis(),
                "description".to_string()
            )
            .execute(pool)
            .await
            .expect("Failed to insert transaction");
        }
    }
    Ok(())
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

pub async fn get_merchant_by_id(pool: &sqlx::PgPool, id: String) -> Result<Merchant, sqlx::Error> {
    sqlx::query_as!(
        Merchant,
        r#"SELECT id, name, description, founded_date, industry, num_employees, vat_number, created_at FROM merchants WHERE id = $1"#,
        id
    )
    .fetch_one(pool)
    .await
}

pub async fn get_transactions_for_merchant(
    pool: &sqlx::PgPool,
    merchant_id: String,
    after: i64,
    after_id: String,
    before: i64,
    before_id: String,
    limit: i64,
) -> Result<(Vec<Transaction>, bool), sqlx::Error> {
    let mut transactions = sqlx::query_as!(
            Transaction,
            r#"SELECT merchant_id, id, transaction_type as "transaction_type: TransactionType", status as "status: TransactionStatus", amount, fees, pan, card_brand as "card_brand: CardBrand", created_at 
             FROM transactions 
             WHERE merchant_id = $1 AND (created_at < $2 OR (created_at = $2 AND id > $3)) AND (created_at > $4 OR (created_at = $4 AND id < $5))
             ORDER BY created_at DESC, id ASC
             LIMIT $6"#,
            merchant_id,
            after,
            after_id,
            before,
            before_id,
            limit + 1
        )
        .fetch_all(pool)
        .await?;

    let has_more = transactions.len() as i64 > limit;
    if limit < transactions.len() as i64 {
        transactions.pop();
    }
    Ok((transactions, has_more))
}
