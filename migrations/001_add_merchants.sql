CREATE TABLE IF NOT EXISTS merchants (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    founded_date VARCHAR(255) NOT NULL,
    industry VARCHAR(255) NOT NULL,
    num_employees INTEGER NOT NULL,
    vat_number VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TYPE transaction_type_enum AS ENUM ('online', 'pos');
CREATE TYPE transaction_status_enum AS ENUM ('pending', 'successful', 'chargeback', 'paid_out');
CREATE TYPE card_brand_enum AS ENUM ('visa', 'mastercard', 'amex', 'discover');

CREATE TABLE IF NOT EXISTS transactions (
    id VARCHAR(255) PRIMARY KEY,
    merchant_id VARCHAR(255) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    fees DOUBLE PRECISION NOT NULL,
    transaction_type transaction_type_enum NOT NULL,
    status transaction_status_enum NOT NULL,
    card_brand card_brand_enum NOT NULL,
    pan VARCHAR(10) NOT NULL,
    created_at BIGINT NOT NULL,
    description TEXT NOT NULL,
    FOREIGN KEY (merchant_id) REFERENCES merchants(id)
);