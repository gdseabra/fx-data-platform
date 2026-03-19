-- FX Data Platform - PostgreSQL Initialization Script
-- This script creates the initial schema for the FX transactions database
-- Tables: users, transactions, exchange_rates

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    cpf VARCHAR(14) UNIQUE,
    email VARCHAR(255) UNIQUE NOT NULL,
    state VARCHAR(2),
    tier VARCHAR(20) DEFAULT 'standard' CHECK (tier IN ('standard', 'gold', 'platinum')),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    avg_monthly_volume DECIMAL(15, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL')),
    currency VARCHAR(3) NOT NULL,
    amount_brl DECIMAL(15, 2) NOT NULL,
    amount_foreign DECIMAL(15, 4) NOT NULL,
    exchange_rate DECIMAL(10, 4) NOT NULL,
    spread_pct DECIMAL(5, 3) NOT NULL,
    fee_brl DECIMAL(10, 2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('pending', 'completed', 'failed', 'cancelled')),
    channel VARCHAR(20) DEFAULT 'mobile' CHECK (channel IN ('mobile', 'web', 'api')),
    device VARCHAR(50),
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_score DECIMAL(5, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create exchange_rates table
CREATE TABLE IF NOT EXISTS exchange_rates (
    rate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    currency_pair VARCHAR(6) NOT NULL,
    rate DECIMAL(10, 4) NOT NULL,
    bid DECIMAL(10, 4),
    ask DECIMAL(10, 4),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX idx_transactions_currency ON transactions(currency);
CREATE INDEX idx_transactions_is_anomaly ON transactions(is_anomaly);
CREATE INDEX idx_exchange_rates_currency_pair ON exchange_rates(currency_pair);
CREATE INDEX idx_exchange_rates_timestamp ON exchange_rates(timestamp);
CREATE INDEX idx_users_email ON users(email);

-- Heartbeat table required by Debezium CDC connector
-- Updated every 10 seconds to detect WAL inactivity
CREATE TABLE IF NOT EXISTS heartbeat (
    id      INTEGER PRIMARY KEY DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO heartbeat (id, updated_at) VALUES (1, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE users IS 'FX platform users with registration and tier information';
COMMENT ON TABLE transactions IS 'FX transactions with real-time anomaly detection scores';
COMMENT ON TABLE exchange_rates IS 'Historical exchange rate data for multiple currency pairs';

COMMENT ON COLUMN users.tier IS 'User tier: standard, gold, or platinum with different limits';
COMMENT ON COLUMN transactions.is_anomaly IS 'Flag set by ML model after anomaly detection';
COMMENT ON COLUMN transactions.anomaly_score IS 'ML model score (0-1) indicating anomaly probability';
COMMENT ON COLUMN exchange_rates.bid IS 'Bid price for the currency pair';
COMMENT ON COLUMN exchange_rates.ask IS 'Ask price for the currency pair';
