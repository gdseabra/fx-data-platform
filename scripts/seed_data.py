# FX Data Platform - Python Script for Seeding Data
# Populates PostgreSQL with realistic test transaction data

import sys
from datetime import datetime, timedelta
import random
import logging

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_db():
    """Connect to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="postgres",
            database="fx_transactions"
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)


def seed_transactions(conn, num_records=1000):
    """Insert sample transactions."""
    logger.info(f"Seeding {num_records} transactions...")
    
    cursor = conn.cursor()
    
    currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD"]
    channels = ["mobile", "web", "api"]
    tx_types = ["BUY", "SELL"]
    
    # Get user IDs
    cursor.execute("SELECT user_id FROM users LIMIT 10")
    user_ids = [row[0] for row in cursor.fetchall()]
    
    transactions = []
    base_date = datetime.utcnow() - timedelta(days=30)
    
    for i in range(num_records):
        tx_date = base_date + timedelta(
            seconds=random.randint(0, 30*24*3600)
        )
        
        amount = random.choice([500, 1000, 2500, 5000, 10000])
        rate = random.uniform(4.5, 5.5)
        
        transaction = (
            user_ids[i % len(user_ids)],
            tx_date,
            random.choice(tx_types),
            random.choice(currencies),
            amount,
            amount / rate,
            rate,
            random.uniform(0.1, 0.5),
            0,  # fee
            "completed",
            random.choice(channels),
            f"device_{random.randint(1000, 9999)}",
            random.choice([True, False]) if random.random() < 0.05 else False,
            random.uniform(0, 0.3) if random.random() < 0.05 else None,
        )
        transactions.append(transaction)
    
    sql = """
    INSERT INTO transactions 
    (user_id, timestamp, transaction_type, currency, amount_brl, amount_foreign,
     exchange_rate, spread_pct, fee_brl, status, channel, device, is_anomaly, anomaly_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    execute_batch(cursor, sql, transactions, page_size=100)
    conn.commit()
    cursor.close()
    
    logger.info(f"✓ Inserted {num_records} transactions")


def seed_exchange_rates(conn, num_records=100):
    """Insert sample exchange rates."""
    logger.info(f"Seeding {num_records} exchange rates...")
    
    cursor = conn.cursor()
    
    currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD"]
    base_date = datetime.utcnow() - timedelta(days=7)
    
    rates = []
    for curr in currencies:
        for i in range(num_records // len(currencies)):
            rate_date = base_date + timedelta(hours=i)
            rate = random.uniform(4.5, 5.5)
            
            rates.append((
                f"{curr}BRL",
                rate,
                rate * 0.99,
                rate * 1.01,
                rate_date,
                "simulation"
            ))
    
    sql = """
    INSERT INTO exchange_rates (currency_pair, rate, bid, ask, timestamp, source)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    execute_batch(cursor, sql, rates, page_size=100)
    conn.commit()
    cursor.close()
    
    logger.info(f"✓ Inserted {len(rates)} exchange rates")


def main():
    """Seed database with test data."""
    logger.info("Starting database seeding...")
    
    conn = connect_db()
    
    try:
        seed_transactions(conn, num_records=1000)
        seed_exchange_rates(conn, num_records=100)
        
        logger.info("✓ Database seeding completed successfully")
        
    except Exception as e:
        logger.error(f"Error during seeding: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
