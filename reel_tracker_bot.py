#!/usr/bin/env python3
"""
Database Migration Script
Fixes missing columns in the users table
"""

import asyncio
import os
from sqlalchemy import create_async_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("❌ DATABASE_URL not found in .env file")
    exit(1)

async def fix_database():
    """Fix missing columns in the database"""
    print("🔧 Starting database migration...")
    
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    try:
        async with engine.begin() as conn:
            print("📋 Checking and adding missing columns to users table...")
            
            # Add missing columns to users table
            columns_to_add = [
                ("approved", "BOOLEAN DEFAULT FALSE"),
                ("total_views", "BIGINT DEFAULT 0"),
                ("total_reels", "INTEGER DEFAULT 0"),
                ("max_slots", "INTEGER DEFAULT 50"),
                ("used_slots", "INTEGER DEFAULT 0"),
                ("last_submission", "TIMESTAMP"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ("registered", "INTEGER DEFAULT 0")
            ]
            
            for column_name, column_type in columns_to_add:
                try:
                    await conn.execute(text(f"""
                        ALTER TABLE users ADD COLUMN IF NOT EXISTS {column_name} {column_type}
                    """))
                    print(f"✅ Added column: {column_name}")
                except Exception as e:
                    print(f"⚠️ Column {column_name} already exists or error: {e}")
            
            # Create other required tables
            print("📋 Creating required tables...")
            
            tables_to_create = [
                ("banned_users", """
                    CREATE TABLE IF NOT EXISTS banned_users (
                        user_id BIGINT PRIMARY KEY
                    )
                """),
                ("allowed_accounts", """
                    CREATE TABLE IF NOT EXISTS allowed_accounts (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        insta_handle VARCHAR NOT NULL
                    )
                """),
                ("payment_details", """
                    CREATE TABLE IF NOT EXISTS payment_details (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        usdt_address VARCHAR,
                        paypal_email VARCHAR,
                        upi_address VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """),
                ("account_requests", """
                    CREATE TABLE IF NOT EXISTS account_requests (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        insta_handle VARCHAR NOT NULL,
                        status VARCHAR DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """),
                ("admins", """
                    CREATE TABLE IF NOT EXISTS admins (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        added_by BIGINT NOT NULL,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """),
                ("referrals", """
                    CREATE TABLE IF NOT EXISTS referrals (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        referrer_id BIGINT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """),
                ("config", """
                    CREATE TABLE IF NOT EXISTS config (
                        id SERIAL PRIMARY KEY,
                        key VARCHAR NOT NULL UNIQUE,
                        value VARCHAR NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """),
                ("submission_logs", """
                    CREATE TABLE IF NOT EXISTS submission_logs (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        shortcode VARCHAR NOT NULL,
                        views BIGINT NOT NULL,
                        old_views BIGINT,
                        insta_handle VARCHAR NOT NULL,
                        action VARCHAR NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """),
                ("force_update_logs", """
                    CREATE TABLE IF NOT EXISTS force_update_logs (
                        id SERIAL PRIMARY KEY,
                        total_reels INTEGER NOT NULL,
                        successful_updates INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            ]
            
            for table_name, create_sql in tables_to_create:
                try:
                    await conn.execute(text(create_sql))
                    print(f"✅ Created table: {table_name}")
                except Exception as e:
                    print(f"⚠️ Table {table_name} already exists or error: {e}")
            
            # Add default commission rate
            try:
                await conn.execute(text("""
                    INSERT INTO config (key, value) 
                    VALUES ('referral_commission_rate', '0.00')
                    ON CONFLICT (key) DO NOTHING
                """))
                print("✅ Added default commission rate")
            except Exception as e:
                print(f"⚠️ Commission rate already exists or error: {e}")
            
            # Add created_at to reels table if it doesn't exist
            try:
                await conn.execute(text("""
                    ALTER TABLE reels ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """))
                print("✅ Added created_at to reels table")
            except Exception as e:
                print(f"⚠️ created_at column already exists or error: {e}")
        
        print("✅ Database migration completed successfully!")
        print("🚀 You can now run the bot with: python reel_tracker_bot.py")
        
    except Exception as e:
        print(f"❌ Database migration failed: {e}")
        raise
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(fix_database()) 
