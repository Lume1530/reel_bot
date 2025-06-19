import os
import re
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from PIL import Image, ImageDraw, ImageFont
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.constants import ParseMode
from sqlalchemy import create_engine, Column, Integer, String, BigInteger, DateTime, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI
import uvicorn
from dotenv import load_dotenv

# Import the new Apify client
from apify_client import get_apify_client

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, BigInteger, text

# ─── Load config ─────────────────────────────────────────────────────────────
load_dotenv()
TOKEN        = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "")
ADMIN_IDS    = set(map(int, ADMIN_IDS_STR.split(","))) if ADMIN_IDS_STR.strip() else set()
PORT         = int(os.getenv("PORT", 8000))
LOG_GROUP_ID_STR = os.getenv("LOG_GROUP_ID", "0")
LOG_GROUP_ID = int(LOG_GROUP_ID_STR) if LOG_GROUP_ID_STR.isdigit() else None
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

if not all([TOKEN, DATABASE_URL, APIFY_TOKEN]):
    print("❌ BOT_TOKEN, DATABASE_URL, and APIFY_TOKEN must be set in .env")
    exit(1)

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── FastAPI health check ──────────────────────────────────────────────────────
app_fastapi = FastAPI()

@app_fastapi.get("/")
async def root():
    return {"message": "Bot is running 🚀"}

async def start_health_check_server():
    config = uvicorn.Config(app_fastapi, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

# ─── Database setup ───────────────────────────────────────────────────────────
Base = declarative_base()
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY
            )
        """))

        # Add missing columns to users table if they don't exist
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS total_views BIGINT DEFAULT 0"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS approved BOOLEAN DEFAULT FALSE"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS total_reels INTEGER DEFAULT 0"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS max_slots INTEGER DEFAULT 50"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS used_slots INTEGER DEFAULT 0"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_submission TIMESTAMP"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ))
        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS registered INTEGER DEFAULT 0"
        ))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS allowed_accounts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                insta_handle VARCHAR NOT NULL
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS payment_details (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                usdt_address VARCHAR,
                paypal_email VARCHAR,
                upi_address VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS account_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                insta_handle VARCHAR NOT NULL,
                status VARCHAR DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS admins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL UNIQUE,
                added_by BIGINT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS slot_accounts (
                id SERIAL PRIMARY KEY,
                slot_number INTEGER NOT NULL,
                insta_handle VARCHAR NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS slot_submissions (
                id SERIAL PRIMARY KEY,
                slot_number INTEGER NOT NULL,
                shortcode VARCHAR NOT NULL,
                insta_handle VARCHAR NOT NULL,
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                view_count BIGINT DEFAULT 0
            )
        """))
        # Add created_at column to reels table if it doesn't exist
        await conn.execute(text("""
            ALTER TABLE reels ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """))
        
        # Add referral table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL UNIQUE,
                referrer_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (referrer_id) REFERENCES users(user_id)
            )
        """))
        
        # Create config table first
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS config (
                id SERIAL PRIMARY KEY,
                key VARCHAR NOT NULL UNIQUE,
                value VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Add global commission rate to config table
        await conn.execute(text("""
            INSERT INTO config (key, value) 
            VALUES ('referral_commission_rate', '0.00')
            ON CONFLICT (key) DO NOTHING
        """))
        
        # Add logging tables
        await conn.execute(text("""
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
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS force_update_logs (
                id SERIAL PRIMARY KEY,
                total_reels INTEGER NOT NULL,
                successful_updates INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Add initial admins from .env
        for admin_id in ADMIN_IDS:
            await conn.execute(text("""
                INSERT INTO admins (user_id, added_by)
                VALUES (:u, :u)
                ON CONFLICT (user_id) DO NOTHING
            """), {"u": admin_id})

# ─── ORM models ───────────────────────────────────────────────────────────────
class Reel(Base):
    __tablename__ = "reels"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)
    shortcode = Column(String, nullable=False, unique=True)
    url = Column(String, nullable=True)
    username = Column(String, nullable=True)
    views = Column(BigInteger, default=0)
    likes = Column(BigInteger, default=0)
    comments = Column(BigInteger, default=0)
    caption = Column(String, nullable=True)
    media_url = Column(String, nullable=True)
    submitted_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now)

class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True)
    username = Column(String, nullable=True)
    registered = Column(Integer, default=0)
    approved = Column(Boolean, default=False)
    total_views = Column(BigInteger, default=0)
    total_reels = Column(Integer, default=0)
    max_slots = Column(Integer, default=50)
    used_slots = Column(Integer, default=0)
    last_submission = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)

class SubmissionLog(Base):
    __tablename__ = "submission_logs"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)
    username = Column(String, nullable=True)
    action = Column(String, nullable=False)
    details = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.now)

# ─── Utilities ─────────────────────────────────────────────────────────────────
def validate_instagram_link(url: str) -> bool:
    """Validate if URL is a valid Instagram reel/post URL"""
    import re
    
    # Clean up the URL - remove @ symbol and query parameters
    url = url.strip().lstrip('@')
    url = url.split('?')[0]  # Remove query parameters
    
    patterns = [
        r'instagram\.com/(?:[^/]+/)?(?:p|reel|tv)/([A-Za-z0-9_-]+)',
        r'instagram\.com/reel/([A-Za-z0-9_-]+)',
        r'instagram\.com/p/([A-Za-z0-9_-]+)',
        r'instagram\.com/tv/([A-Za-z0-9_-]+)'
    ]
    
    for pattern in patterns:
        if re.search(pattern, url):
            return True
    return False

def extract_shortcode_from_url(url: str) -> Optional[str]:
    """Extract shortcode from Instagram URL"""
    import re
    
    # Clean up the URL - remove @ symbol and query parameters
    url = url.strip().lstrip('@')
    url = url.split('?')[0]  # Remove query parameters
    
    patterns = [
        r'instagram\.com/(?:[^/]+/)?(?:p|reel|tv)/([A-Za-z0-9_-]+)',
        r'/(?:p|reel|tv)/([A-Za-z0-9_-]+)',
        r'^([A-Za-z0-9_-]+)$'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

# Create a synchronous session for the updated functions
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Create synchronous engine and session
sync_engine = create_engine(DATABASE_URL.replace('+asyncpg', ''))
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)
session = SessionLocal()

# Update ADMIN_USER_IDS to use the correct variable name
ADMIN_USER_IDS = ADMIN_IDS

async def is_admin(user_id: int) -> bool:
    async with AsyncSessionLocal() as s:
        result = await s.execute(
            text("SELECT 1 FROM admins WHERE user_id = :u"),
            {"u": user_id}
        )
        return bool(result.scalar())

def debug_handler(fn):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if LOG_GROUP_ID and update.message:
            user = update.effective_user
            name = user.full_name
            handle = f"@{user.username}" if user.username else ""
            text = update.message.text or ""
            try:
                await context.bot.send_message(LOG_GROUP_ID, f"{name} {handle}: {text}")
            except Exception as e:
                logger.error(f"Failed to send log message: {e}")
        try:
            return await fn(update, context)
        except Exception as e:
            logger.exception("Handler error")
            if update.message:
                await update.message.reply_text(f"⚠️ Error: {e}")
            raise
    return wrapper

async def get_reel_data(shortcode: str) -> dict:
    """Get reel data using Apify API"""
    try:
        # Get the Apify client instance
        apify_client = get_apify_client()
        
        # Use the legacy method that returns data in the expected format
        reel_data = await apify_client.get_reel_data(shortcode)
        
        logger.info(f"✅ Successfully scraped reel data for: {reel_data.get('owner_username', 'unknown')}")
        logger.info(f"View count: {reel_data.get('view_count', 0)}")
        
        return reel_data
        
    except Exception as e:
        logger.error(f"❌ Error in get_reel_data: {str(e)}")
        raise Exception(f"Error fetching reel data: {str(e)}")

async def update_all_reel_views():
    """Aggregate and update total views for each user based on all their reels"""
    logger.info("Starting aggregated view count update...")
    async with AsyncSessionLocal() as s:
        # Fetch distinct users with reels
        users = (await s.execute(text("SELECT DISTINCT user_id FROM reels"))).fetchall()
        total_updated = 0

        for (user_id,) in users:
            total_views = 0
            # Get all reels for this user
            reels = (await s.execute(
                text("SELECT shortcode FROM reels WHERE user_id = :u"),
                {"u": user_id}
            )).fetchall()

            for (shortcode,) in reels:
                try:
                    reel_data = await get_reel_data(shortcode)
                    total_views += reel_data['view_count']
                except Exception as e:
                    logger.warning(f"Skipping reel {shortcode}: {e}")
                    continue

            # Update user's total_views once
            await s.execute(
                text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                {"v": total_views, "u": user_id}
            )
            total_updated += 1
            logger.info(f"Updated user {user_id} to {total_views} views")

        await s.commit()
        logger.info(f"✅ Completed view updates for {total_updated} users")

# Global dict for submit cooldowns
submit_cooldowns = {}

# Add at the top with other global variables
force_update_lock = asyncio.Lock()

@debug_handler
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command - register user and show help"""
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    
    # Create user account if it doesn't exist
    async with AsyncSessionLocal() as s:
        # Check if user already exists
        existing_user = (await s.execute(
            text("SELECT user_id FROM users WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if not existing_user:
            # Create new user account
            await s.execute(
                text("INSERT INTO users (user_id, username, approved, total_views, total_reels, max_slots, used_slots) VALUES (:u, :n, :a, :v, :r, :m, :s)"),
                {"u": user_id, "n": username, "a": False, "v": 0, "r": 0, "m": 50, "s": 0}
            )
            await s.commit()
            
            welcome_msg = f"🎉 Welcome to Instagram Reel Tracker Bot, {username}!\n\n"
            welcome_msg += "✅ Your account has been created successfully!\n\n"
        else:
            welcome_msg = f"👋 Welcome back, {username}!\n\n"
    
    # Show help text
    help_text = """🤖 <b>Instagram Reel Tracker Bot</b>

📋 <b>Available Commands:</b>
• <code>/submit &lt;url&gt;</code> - Submit Instagram reel URLs for tracking
• <code>/profile</code> - View your profile and statistics  
• <code>/addusdt &lt;address&gt;</code> - Add USDT ERC20 payment address
• <code>/addpaypal &lt;email&gt;</code> - Add PayPal payment email
• <code>/addupi &lt;address&gt;</code> - Add UPI payment address
• <code>/addaccount &lt;instagram_handle&gt;</code> - Request to link Instagram account

💡 <b>Getting Started:</b>
1. Add at least one payment method (USDT, PayPal, or UPI)
2. Request to link your Instagram account with <code>/addaccount</code>
3. Wait for admin approval
4. Start submitting reels with <code>/submit</code>

📊 <b>Features:</b>
• Track Instagram reel views, likes, and comments
• Automatic view updates
• Detailed statistics and analytics
• Secure payment processing

❓ Need help? Contact an admin for support."""

    if await is_admin(user_id):
        help_text += """

🔧 <b>Admin Commands:</b>
• <code>/userstats</code> - View user statistics
• <code>/currentaccounts</code> - View linked accounts
• <code>/creatorstats &lt;user_id&gt;</code> - View creator stats
• <code>/addviews &lt;shortcode&gt; &lt;views&gt;</code> - Add custom views
• <code>/banuser &lt;user_id&gt;</code> - Ban a user
• <code>/unban &lt;user_id&gt;</code> - Unban a user
• <code>/broadcast &lt;message&gt;</code> - Send message to all users
• <code>/export</code> - Export data
• <code>/forceupdate</code> - Force update all reel views
• <code>/addadmin &lt;user_id&gt;</code> - Add admin
• <code>/removeadmin &lt;user_id&gt;</code> - Remove admin
• <code>/review &lt;user_id&gt;</code> - Review account requests"""

    await update.message.reply_text(welcome_msg + help_text, parse_mode=ParseMode.HTML)

@debug_handler
async def addaccount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request to link an Instagram account"""
    if not context.args:
        return await update.message.reply_text(
            "❗ Please provide your Instagram handle.\n"
            "Usage: /addaccount <@handle>\n"
            "Example: /addaccount johndoe"
        )
    
    handle = context.args[0].lstrip("@")
    user_id = update.effective_user.id
    
    async with AsyncSessionLocal() as s:
        # Check if user already has 15 linked accounts
        account_count = (await s.execute(
            text("SELECT COUNT(*) FROM allowed_accounts WHERE user_id = :u"),
            {"u": user_id}
        )).scalar() or 0
        
        if account_count >= 15:
            return await update.message.reply_text(
                "❌ You have reached the maximum limit of 15 Instagram accounts.\n"
                "Please remove some accounts using /removeaccount before adding new ones."
            )
        
        # Check if this handle is already linked
        existing_handle = (await s.execute(
            text("SELECT 1 FROM allowed_accounts WHERE user_id = :u AND insta_handle = :h"),
            {"u": user_id, "h": handle}
        )).fetchone()
        
        if existing_handle:
            return await update.message.reply_text(f"❌ You have already linked @{handle}")
        
        # Check number of pending requests
        pending_count = (await s.execute(
            text("SELECT COUNT(*) FROM account_requests WHERE user_id = :u AND status = 'pending'"),
            {"u": user_id}
        )).scalar() or 0
        
        if pending_count >= 5:
            return await update.message.reply_text(
                "❌ You have reached the maximum limit of 5 pending requests.\n"
                "Please wait for admin approval of your existing requests before adding more."
            )
        
        # Check if there's already a pending request for this handle
        pending = (await s.execute(
            text("""
                SELECT 1 FROM account_requests 
                WHERE user_id = :u AND insta_handle = :h AND status = 'pending'
            """),
            {"u": user_id, "h": handle}
        )).fetchone()
        
        if pending:
            return await update.message.reply_text(
                f"❌ You already have a pending request for @{handle}.\n"
                "Please wait for admin approval."
            )
        
        # Create new request
        await s.execute(
            text("""
                INSERT INTO account_requests (user_id, insta_handle)
                VALUES (:u, :h)
            """),
            {"u": user_id, "h": handle}
        )
        await s.commit()
        
        # Notify admins
        admin_notifications_sent = 0
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"🔔 <b>New Account Link Request</b>\n\n"
                    f"👤 <b>User:</b> {update.effective_user.full_name}\n"
                    f"📱 <b>Username:</b> @{update.effective_user.username or 'None'}\n"
                    f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
                    f"📸 <b>Instagram:</b> @{handle}\n"
                    f"📊 <b>Current Accounts:</b> {account_count}/15\n"
                    f"⏳ <b>Pending Requests:</b> {pending_count + 1}/5\n\n"
                    f"🔧 <b>Action Required:</b>\n"
                    f"Use <code>/review {user_id}</code> to approve/reject",
                    parse_mode=ParseMode.HTML
                )
                admin_notifications_sent += 1
                logger.info(f"✅ Admin notification sent to {admin_id}")
            except Exception as e:
                logger.error(f"❌ Failed to notify admin {admin_id}: {e}")
        
        # Log admin notification status
        if admin_notifications_sent == 0:
            logger.warning(f"⚠️ No admin notifications were sent for user {user_id} account request")
        else:
            logger.info(f"✅ {admin_notifications_sent} admin(s) notified about account request from user {user_id}")
        
        await update.message.reply_text(
            f"✅ Your request to link @{handle} has been submitted.\n"
            f"📊 Current accounts: {account_count}/15\n"
            f"⏳ Pending requests: {pending_count + 1}/5\n"
            f"📧 {admin_notifications_sent} admin(s) notified\n"
            "Please wait for admin approval."
        )

@debug_handler
async def banuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban a user and delete all their data"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /banuser <user_id>\n"
            "Example: /banuser 123456789\n\n"
            "This will ban the user and delete all their data."
        )
    
    try:
        user_id = int(context.args[0])
        
        async with AsyncSessionLocal() as s:
            # Check if user exists
            user = await s.execute(
                text("SELECT 1 FROM users WHERE user_id = :u"),
                {"u": user_id}
            )
            
            if not user.scalar():
                return await update.message.reply_text("❌ User not found in database")
            
            # Check if user is already banned
            banned = await s.execute(
                text("SELECT 1 FROM banned_users WHERE user_id = :u"),
                {"u": user_id}
            )
            
            if banned.scalar():
                return await update.message.reply_text("❌ User is already banned")
            
            # Ban the user
            await s.execute(
                text("INSERT INTO banned_users (user_id) VALUES (:u)"),
                {"u": user_id}
            )
            
            # Delete user data
            await s.execute(text("DELETE FROM reels WHERE user_id = :u"), {"u": user_id})
            await s.execute(text("DELETE FROM allowed_accounts WHERE user_id = :u"), {"u": user_id})
            await s.execute(text("DELETE FROM payment_details WHERE user_id = :u"), {"u": user_id})
            await s.execute(text("DELETE FROM account_requests WHERE user_id = :u"), {"u": user_id})
            await s.execute(text("DELETE FROM users WHERE user_id = :u"), {"u": user_id})
            
            await s.commit()
            
            await update.message.reply_text(
                f"✅ User {user_id} has been banned and all their data has been deleted."
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")

@debug_handler
async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban a user"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /unban <user_id>\n"
            "Example: /unban 123456789\n\n"
            "This will unban the user so they can use the bot again."
        )
    
    try:
        user_id = int(context.args[0])
        
        async with AsyncSessionLocal() as s:
            # Check if user is banned
            banned = await s.execute(
                text("SELECT 1 FROM banned_users WHERE user_id = :u"),
                {"u": user_id}
            )
            
            if not banned.scalar():
                return await update.message.reply_text("❌ User is not banned")
            
            # Unban the user
            await s.execute(
                text("DELETE FROM banned_users WHERE user_id = :u"),
                {"u": user_id}
            )
            await s.commit()
            
            await update.message.reply_text(
                f"✅ User {user_id} has been unbanned."
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")


@debug_handler
async def removeallaccs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove all Instagram accounts for a user and their associated reels"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /removeallaccs <user_id>\n"
            "Example: /removeallaccs 123456789"
        )
    
    try:
        user_id = int(context.args[0])
        
        async with AsyncSessionLocal() as s:
            # Get count of accounts before deletion
            account_count = (await s.execute(
                text("SELECT COUNT(*) FROM allowed_accounts WHERE user_id = :u"),
                {"u": user_id}
            )).scalar() or 0
            
            if account_count == 0:
                return await update.message.reply_text("❌ User has no linked accounts")
            
            # Delete all accounts
            await s.execute(
                text("DELETE FROM allowed_accounts WHERE user_id = :u"),
                {"u": user_id}
            )
            
            # Delete all reels for this user
            reel_count = (await s.execute(
                text("SELECT COUNT(*) FROM reels WHERE user_id = :u"),
                {"u": user_id}
            )).scalar() or 0
            
            await s.execute(
                text("DELETE FROM reels WHERE user_id = :u"),
                {"u": user_id}
            )
            
            # Reset user's total views
            await s.execute(
                text("UPDATE users SET total_views = 0 WHERE user_id = :u"),
                {"u": user_id}
            )
            
            await s.commit()
            
            # Get username for confirmation
            try:
                chat = await context.bot.get_chat(user_id)
                user_name = chat.username or str(user_id)
            except:
                user_name = str(user_id)
            
            await update.message.reply_text(
                f"✅ Removed all {account_count} accounts and {reel_count} reels for @{user_name}\n"
                f"Total views have been reset to 0."
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")

@debug_handler
async def removeacc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a specific Instagram account for a user and its associated reels"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    if len(context.args) != 2:
        return await update.message.reply_text(
            "Usage: /removeacc <user_id> <@handle>\n"
            "Example: /removeacc 123456789 johndoe"
        )
    
    try:
        user_id = int(context.args[0])
        handle = context.args[1].lstrip("@")
        
        async with AsyncSessionLocal() as s:
            # Check if account exists
            exists = await s.execute(
                text("""
                    SELECT 1 FROM allowed_accounts 
                    WHERE user_id = :u AND insta_handle = :h
                """),
                {"u": user_id, "h": handle}
            )
            
            if not exists.scalar():
                return await update.message.reply_text(
                    f"❌ @{handle} is not linked to this user"
                )
            
            # Delete the account
            await s.execute(
                text("""
                    DELETE FROM allowed_accounts 
                    WHERE user_id = :u AND insta_handle = :h
                """),
                {"u": user_id, "h": handle}
            )
            
            # Delete all reels that belong to this Instagram handle
            # First get all reels for this user
            reels = await s.execute(
                text("SELECT shortcode FROM reels WHERE user_id = :u"),
                {"u": user_id}
            )
            reel_codes = [r[0] for r in reels.fetchall()]
            
            # Then check which reels belong to this handle
            reels_to_delete = []
            for code in reel_codes:
                try:
                    reel_data = await get_reel_data(code)
                    if reel_data['owner_username'].lower() == handle.lower():
                        reels_to_delete.append(code)
                except Exception:
                    continue
            
            # Delete the matching reels
            for code in reels_to_delete:
                await s.execute(
                    text("DELETE FROM reels WHERE shortcode = :c AND user_id = :u"),
                    {"c": code, "u": user_id}
                )
            
            # Recalculate total views for this user
            remaining_reels = await s.execute(
                text("SELECT shortcode FROM reels WHERE user_id = :u"),
                {"u": user_id}
            )
            remaining_codes = [r[0] for r in remaining_reels.fetchall()]
            
            total_views = 0
            for code in remaining_codes:
                try:
                    reel_data = await get_reel_data(code)
                    total_views += reel_data['view_count']
                except Exception as e:
                    logger.error(f"Error getting views for reel {code}: {e}")
            
            await s.execute(
                text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                {"v": total_views, "u": user_id}
            )
            
            await s.commit()
            
            # Get username for confirmation
            try:
                chat = await context.bot.get_chat(user_id)
                user_name = chat.username or str(user_id)
            except:
                user_name = str(user_id)
            
            await update.message.reply_text(
                f"✅ Removed @{handle} from @{user_name}\n"
                f"Deleted {len(reels_to_delete)} reels associated with this account.\n"
                f"Updated total views: {total_views:,}"
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")


@debug_handler
async def addviews_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add views to a user's total"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 2:
        return await update.message.reply_text("Usage: /addviews <user_id> <views>")
    
    try:
        user_id = int(context.args[0])
        views_to_add = int(context.args[1])
        
        async with AsyncSessionLocal() as s:
            exists = (await s.execute(text("SELECT 1 FROM users WHERE user_id=:u"), {"u": user_id})).scalar()
            if exists:
                await s.execute(
                    text("UPDATE users SET total_views=total_views+:v WHERE user_id=:u"),
                    {"v": views_to_add, "u": user_id}
                )
            else:
                await s.execute(
                    text("INSERT INTO users(user_id,username,total_views) VALUES(:u,NULL,:v)"),
                    {"u": user_id, "v": views_to_add}
                )
            await s.commit()
            
            # Get username for confirmation
            try:
                chat = await context.bot.get_chat(user_id)
                user_name = chat.username or str(user_id)
            except:
                user_name = str(user_id)
            
            await update.message.reply_text(f"✅ Added {views_to_add:,} views to @{user_name}")
            
    except ValueError:
        await update.message.reply_text("❌ Invalid input. Please provide valid numbers.")

@debug_handler
async def submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle reel submission with automatic task creation"""
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    
    try:
        # Check if user exists using async session
        async with AsyncSessionLocal() as s:
            user_result = await s.execute(
                text("SELECT user_id, approved, last_submission, total_reels, total_views, used_slots, max_slots FROM users WHERE user_id = :u"),
                {"u": user_id}
            )
            user_data = user_result.fetchone()
            
            if not user_data:
                await update.message.reply_text("❌ You need to register first. Use /start to begin.")
                return
            
            user_id_db, approved, last_submission, total_reels, total_views, used_slots, max_slots = user_data
            
            # Check if user is approved (or if user is admin)
            is_user_admin = user_id in ADMIN_IDS
            if not approved and not is_user_admin:
                await update.message.reply_text("❌ Your account is pending approval. Please wait for admin approval.")
                return
            
            # Check cooldown
            if last_submission:
                time_since_last = datetime.now() - last_submission
                cooldown_minutes = 5  # 5 minute cooldown
                if time_since_last.total_seconds() < cooldown_minutes * 60:
                    remaining = cooldown_minutes * 60 - time_since_last.total_seconds()
                    await update.message.reply_text(f"⏰ Please wait {int(remaining/60)} minutes and {int(remaining%60)} seconds before submitting again.")
                    return
        
        # Get the message text
        message_text = update.message.text.strip()
        if not message_text or message_text == '/submit':
            await update.message.reply_text("📝 Please provide Instagram reel URLs after the /submit command.\n\nExample: `/submit https://instagram.com/reel/ABC123/`")
            return
        
        # Extract URLs from message
        urls = []
        
        # Remove the /submit command and get the rest
        if message_text.startswith('/submit'):
            url_part = message_text[7:].strip()  # Remove '/submit' and whitespace
            if url_part:
                # Split by whitespace to handle multiple URLs
                potential_urls = url_part.split()
                for url in potential_urls:
                    url = url.strip()
                    if url:
                        # Clean up the URL
                        if 'instagram.com' in url or url.startswith('http'):
                            urls.append(url)
                        elif len(url) > 5:  # Assume it's a shortcode
                            urls.append(f"https://www.instagram.com/reel/{url}/")
        else:
            # Handle multi-line format
            lines = message_text.split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith('/submit'):
                    # Clean up the URL
                    if 'instagram.com' in line or line.startswith('http'):
                        urls.append(line)
                    elif len(line) > 5:  # Assume it's a shortcode
                        urls.append(f"https://www.instagram.com/reel/{line}/")
        
        if not urls:
            await update.message.reply_text("❌ No valid Instagram URLs found. Please provide valid Instagram reel URLs.")
            return
        
        # Validate URLs and check for duplicates
        valid_urls = []
        invalid_urls = []
        duplicate_urls = []
        
        async with AsyncSessionLocal() as s:
            for url in urls:
                if validate_instagram_link(url):
                    # Extract shortcode and check if it already exists
                    shortcode = extract_shortcode_from_url(url)
                    if shortcode:
                        existing = await s.execute(
                            text("SELECT 1 FROM reels WHERE shortcode = :sc"),
                            {"sc": shortcode}
                        )
                        if existing.fetchone():
                            duplicate_urls.append(url)
                        else:
                            valid_urls.append(url)
                    else:
                        invalid_urls.append(url)
                else:
                    invalid_urls.append(url)
        
        if not valid_urls:
            error_msg = "❌ No valid new Instagram reel URLs found."
            if duplicate_urls:
                error_msg += f"\n\n🔄 {len(duplicate_urls)} URL(s) already exist in database."
            if invalid_urls:
                error_msg += f"\n\n❌ {len(invalid_urls)} invalid URL(s) were skipped."
            await update.message.reply_text(error_msg)
            return
        
        # Notify user about skipped URLs if any
        if invalid_urls or duplicate_urls:
            skip_msg = ""
            if duplicate_urls:
                skip_msg += f"🔄 {len(duplicate_urls)} URL(s) already exist and were skipped.\n"
            if invalid_urls:
                skip_msg += f"❌ {len(invalid_urls)} invalid URL(s) were skipped.\n"
            skip_msg += f"📋 Processing {len(valid_urls)} new URL(s)..."
            await update.message.reply_text(skip_msg)
        
        # Create Apify task for scraping
        processing_msg = await update.message.reply_text(f"🔄 Creating scraping task for {len(valid_urls)} reel(s)...")
        
        try:
            # Get Apify client and create task
            apify_client = get_apify_client()
            task_id = await apify_client.create_scraping_task(valid_urls, "single")
            
            await processing_msg.edit_text(f"📋 Task created: {task_id}\n🔄 Processing {len(valid_urls)} reel(s)... This may take a few minutes.")
            
            # Wait for task completion with progress updates
            start_time = datetime.now()
            timeout = 300  # 5 minutes timeout
            
            # Wait for task completion using the built-in wait method
            result = await apify_client.get_task_results(task_id, wait=True, timeout=timeout)
            
            if result["status"] != "completed":
                if result["status"] == "timeout":
                    raise Exception("Task timed out after 5 minutes")
                else:
                    raise Exception(f"Task failed: {result.get('task_info', {}).get('error', 'Unknown error')}")
            
            await processing_msg.edit_text(f"📋 Task: {task_id}\n✅ Processing completed! Analyzing results...")
            
            # Process results and add to database
            successful_reels = []
            failed_reels = []
            
            # Check if results exist and are in the expected format
            if not result.get("results"):
                raise Exception("No results returned from Apify task")
            
            async with AsyncSessionLocal() as s:
                try:
                    for item in result["results"]:
                        try:
                            # Check if item has the expected structure
                            if not isinstance(item, dict):
                                failed_reels.append({"url": "unknown", "error": "Invalid result format"})
                                continue
                            
                            # Check if the item was successful
                            if not item.get("success", False):
                                error_msg = item.get("error", "Scraping failed")
                                url = item.get("url", "unknown")
                                failed_reels.append({"url": url, "error": error_msg})
                                continue
                            
                            # Extract URL and validate
                            url = item.get("url")
                            if not url:
                                failed_reels.append({"url": "unknown", "error": "No URL in result"})
                                continue
                            
                            # Extract shortcode from URL
                            shortcode = extract_shortcode_from_url(url)
                            if not shortcode:
                                failed_reels.append({"url": url, "error": "Could not extract shortcode"})
                                continue
                            
                            # Double-check if reel already exists (race condition protection)
                            existing_check = await s.execute(
                                text("SELECT 1 FROM reels WHERE shortcode = :sc"),
                                {"sc": shortcode}
                            )
                            if existing_check.fetchone():
                                failed_reels.append({"url": url, "error": "Reel already exists"})
                                continue
                            
                            # Insert new reel entry
                            await s.execute(
                                text("""
                                    INSERT INTO reels (user_id, shortcode, url, username, views, likes, comments, caption, media_url, submitted_at, last_updated)
                                    VALUES (:user_id, :shortcode, :url, :username, :views, :likes, :comments, :caption, :media_url, :submitted_at, :last_updated)
                                """),
                                {
                                    "user_id": user_id,
                                    "shortcode": shortcode,
                                    "url": url,
                                    "username": item.get("username", ""),
                                    "views": item.get("views", 0),
                                    "likes": item.get("likes", 0),
                                    "comments": item.get("comments", 0),
                                    "caption": item.get("caption", "")[:500],  # Limit caption length
                                    "media_url": item.get("media_url", ""),
                                    "submitted_at": datetime.now(),
                                    "last_updated": datetime.now()
                                }
                            )
                            
                            successful_reels.append({
                                "shortcode": shortcode,
                                "username": item.get("username", ""),
                                "views": item.get("views", 0)
                            })
                            
                        except Exception as e:
                            logger.error(f"Error processing individual reel: {str(e)}")
                            url = item.get("url", "unknown") if isinstance(item, dict) else "unknown"
                            failed_reels.append({"url": url, "error": str(e)})
                    
                    # Update user statistics
                    if successful_reels:
                        # Ensure user fields have default values if they're None
                        total_reels = total_reels or 0
                        total_views = total_views or 0
                        used_slots = used_slots or 0
                        
                        new_total_reels = total_reels + len(successful_reels)
                        new_total_views = total_views + sum(reel["views"] for reel in successful_reels)
                        new_used_slots = used_slots + len(successful_reels)
                        
                        await s.execute(
                            text("""
                                UPDATE users 
                                SET total_reels = :total_reels, total_views = :total_views, 
                                    used_slots = :used_slots, last_submission = :last_submission
                                WHERE user_id = :user_id
                            """),
                            {
                                "total_reels": new_total_reels,
                                "total_views": new_total_views,
                                "used_slots": new_used_slots,
                                "last_submission": datetime.now(),
                                "user_id": user_id
                            }
                        )
                    
                    # Log submission
                    await s.execute(
                        text("""
                            INSERT INTO submission_logs (user_id, username, action, details, timestamp)
                            VALUES (:user_id, :username, :action, :details, :timestamp)
                        """),
                        {
                            "user_id": user_id,
                            "username": username,
                            "action": "submit",
                            "details": f"Task: {task_id}, Success: {len(successful_reels)}, Failed: {len(failed_reels)}",
                            "timestamp": datetime.now()
                        }
                    )
                    
                    await s.commit()
                    
                except Exception as e:
                    await s.rollback()
                    logger.error(f"Database error in submit: {str(e)}")
                    raise Exception(f"Database error: {str(e)}")
            
            # Send results to user
            if successful_reels:
                result_text = f"✅ Successfully added {len(successful_reels)} reel(s):\n\n"
                for reel in successful_reels[:5]:  # Show first 5
                    result_text += f"📊 @{reel['username']} - {reel['views']:,} views\n"
                
                if len(successful_reels) > 5:
                    result_text += f"\n... and {len(successful_reels) - 5} more reels"
                
                new_total_reels = (total_reels or 0) + len(successful_reels)
                new_total_views = (total_views or 0) + sum(reel["views"] for reel in successful_reels)
                new_used_slots = (used_slots or 0) + len(successful_reels)
                
                result_text += f"\n\n📈 Your total: {new_total_reels} reels, {new_total_views:,} views"
                result_text += f"\n🎯 Slots used: {new_used_slots}/{max_slots or 50}"
                
                await processing_msg.edit_text(result_text)
            
            if failed_reels:
                error_text = f"❌ {len(failed_reels)} reel(s) failed:\n\n"
                for fail in failed_reels[:3]:  # Show first 3 failures
                    error_text += f"• {fail['url'][:50]}... - {fail['error']}\n"
                
                if len(failed_reels) > 3:
                    error_text += f"\n... and {len(failed_reels) - 3} more failures"
                
                await update.message.reply_text(error_text)
            
        except Exception as e:
            logger.error(f"Error in submit task processing: {str(e)}")
            await processing_msg.edit_text(f"❌ Error processing reels: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error in submit command: {str(e)}")
        await update.message.reply_text(f"❌ An error occurred: {str(e)}")

@debug_handler
async def remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = context.args[0] if context.args else None
    if not raw:
        return await update.message.reply_text("❗ Provide shortcode or URL.")
    m = re.search(r"instagram\.com/reel/(?P<code>[^/?#&]+)", raw)
    code = m.group("code") if m else raw
    uid = update.effective_user.id
    async with AsyncSessionLocal() as s:
        await s.execute(text("DELETE FROM reels WHERE shortcode=:c AND user_id=:u"), {"c": code, "u": uid})
        # Recalculate total_views for this user
        reels = await s.execute(text("SELECT shortcode FROM reels WHERE user_id = :u"), {"u": uid})
        codes = [r[0] for r in reels.fetchall()]
        total_views = 0
        for sc in codes:
            try:
                reel_data = await get_reel_data(sc)
                total_views += reel_data['view_count']
            except Exception:
                pass
        await s.execute(text("UPDATE users SET total_views = :v WHERE user_id = :u"), {"v": total_views, "u": uid})
        await s.commit()
    await update.message.reply_text("🗑️ Reel removed and stats updated.")

@debug_handler
async def cleardata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    async with AsyncSessionLocal() as s:
        await s.execute(text("DELETE FROM reels"))
        await s.execute(text("UPDATE users SET total_views = 0"))
        await s.commit()
    await update.message.reply_text("✅ All reels and view counts cleared.")

@debug_handler
async def userstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    await send_userstats_page(update, context, page)

#userstats pagination function
async def send_userstats_page(source, context, page):
    page_size = 1  # 1 user per page for clarity
    async with AsyncSessionLocal() as s:
        users = (await s.execute(text("""
            SELECT 
                u.user_id, u.username, u.total_views, 
                p.usdt_address, p.paypal_email, p.upi_address
            FROM users u
            LEFT JOIN payment_details p ON u.user_id = p.user_id
            WHERE u.total_views > 0
            ORDER BY u.total_views DESC
        """))).fetchall()

        if not users:
            return await source.message.reply_text("ℹ️ No user stats available.")

        paged_users, total_pages = paginate_list(users, page, page_size)
        user_id, username, views, usdt, paypal, upi = paged_users[0]

        total_videos = (await s.execute(
            text("SELECT COUNT(*) FROM reels WHERE user_id = :u"),
            {"u": user_id}
        )).scalar() or 0
        handles = [r[0] for r in (await s.execute(
            text("SELECT insta_handle FROM allowed_accounts WHERE user_id = :u"),
            {"u": user_id}
        )).fetchall()]
        try:
            chat = await context.bot.get_chat(user_id)
            name = chat.full_name
        except:
            name = str(user_id)

        payable = (views / 1000) * 0.025
        msg = [
            f"👤 <b>{name}</b> (@{username or '—'})",
            f"• Views: <b>{int(views):,}</b>",
            f"• Videos: <b>{total_videos}</b>",
            f"• Accounts: {', '.join(f'@{h}' for h in handles) if handles else '—'}",
            f"• 💰 Payable: ${payable:.2f}",
            "",
            "<b>💳 Payment Methods:</b>",
            f"• USDT: {usdt or '—'}",
            f"• PayPal: {paypal or '—'}",
            f"• UPI: {upi or '—'}",
            f"\n📄 Page {page}/{total_pages}"
        ]

        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"userstats_{page - 1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"userstats_{page + 1}"))
        markup = InlineKeyboardMarkup([buttons]) if buttons else None

        send_fn = source.edit_message_text if hasattr(source, "edit_message_text") else source.message.reply_text
        await send_fn("\n".join(msg), parse_mode=ParseMode.HTML, reply_markup=markup)

@debug_handler
async def handle_userstats_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    match = re.match(r"userstats_(\d+)", query.data)
    if not match:
        return
    page = int(match.group(1))
    await send_userstats_page(query, context, page)


@debug_handler
async def currentaccounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    await send_currentaccounts_page(update, context, page)

#Current account Pagination Function
async def send_currentaccounts_page(source, context, page):
    page_size = 10
    async with AsyncSessionLocal() as s:
        accounts = await s.execute(text("""
            SELECT a.user_id, a.insta_handle, u.username, u.total_views
            FROM allowed_accounts a
            JOIN users u ON a.user_id = u.user_id
            ORDER BY LOWER(a.insta_handle)
        """))
        rows = accounts.fetchall()

        if not rows:
            return await source.message.reply_text("ℹ️ No Instagram accounts are currently linked.")

        grouped = {}
        for user_id, handle, username, views in rows:
            grouped.setdefault(handle.lower(), []).append((user_id, username, views))

        handles = sorted(grouped)
        paged_handles, total_pages = paginate_list(handles, page, page_size)

        msg = [f"📱 <b>Linked Accounts (Page {page}/{total_pages})</b>\n"]
        for handle in paged_handles:
            msg.append(f"🔹 <b>@{handle}</b>")
            for user_id, username, views in grouped[handle]:
                user_display = f"@{username}" if username else f"User {user_id}"
                msg.append(f"   └ {user_display} — {int(views):,} views")
            msg.append("")

        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"currentaccounts_{page - 1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"currentaccounts_{page + 1}"))
        markup = InlineKeyboardMarkup([buttons]) if buttons else None

        send_fn = source.edit_message_text if hasattr(source, "edit_message_text") else source.message.reply_text
        await send_fn("\n".join(msg), parse_mode=ParseMode.HTML, reply_markup=markup)

@debug_handler
async def handle_currentaccounts_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    match = re.match(r"currentaccounts_(\d+)", query.data)
    if not match:
        return
    page = int(match.group(1))
    await send_currentaccounts_page(query, context, page)


@debug_handler
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    if not context.args:
        return await update.message.reply_text("Usage: /broadcast <message>")
    message = " ".join(context.args)
    async with AsyncSessionLocal() as s:
        uids = [r[0] for r in (await s.execute(text("SELECT DISTINCT user_id FROM allowed_accounts"))).fetchall()]
    for uid in uids:
        try:
            await context.bot.send_message(uid, message)
        except Exception as e:
            logger.warning(f"Broadcast to {uid} failed: {e}")
    await update.message.reply_text("✅ Broadcast sent.", parse_mode=ParseMode.HTML)

@debug_handler
async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    async with AsyncSessionLocal() as s:
        users = (await s.execute(text(
            "SELECT u.user_id,u.username,u.total_views,a.insta_handle "
            "FROM users u LEFT JOIN allowed_accounts a ON u.user_id=a.user_id"
        ))).fetchall()
        reels = (await s.execute(text("SELECT user_id,shortcode FROM reels"))).fetchall()
    lines = []
    for uid, uname, views, insta in users:
        acct = f"@{insta}" if insta else "—"
        lines.append(f"User {uid} ({uname or '—'}), Views: {views}, Insta: {acct}")
        for u, sc in reels:
            if u == uid:
                lines.append(f"  • https://www.instagram.com/reel/{sc}/")
        lines.append("")
    import io
    buf = io.BytesIO("\n".join(lines).encode()); buf.name = "stats.txt"
    await update.message.reply_document(document=buf, filename="stats.txt")

def paginate_list(items, page, page_size):
    total_pages = (len(items) + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end], total_pages

@debug_handler
async def creatorstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    await send_creatorstats_page(update, context, uid, page)

#Creator stats pagination function
async def send_creatorstats_page(source, context, uid, page):
    page_size = 10
    async with AsyncSessionLocal() as s:
        total_views = (await s.execute(
            text("SELECT COALESCE(total_views, 0) FROM users WHERE user_id = :u"),
            {"u": uid}
        )).scalar() or 0
        payable_amount = (total_views / 1000) * 0.025

        links = [r[0] for r in (await s.execute(
            text("SELECT shortcode FROM reels WHERE user_id = :u"),
            {"u": uid}
        )).fetchall()]
        handles = [r[0] for r in (await s.execute(
            text("SELECT insta_handle FROM allowed_accounts WHERE user_id = :u"),
            {"u": uid}
        )).fetchall()]
        payment_details = (await s.execute(
            text("SELECT usdt_address, paypal_email, upi_address FROM payment_details WHERE user_id = :u"),
            {"u": uid}
        )).fetchone()
        usdt, paypal, upi = payment_details or (None, None, None)

        # Get referral information
        # Get who referred this user
        referrer_data = await s.execute(
            text("""
                SELECT r.referrer_id, u.username
                FROM referrals r
                JOIN users u ON r.referrer_id = u.user_id
                WHERE r.user_id = :u
            """),
            {"u": uid}
        )
        referrer_info = referrer_data.fetchone()
        
        # Get users referred by this user
        referred_data = await s.execute(
            text("""
                SELECT r.user_id, u.username, u.total_views
                FROM referrals r
                JOIN users u ON r.user_id = u.user_id
                WHERE r.referrer_id = :u
            """),
            {"u": uid}
        )
        referred_users = referred_data.fetchall()
        
        # Get global commission rate
        commission_rate_data = await s.execute(
            text("""
                SELECT value
                FROM config
                WHERE key = 'referral_commission_rate'
            """)
        )
        commission_rate = float(commission_rate_data.scalar() or 0)

        # Calculate referral stats
        total_referral_views = 0
        if referred_users:
            for ref_id, ref_username, views in referred_users:
                views = views or 0
                total_referral_views += views
        
        commission_amount = (total_referral_views / 1000) * 0.025 * (commission_rate / 100)

        msg = [
            "📊 <b>Your Creator Statistics</b>",
            f"• Total Views: <b>{int(total_views):,}</b>",
            f"• Total Videos: <b>{len(links)}</b>",
            f"• Linked Accounts: <b>{len(handles)}</b>",
            f"• Payable Amount: <b>${payable_amount:.2f}</b>",
            "",
            "<i>Pay Rate: <b>$25 per 1M Views</b></i>",
            ""
        ]

        # Add referral overview if user has referrals
        if referred_users or referrer_info:
            msg.extend([
                "🤝 <b>Referral Overview</b>",
                f"• Users Referred: <b>{len(referred_users)}</b>",
                f"• Referral Views: <b>{total_referral_views:,}</b>",
                f"• Commission Rate: <b>{commission_rate:.2f}%</b>",
                f"• Commission Earned: <b>${commission_amount:.2f}</b>",
                ""
            ])
            
            if referrer_info:
                referrer_id, referrer_username = referrer_info
                try:
                    referrer_chat = await context.bot.get_chat(referrer_id)
                    referrer_name = referrer_chat.username or str(referrer_id)
                except:
                    referrer_name = str(referrer_id)
                msg.append(f"• Referred by: <b>@{referrer_name}</b>")
                msg.append("")

        # Add linked Instagram accounts
        if handles:
            msg.append("📸 <b>Linked Instagram Accounts</b>")
            msg.extend([f"• @{handle}" for handle in handles])
            msg.append("")

        # Add payment methods
        payment_methods = []
        if usdt: payment_methods.append(f"USDT: <code>{usdt}</code>")
        if paypal: payment_methods.append(f"PayPal: <code>{paypal}</code>")
        if upi: payment_methods.append(f"UPI: <code>{upi}</code>")
        
        if payment_methods:
            msg.append("💳 <b>Payment Methods</b>")
            msg.extend([f"• {method}" for method in payment_methods])
            msg.append("")

        # Pagination of reel links
        links_page, total_pages = paginate_list(links, page, page_size)
        msg.append(f"🎥 <b>Your Reels (Page {page}/{total_pages})</b>")
        if links_page:
            msg.extend([f"• https://www.instagram.com/reel/{sc}/" for sc in links_page])
        else:
            msg.append("• No reels submitted yet")

        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"creatorstats_{page - 1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"creatorstats_{page + 1}"))
        markup = InlineKeyboardMarkup([buttons]) if buttons else None

        send_fn = source.edit_message_text if hasattr(source, "edit_message_text") else source.message.reply_text
        await send_fn("\n".join(msg), parse_mode=ParseMode.HTML, reply_markup=markup)


@debug_handler
async def handle_creatorstats_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    match = re.match(r"creatorstats_(\d+)", query.data)
    if not match:
        return
    page = int(match.group(1))
    await send_creatorstats_page(query, context, query.from_user.id, page)

@debug_handler
async def currentstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current total views, payable amount, and working accounts"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    async with AsyncSessionLocal() as s:
        # Get total views across all users
        total_views = (await s.execute(text("SELECT COALESCE(SUM(total_views), 0) FROM users"))).scalar() or 0
        total_views = float(total_views)
        
        # Get total working accounts (users with linked Instagram accounts)
        working_accounts = (await s.execute(text("SELECT COUNT(DISTINCT user_id) FROM allowed_accounts"))).scalar() or 0
        
        # Get total videos
        total_videos = (await s.execute(text("SELECT COUNT(*) FROM reels"))).scalar() or 0
        
        # Calculate payable amount (views * 0.025 per 1000 views)
        payable_amount = (total_views / 1000) * 0.025
        
        # Format the message
        msg = [
            "📊 <b>Current Statistics</b>",
            f"• Total Views: <b>{int(total_views):,}</b>",
            f"• Total Videos: <b>{total_videos:,}</b>",
            f"• Working Accounts: <b>{working_accounts}</b>",
            f"• Payable Amount: <b>${payable_amount:,.2f}</b>",
            "",
            "<i>Pay Rate: <b>$25 per 1M Views</b></i>"
        ]
        
        await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)


@debug_handler
async def userinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")

    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Usage: /userinfo <user_id>")

    target_uid = int(context.args[0])
    await send_userinfo_page(update, context, target_uid, page=1)

#userinfo pagination Function
async def send_userinfo_page(source, context, uid, page):
    page_size = 10
    async with AsyncSessionLocal() as s:
        # Basic info
        total_views = (await s.execute(
            text("SELECT COALESCE(total_views, 0) FROM users WHERE user_id = :u"),
            {"u": uid}
        )).scalar() or 0
        payable = (total_views / 1000) * 0.025
        handles = [r[0] for r in (await s.execute(
            text("SELECT insta_handle FROM allowed_accounts WHERE user_id = :u"),
            {"u": uid}
        )).fetchall()]
        payment = (await s.execute(
            text("SELECT usdt_address, paypal_email, upi_address FROM payment_details WHERE user_id = :u"),
            {"u": uid}
        )).fetchone()
        usdt, paypal, upi = payment or (None, None, None)
        reels = [r[0] for r in (await s.execute(
            text("SELECT shortcode FROM reels WHERE user_id = :u"),
            {"u": uid}
        )).fetchall()]

        try:
            chat = await context.bot.get_chat(uid)
            name = chat.full_name
            username = f"@{chat.username}" if chat.username else f"ID {uid}"
        except:
            name = f"ID {uid}"
            username = f"ID {uid}"

        # Pagination
        paged_reels, total_pages = paginate_list(reels, page, page_size)

        # Message
        msg = [
            f"👤 <b>{name}</b> ({username})",
            f"📸 Instagram: {', '.join(f'@{h}' for h in handles) if handles else '—'}",
            f"🎥 Total Reels: <b>{len(reels)}</b>",
            f"👁️ Total Views: <b>{int(total_views):,}</b>",
            f"💰 Payable Amount: <b>${payable:.2f}</b>",
            "",
            "💳 <b>Payment Methods</b>",
            f"• USDT: {usdt or '—'}",
            f"• PayPal: {paypal or '—'}",
            f"• UPI: {upi or '—'}",
            f"\n🎞️ <b>Reels (Page {page}/{total_pages})</b>"
        ]
        msg += [f"• https://www.instagram.com/reel/{sc}/" for sc in paged_reels] or ["• No reels submitted."]

        # Buttons
        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"userinfo_{uid}_{page-1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"userinfo_{uid}_{page+1}"))
        markup = InlineKeyboardMarkup([buttons]) if buttons else None

        send_fn = source.edit_message_text if hasattr(source, "edit_message_text") else source.message.reply_text
        await send_fn("\n".join(msg), parse_mode=ParseMode.HTML, reply_markup=markup)

@debug_handler
async def handle_userinfo_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    match = re.match(r"userinfo_(\d+)_(\d+)", query.data)
    if not match:
        return
    uid = int(match.group(1))
    page = int(match.group(2))
    await send_userinfo_page(query, context, uid, page)

@debug_handler
async def add_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add or update USDT ERC20 address"""
    if not context.args:
        return await update.message.reply_text(
            "❗ Please provide your USDT ERC20 address.\n"
            "Usage: /addusdt <your usdt erc20 address>\n"
            "Example: /addusdt 0x1234567890abcdef1234567890abcdef12345678"
        )
    
    usdt_address = " ".join(context.args).strip()
    user_id = update.effective_user.id
    
    # Basic validation
    if not usdt_address.startswith('0x') or len(usdt_address) < 10:
        return await update.message.reply_text("❌ Invalid USDT ERC20 address")
    
    async with AsyncSessionLocal() as s:
        # Check if user exists
        user = (await s.execute(
            text("SELECT 1 FROM users WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if not user:
            await s.execute(
                text("INSERT INTO users (user_id, username) VALUES (:u, :n)"),
                {"u": user_id, "n": update.effective_user.username}
            )
        
        # Check if payment details exist
        existing = (await s.execute(
            text("SELECT id FROM payment_details WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if existing:
            # Update USDT address
            await s.execute(
                text("UPDATE payment_details SET usdt_address = :a WHERE user_id = :u"),
                {"a": usdt_address, "u": user_id}
            )
            await update.message.reply_text(
                f"✅ Updated USDT address:\n`{usdt_address}`",
                parse_mode=ParseMode.HTML
            )
        else:
            # Insert new payment details with USDT
            await s.execute(
                text("""
                    INSERT INTO payment_details (user_id, usdt_address)
                    VALUES (:u, :a)
                """),
                {"u": user_id, "a": usdt_address}
            )
            await update.message.reply_text(
                f"✅ Added USDT address:\n`{usdt_address}`",
                parse_mode=ParseMode.HTML
            )
        
        await s.commit()

@debug_handler
async def add_paypal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add or update PayPal email"""
    if not context.args:
        return await update.message.reply_text(
            "❗ Please provide your PayPal email.\n"
            "Usage: /addpaypal <your paypal email>\n"
            "Example: /addpaypal john@example.com"
        )
    
    paypal_email = " ".join(context.args).strip()
    user_id = update.effective_user.id
    
    # Basic validation
    if '@' not in paypal_email or '.' not in paypal_email:
        return await update.message.reply_text("❌ Invalid PayPal email")
    
    async with AsyncSessionLocal() as s:
        # Check if user exists
        user = (await s.execute(
            text("SELECT 1 FROM users WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if not user:
            await s.execute(
                text("INSERT INTO users (user_id, username) VALUES (:u, :n)"),
                {"u": user_id, "n": update.effective_user.username}
            )
        
        # Check if payment details exist
        existing = (await s.execute(
            text("SELECT id FROM payment_details WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if existing:
            # Update PayPal email
            await s.execute(
                text("UPDATE payment_details SET paypal_email = :e WHERE user_id = :u"),
                {"e": paypal_email, "u": user_id}
            )
            await update.message.reply_text(
                f"✅ Updated PayPal email:\n`{paypal_email}`",
                parse_mode=ParseMode.HTML
            )
        else:
            # Insert new payment details with PayPal
            await s.execute(
                text("""
                    INSERT INTO payment_details (user_id, paypal_email)
                    VALUES (:u, :e)
                """),
                {"u": user_id, "e": paypal_email}
            )
            await update.message.reply_text(
                f"✅ Added PayPal email:\n`{paypal_email}`",
                parse_mode=ParseMode.HTML
            )
        
        await s.commit()

@debug_handler
async def add_upi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add or update UPI address"""
    if not context.args:
        return await update.message.reply_text(
            "❗ Please provide your UPI address.\n"
            "Usage: /addupi <your upi address>\n"
            "Example: /addupi username@upi"
        )
    
    upi_address = " ".join(context.args).strip()
    user_id = update.effective_user.id
    
    # Basic validation
    if not upi_address:
        return await update.message.reply_text("❌ Invalid UPI address")
    
    async with AsyncSessionLocal() as s:
        # Check if user exists
        user = (await s.execute(
            text("SELECT 1 FROM users WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if not user:
            await s.execute(
                text("INSERT INTO users (user_id, username) VALUES (:u, :n)"),
                {"u": user_id, "n": update.effective_user.username}
            )
        
        # Check if payment details exist
        existing = (await s.execute(
            text("SELECT id FROM payment_details WHERE user_id = :u"),
            {"u": user_id}
        )).fetchone()
        
        if existing:
            # Update UPI address
            await s.execute(
                text("UPDATE payment_details SET upi_address = :a WHERE user_id = :u"),
                {"a": upi_address, "u": user_id}
            )
            await update.message.reply_text(
                f"✅ Updated UPI address:\n`{upi_address}`",
                parse_mode=ParseMode.HTML
            )
        else:
            # Insert new payment details with UPI
            await s.execute(
                text("""
                    INSERT INTO payment_details (user_id, upi_address)
                    VALUES (:u, :a)
                """),
                {"u": user_id, "a": upi_address}
            )
            await update.message.reply_text(
                f"✅ Added UPI address:\n`{upi_address}`",
                parse_mode=ParseMode.HTML
            )
        
        await s.commit()

@debug_handler
async def review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Review and approve/reject account link requests"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /review <user_id>\n"
            "This will show pending requests for that user."
        )
    
    user_id = int(context.args[0])
    
    async with AsyncSessionLocal() as s:
        # Get pending request
        request = (await s.execute(
            text("""
                SELECT id, insta_handle, created_at 
                FROM account_requests 
                WHERE user_id = :u AND status = 'pending'
            """),
            {"u": user_id}
        )).fetchone()
        
        if not request:
            return await update.message.reply_text("No pending requests found for this user.")
        
        request_id, handle, created_at = request
        
        # Get user info
        try:
            chat = await context.bot.get_chat(user_id)
            user_info = f"{chat.full_name} (@{chat.username})"
        except:
            user_info = str(user_id)
        
        # Create inline keyboard for approve/reject
        keyboard = [
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{request_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{request_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"📝 Review request for {user_info}:\n"
            f"• Instagram: @{handle}\n"
            f"• Requested: {created_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"Choose an action:",
            reply_markup=reply_markup
        )

@debug_handler
async def handle_review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle approve/reject callbacks"""
    query = update.callback_query
    await query.answer()
    logger.info(f"Review callback triggered by user {query.from_user.id} with data: {query.data}")
    if not await is_admin(query.from_user.id):
        await query.edit_message_text("🚫 Unauthorized")
        return
    action, request_id = query.data.split('_')
    request_id = int(request_id)
    async with AsyncSessionLocal() as s:
        # Get request details
        request = (await s.execute(
            text("""
                SELECT user_id, insta_handle 
                FROM account_requests 
                WHERE id = :id AND status = 'pending'
            """),
            {"id": request_id}
        )).fetchone()
        logger.info(f"Fetched request: {request}")
        if not request:
            await query.edit_message_text("❌ Request not found or already processed.")
            return
        user_id, handle = request
        if action == 'approve':
            try:
                # Check for duplicate before insert
                exists = await s.execute(
                    text("SELECT 1 FROM allowed_accounts WHERE user_id = :u AND insta_handle = :h"),
                    {"u": user_id, "h": handle}
                )
                if exists.scalar():
                    await query.edit_message_text(
                        f"⚠️ This account is already linked for the user. No action taken.",
                        reply_markup=None
                    )
                    return
                await s.execute(
                    text("""
                        INSERT INTO allowed_accounts (user_id, insta_handle)
                        VALUES (:u, :h)
                    """),
                    {"u": user_id, "h": handle}
                )
                await s.execute(
                    text("UPDATE account_requests SET status = 'approved' WHERE id = :id"),
                    {"id": request_id}
                )
                
                # IMPORTANT: Set the user as approved so they can submit reels
                await s.execute(
                    text("UPDATE users SET approved = TRUE WHERE user_id = :u"),
                    {"u": user_id}
                )
                
                await s.commit()
                try:
                    await context.bot.send_message(
                        user_id,
                        f"✅ Your request to link @{handle} has been approved!\n"
                        f"You can now use /submit to submit your reels."
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {user_id}: {e}")
                await query.edit_message_text(
                    f"✅ Approved request for @{handle}",
                    reply_markup=None
                )
            except Exception as e:
                logger.error(f"Approval error: {e}")
                await query.edit_message_text(f"❌ Error during approval: {e}")
        else:  # reject
            try:
                await s.execute(
                    text("UPDATE account_requests SET status = 'rejected' WHERE id = :id"),
                    {"id": request_id}
                )
                await s.commit()
                try:
                    await context.bot.send_message(
                        user_id,
                        f"❌ Your request to link @{handle} has been rejected.\n"
                        f"Please contact an admin for more information."
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {user_id}: {e}")
                await query.edit_message_text(
                    f"❌ Rejected request for @{handle}",
                    reply_markup=None
                )
            except Exception as e:
                logger.error(f"Rejection error: {e}")
                await query.edit_message_text(f"❌ Error during rejection: {e}")

@debug_handler
async def forceupdate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force update all user view counts using batch task processing"""
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ You don't have permission to use this command.")
        return
    
    try:
        # Get all users with reels using async session
        async with AsyncSessionLocal() as s:
            users_result = await s.execute(
                text("""
                    SELECT DISTINCT u.user_id 
                    FROM users u 
                    JOIN reels r ON u.user_id = r.user_id
                """)
            )
            user_ids = [row[0] for row in users_result.fetchall()]
            
            if not user_ids:
                await update.message.reply_text("ℹ️ No users with reels found.")
                return
            
            # Get all reel URLs for batch processing
            reels_result = await s.execute(
                text("SELECT id, user_id, shortcode, url FROM reels")
            )
            all_reels = reels_result.fetchall()
        
        # Collect all reel URLs for batch processing
        all_reel_urls = []
        reel_mapping = {}  # URL -> reel info for updating database
        
        for reel_id, reel_user_id, shortcode, url in all_reels:
            reel_url = url or f"https://www.instagram.com/reel/{shortcode}/"
            all_reel_urls.append(reel_url)
            reel_mapping[reel_url] = {
                "reel_id": reel_id,
                "user_id": reel_user_id,
                "shortcode": shortcode
            }
        
        total_reels = len(all_reel_urls)
        status_msg = await update.message.reply_text(f"🔄 Starting batch update for {total_reels} reels from {len(user_ids)} users...")
        
        # Create batch scraping task
        apify_client = get_apify_client()
        task_id = await apify_client.create_scraping_task(all_reel_urls, "bulk_update")
        
        await status_msg.edit_text(f"📋 Batch task created: {task_id}\n🔄 Processing {total_reels} reels... This may take 10-15 minutes.")
        
        # Monitor task progress
        start_time = datetime.now()
        timeout = 1800  # 30 minutes timeout for bulk updates
        last_update = 0
        
        while True:
            elapsed = (datetime.now() - start_time).seconds
            
            # Check timeout first
            if elapsed >= timeout:
                logger.error(f"Task {task_id} timed out after 30 minutes")
                raise Exception("Batch task timed out after 30 minutes")
            
            result = await apify_client.get_task_results(task_id, wait=False)
            logger.info(f"Task {task_id} status: {result['status']}")
            
            if result["status"] == "completed":
                logger.info(f"Task {task_id} completed successfully")
                break
            elif result["status"] == "failed":
                error_msg = result.get('task_info', {}).get('error', 'Unknown error')
                logger.error(f"Task {task_id} failed: {error_msg}")
                raise Exception(f"Batch task failed: {error_msg}")
            
            # Update progress every 60 seconds
            if elapsed - last_update >= 60:
                last_update = elapsed
                logger.info(f"Task {task_id} still running after {elapsed} seconds")
                await status_msg.edit_text(f"📋 Task: {task_id}\n⏳ Processing {total_reels} reels... ({elapsed//60}m {elapsed%60}s elapsed)")
            
            # Wait 10 seconds before next check
            await asyncio.sleep(10)
        
        # Process results and update database using async session
        updated_count = 0
        failed_count = 0
        user_totals = {}  # user_id -> {"views": total, "reels": count}
        
        async with AsyncSessionLocal() as s:
            try:
                for item in result["results"]:
                    if item.get("success", False):
                        url = item["url"]
                        if url in reel_mapping:
                            try:
                                reel_info = reel_mapping[url]
                                reel_id = reel_info["reel_id"]
                                
                                # Get current reel data
                                reel_result = await s.execute(
                                    text("SELECT views FROM reels WHERE id = :id"),
                                    {"id": reel_id}
                                )
                                current_reel = reel_result.fetchone()
                                
                                if current_reel:
                                    old_views = current_reel[0]
                                    new_views = item.get("views", 0)
                                    
                                    # Update reel data
                                    await s.execute(
                                        text("""
                                            UPDATE reels 
                                            SET views = :views, likes = :likes, comments = :comments, last_updated = :last_updated
                                            WHERE id = :id
                                        """),
                                        {
                                            "views": new_views,
                                            "likes": item.get("likes", 0),
                                            "comments": item.get("comments", 0),
                                            "last_updated": datetime.now(),
                                            "id": reel_id
                                        }
                                    )
                                    
                                    # Track user totals
                                    user_id_reel = reel_info["user_id"]
                                    if user_id_reel not in user_totals:
                                        user_totals[user_id_reel] = {"views": 0, "reels": 0}
                                    
                                    user_totals[user_id_reel]["views"] += new_views
                                    user_totals[user_id_reel]["reels"] += 1
                                    
                                    updated_count += 1
                                    
                                    logger.info(f"Updated reel {reel_info['shortcode']}: {old_views} -> {new_views} views")
                                
                            except Exception as e:
                                logger.error(f"Error updating reel {url}: {str(e)}")
                                failed_count += 1
                    else:
                        failed_count += 1
                
                # Update user totals
                users_updated = 0
                for user_id_update, totals in user_totals.items():
                    try:
                        await s.execute(
                            text("""
                                UPDATE users 
                                SET total_views = :total_views, total_reels = :total_reels
                                WHERE user_id = :user_id
                            """),
                            {
                                "total_views": totals["views"],
                                "total_reels": totals["reels"],
                                "user_id": user_id_update
                            }
                        )
                        users_updated += 1
                    except Exception as e:
                        logger.error(f"Error updating user {user_id_update}: {str(e)}")
                
                await s.commit()
                
            except Exception as e:
                await s.rollback()
                raise e
        
        # Send completion message
        completion_time = datetime.now() - start_time
        result_text = f"✅ Batch update completed!\n\n"
        result_text += f"📊 Updated: {updated_count} reels\n"
        result_text += f"❌ Failed: {failed_count} reels\n"
        result_text += f"👥 Users updated: {users_updated}\n"
        result_text += f"⏱️ Time taken: {completion_time.seconds//60}m {completion_time.seconds%60}s"
        
        await status_msg.edit_text(result_text)
        
    except Exception as e:
        logger.error(f"Error in forceupdate: {str(e)}")
        await update.message.reply_text(f"❌ An error occurred: {str(e)}")

@debug_handler
async def addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new admin"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 1:
        return await update.message.reply_text("Usage: /addadmin <user_id>")
    
    try:
        new_admin_id = int(context.args[0])
        current_admin_id = update.effective_user.id
        
        async with AsyncSessionLocal() as s:
            # Check if user is already an admin
            existing = await s.execute(
                text("SELECT 1 FROM admins WHERE user_id = :u"),
                {"u": new_admin_id}
            )
            if existing.scalar():
                return await update.message.reply_text("❌ User is already an admin")
            
            # Add new admin
            await s.execute(
                text("INSERT INTO admins (user_id, added_by) VALUES (:u, :a)"),
                {"u": new_admin_id, "a": current_admin_id}
            )
            await s.commit()
            
            # Notify new admin
            try:
                await context.bot.send_message(
                    new_admin_id,
                    "👑 You have been added as an admin!"
                )
            except Exception as e:
                logger.error(f"Failed to notify new admin {new_admin_id}: {e}")
            
            await update.message.reply_text(f"✅ Added {new_admin_id} as admin")
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")

@debug_handler
async def removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove an admin"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 1:
        return await update.message.reply_text("Usage: /removeadmin <user_id>")
    
    try:
        admin_id = int(context.args[0])
        current_admin_id = update.effective_user.id
        
        # Prevent removing self
        if admin_id == current_admin_id:
            return await update.message.reply_text("❌ You cannot remove yourself as admin")
        
        async with AsyncSessionLocal() as s:
            # Check if user is an admin
            existing = await s.execute(
                text("SELECT 1 FROM admins WHERE user_id = :u"),
                {"u": admin_id}
            )
            if not existing.scalar():
                return await update.message.reply_text("❌ User is not an admin")
            
            # Remove admin
            await s.execute(
                text("DELETE FROM admins WHERE user_id = :u"),
                {"u": admin_id}
            )
            await s.commit()
            
            # Notify removed admin
            try:
                await context.bot.send_message(
                    admin_id,
                    "👋 You have been removed as an admin"
                )
            except Exception as e:
                logger.error(f"Failed to notify removed admin {admin_id}: {e}")
            
            await update.message.reply_text(f"✅ Removed {admin_id} from admins")
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")

@debug_handler
async def clearbad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove submissions with less than 10k views after 5 minutes and update user total views."""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    async with AsyncSessionLocal() as s:
        # Get submissions older than 5 minutes
        old_submissions = await s.execute(text("""
            SELECT r.shortcode, r.user_id, u.total_views, u.username
            FROM reels r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.created_at < NOW() - INTERVAL '5 minutes'
        """))
        
        removed_count = 0
        affected_users = set()
        removal_details = []
        
        for shortcode, user_id, views, username in old_submissions:
            try:
                # Get current view count from API
                reel_data = await get_reel_data(shortcode)
                current_views = reel_data['view_count']
                
                if current_views < 10000:
                    # Remove the submission
                    await s.execute(
                        text("DELETE FROM reels WHERE shortcode = :s AND user_id = :u"),
                        {"s": shortcode, "u": user_id}
                    )
                    removed_count += 1
                    affected_users.add(user_id)
                    removal_details.append(f"• @{username or user_id}: {shortcode} ({current_views:,} views)")
                    
                    # Notify user with professional message
                    try:
                        await context.bot.send_message(
                            user_id,
                            f"Dear Creator,\n\n"
                            f"We regret to inform you that your submission https://www.instagram.com/reel/{shortcode}/ "
                            f"did not meet our qualification criteria of 10,000 views within 5 minutes "
                            f"(current views: {current_views:,})."
                        )
                    except Exception as e:
                        logger.error(f"Failed to notify user {user_id}: {e}")
            except Exception as e:
                logger.error(f"Error processing reel {shortcode}: {e}")
                continue
        
        # Recalculate total_views for affected users
        for user_id in affected_users:
            total_views = 0
            # Get all remaining reels for this user
            reels = await s.execute(text("SELECT shortcode FROM reels WHERE user_id = :u"), {"u": user_id})
            codes = [r[0] for r in reels.fetchall()]
            
            for code in codes:
                try:
                    reel_data = await get_reel_data(code)
                    total_views += reel_data['view_count']
                except Exception as e:
                    logger.error(f"Error getting views for reel {code}: {e}")
            
            # Update user's total views
            await s.execute(
                text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                {"v": total_views, "u": user_id}
            )
            
            # Log the update
            logger.info(f"Updated total views for user {user_id}: {total_views:,}")
        
        await s.commit()
        
        # Build detailed response message
        msg = [f"✅ Removed {removed_count} submissions with low views:"]
        if removal_details:
            msg.extend(removal_details)
        msg.append("\nUser view counts have been updated.")
        
        await update.message.reply_text("\n".join(msg))


@debug_handler
async def removeviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove views from a user's total"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 2:
        return await update.message.reply_text("Usage: /removeviews <user_id> <views>")
    
    try:
        user_id = int(context.args[0])
        views_to_remove = int(context.args[1])
        
        async with AsyncSessionLocal() as s:
            current = await s.execute(text("SELECT total_views FROM users WHERE user_id = :u"), {"u": user_id})
            row = current.fetchone()
            if not row:
                return await update.message.reply_text("❌ User not found in database")
            
            current_views = row[0] or 0
            new_total = max(0, current_views - views_to_remove)
            
            await s.execute(
                text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                {"v": new_total, "u": user_id}
            )
            await s.commit()

            # Try to get Telegram username
            try:
                chat = await context.bot.get_chat(user_id)
                user_name = chat.username or str(user_id)
            except:
                user_name = str(user_id)

            await update.message.reply_text(f"🗑️ Removed {views_to_remove:,} views from @{user_name}\nNew total: {new_total:,}")
    
    except ValueError:
        await update.message.reply_text("❌ Invalid input. Please use valid numbers.")

@debug_handler
async def addslot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add Instagram handles to a slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) < 2:
        return await update.message.reply_text(
            "Usage: /addslot <slot_number> <handle1> <handle2> ...\n"
            "Example: /addslot 1 johndoe janedoe\n\n"
            "This will replace all existing handles in the slot with the new ones."
        )
    
    try:
        slot_number = int(context.args[0])
        if slot_number not in [1, 2]:
            return await update.message.reply_text("❌ Slot number must be 1 or 2")
        
        handles = [h.lstrip('@') for h in context.args[1:]]
        
        async with AsyncSessionLocal() as s:
            # Get current handles in the slot
            current_handles = await s.execute(
                text("SELECT insta_handle FROM slot_accounts WHERE slot_number = :s"),
                {"s": slot_number}
            )
            current_handles = [h[0] for h in current_handles.fetchall()]
            
            # Remove existing handles for this slot
            await s.execute(
                text("DELETE FROM slot_accounts WHERE slot_number = :s"),
                {"s": slot_number}
            )
            
            # Add new handles
            for handle in handles:
                await s.execute(
                    text("""
                        INSERT INTO slot_accounts (slot_number, insta_handle)
                        VALUES (:s, :h)
                    """),
                    {"s": slot_number, "h": handle}
                )
            
            await s.commit()
            
            # Build response message
            msg = [
                f"✅ Updated slot {slot_number}:",
                "",
                "📤 <b>Previous Handles:</b>"
            ]
            
            if current_handles:
                msg.extend([f"• @{h}" for h in current_handles])
            else:
                msg.append("• No handles")
            
            msg.extend([
                "",
                "📥 <b>New Handles:</b>",
                *[f"• @{h}" for h in handles]
            ])
            
            await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)
            
    except ValueError:
        await update.message.reply_text("❌ Invalid slot number")

@debug_handler
async def removeslot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove handles from a specific slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) < 2:
        return await update.message.reply_text(
            "Usage: /removeslot <slot_number> <handle1> <handle2> ...\n"
            "Example: /removeslot 1 johndoe janedoe\n\n"
            "This will remove the specified handles from the slot."
        )
    
    try:
        slot_number = int(context.args[0])
        if slot_number not in [1, 2]:
            return await update.message.reply_text("❌ Slot number must be 1 or 2")
        
        handles = [h.lstrip('@') for h in context.args[1:]]
        
        async with AsyncSessionLocal() as s:
            # Get current handles in the slot
            current_handles = await s.execute(
                text("SELECT insta_handle FROM slot_accounts WHERE slot_number = :s"),
                {"s": slot_number}
            )
            current_handles = [h[0] for h in current_handles.fetchall()]
            
            # Remove specified handles
            removed = []
            not_found = []
            
            for handle in handles:
                # Check if handle exists in the slot
                if handle in current_handles:
                    await s.execute(
                        text("""
                            DELETE FROM slot_accounts 
                            WHERE slot_number = :s AND insta_handle = :h
                        """),
                        {"s": slot_number, "h": handle}
                    )
                    removed.append(handle)
                else:
                    not_found.append(handle)
            
            await s.commit()
            
            # Get remaining handles
            remaining = await s.execute(
                text("SELECT insta_handle FROM slot_accounts WHERE slot_number = :s"),
                {"s": slot_number}
            )
            remaining = [h[0] for h in remaining.fetchall()]
            
            # Build response message
            msg = [f"📊 <b>Slot {slot_number} Update</b>"]
            
            if removed:
                msg.extend([
                    "",
                    "✅ <b>Removed Handles:</b>",
                    *[f"• @{h}" for h in removed]
                ])
            
            if not_found:
                msg.extend([
                    "",
                    "⚠️ <b>Not Found in Slot:</b>",
                    *[f"• @{h}" for h in not_found]
                ])
            
            msg.extend([
                "",
                "📋 <b>Remaining Handles:</b>"
            ])
            
            if remaining:
                msg.extend([f"• @{h}" for h in remaining])
            else:
                msg.append("• No handles")
            
            await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)
            
    except ValueError:
        await update.message.reply_text("❌ Invalid slot number")

@debug_handler
async def slotstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current handles in each slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    async with AsyncSessionLocal() as s:
        # Get handles for both slots
        slot1_handles = await s.execute(
            text("""
                SELECT insta_handle 
                FROM slot_accounts 
                WHERE slot_number = 1 
                ORDER BY insta_handle
            """)
        )
        slot2_handles = await s.execute(
            text("""
                SELECT insta_handle 
                FROM slot_accounts 
                WHERE slot_number = 2 
                ORDER BY insta_handle
            """)
        )
        
        # Get submission counts for each slot
        slot1_count = await s.execute(
            text("""
                SELECT COUNT(*) 
                FROM slot_submissions 
                WHERE slot_number = 1 
                AND submitted_at > NOW() - INTERVAL '5 minutes'
            """)
        )
        slot2_count = await s.execute(
            text("""
                SELECT COUNT(*) 
                FROM slot_submissions 
                WHERE slot_number = 2 
                AND submitted_at > NOW() - INTERVAL '5 minutes'
            """)
        )
        
        # Debug: Get actual submissions
        slot1_subs = await s.execute(
            text("""
                SELECT shortcode, insta_handle, submitted_at 
                FROM slot_submissions 
                WHERE slot_number = 1 
                AND submitted_at > NOW() - INTERVAL '5 minutes'
            """)
        )
        slot2_subs = await s.execute(
            text("""
                SELECT shortcode, insta_handle, submitted_at 
                FROM slot_submissions 
                WHERE slot_number = 2 
                AND submitted_at > NOW() - INTERVAL '5 minutes'
            """)
        )
        
        # Log submissions for debugging
        logger.info(f"Slot 1 submissions: {slot1_subs.fetchall()}")
        logger.info(f"Slot 2 submissions: {slot2_subs.fetchall()}")
        
        msg = [
            "📊 <b>Slot Statistics</b>",
            "",
            "🎯 <b>Slot 1 (Lume)</b>",
            f"• Active Submissions: {slot1_count.scalar() or 0}",
            "• Handles:"
        ]
        
        # Add handles for slot 1
        handles = [h[0] for h in slot1_handles]
        if handles:
            msg += [f"  - @{h}" for h in handles]
        else:
            msg.append("  - No handles added")
        
        msg += [
            "",
            "🎯 <b>Slot 2 (Eagle)</b>",
            f"• Active Submissions: {slot2_count.scalar() or 0}",
            "• Handles:"
        ]
        
        # Add handles for slot 2
        handles = [h[0] for h in slot2_handles]
        if handles:
            msg += [f"  - @{h}" for h in handles]
        else:
            msg.append("  - No handles added")
        
        await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)


@debug_handler
async def lbpng(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate private campaign leaderboard image without username censor, using first name."""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")

    import io
    from PIL import Image, ImageDraw, ImageFont

    # Leaderboard layout
    cols = 5
    box_w, box_h = 240, 80
    pad_x, pad_y = 20, 18
    spacing_x, spacing_y = 20, 20
    margin_top = 160
    margin_side = 40

    # Load leaderboard data
    async with AsyncSessionLocal() as s:
        creators = (await s.execute(text("""
            SELECT u.user_id, u.username, u.total_views
            FROM users u
            WHERE u.total_views > 0
            ORDER BY u.total_views DESC
        """))).fetchall()

    if not creators:
        return await update.message.reply_text("No creator statistics available.")

    # Format views and fetch names
    def format_views(views):
        if views >= 1_000_000:
            return f"{views/1_000_000:.1f}M"
        elif views >= 1_000:
            return f"{views/1_000:.1f}K"
        return str(views)

    leaderboard = []
    for user_id, _, views in creators:
        try:
            chat = await context.bot.get_chat(user_id)
            name = chat.first_name or str(user_id)
        except:
            name = str(user_id)
        # Truncate name if too long
        name = name.strip()
        if len(name) > 9:
            name = name[:8] + "…"
        leaderboard.append({
            "name": name,
            "views": format_views(int(views))
        })

    rows = (len(leaderboard) + cols - 1) // cols

    # Canvas size
    img_width = margin_side * 2 + cols * box_w + (cols - 1) * spacing_x
    img_height = margin_top + rows * box_h + (rows - 1) * spacing_y + 60

    # Create image
    img = Image.new("RGBA", (img_width, img_height), (20, 20, 25, 255))  # dark grey background
    draw = ImageDraw.Draw(img)

    # Fonts
    try:
        font_title = ImageFont.truetype("DejaVuSans-Bold.ttf", 48)
        font_box = ImageFont.truetype("DejaVuSans-Bold.ttf", 28)
        font_views = ImageFont.truetype("DejaVuSans.ttf", 22)
    except:
        font_title = font_box = font_views = ImageFont.load_default()

    # Title
    title = "Leaderboard for\nprivate Campaign"
    bbox = draw.multiline_textbbox((0, 0), title, font=font_title)
    title_w = bbox[2] - bbox[0]
    draw.multiline_text(
        ((img_width - title_w) // 2, 30),
        title,
        font=font_title,
        fill=(255, 255, 255),
        align="center"
    )

    red = (220, 38, 54)
    white = (255, 255, 255)

    # Draw boxes
    for idx, entry in enumerate(leaderboard):
        row = idx // cols
        col = idx % cols
        x = margin_side + col * (box_w + spacing_x)
        y = margin_top + row * (box_h + spacing_y)
        draw.rounded_rectangle([x, y, x + box_w, y + box_h], radius=18, fill=red)
        draw.text((x + pad_x, y + 10), f"{idx+1}. {entry['name']}", font=font_box, fill=white)
        draw.text((x + pad_x, y + 44), f"{entry['views']} views", font=font_views, fill=white)

    # Export image
    img_byte_arr = io.BytesIO()
    img.convert("RGB").save(img_byte_arr, format='JPEG', quality=95)
    img_byte_arr.seek(0)

    await update.message.reply_photo(
        photo=img_byte_arr,
        caption="🏆 Private Campaign Leaderboard"
    )


# Add config table for minimum date
async def ensure_config_table():
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS config (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """))

# Helper to get/set min date
def parse_date(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%Y-%m-%d").date()

async def set_min_date(date_str):
    async with AsyncSessionLocal() as s:
        await s.execute(text("""
            INSERT INTO config (key, value) VALUES ('min_date', :v)
            ON CONFLICT (key) DO UPDATE SET value = :v
        """), {"v": date_str})
        await s.commit()

async def get_min_date():
    async with AsyncSessionLocal() as s:
        row = (await s.execute(text("SELECT value FROM config WHERE key = 'min_date'"))).fetchone()
        if row:
            return row[0]
        return None

@debug_handler
async def setmindate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    if not context.args:
        return await update.message.reply_text("Usage: /setmindate YYYY-MM-DD")
    date_str = context.args[0]
    try:
        parse_date(date_str)
    except Exception:
        return await update.message.reply_text("❌ Invalid date format. Use YYYY-MM-DD.")
    await set_min_date(date_str)
    await update.message.reply_text(f"✅ Minimum allowed video date set to {date_str}")

@debug_handler
async def getmindate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    min_date = await get_min_date()
    if min_date:
        await update.message.reply_text(f"Current minimum allowed video date: {min_date}")
    else:
        await update.message.reply_text("No minimum date set.")

# Ensure allowed_accounts has a unique constraint on (user_id, insta_handle)
async def ensure_allowed_accounts_unique():
    async with engine.begin() as conn:
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE table_name = 'allowed_accounts' AND constraint_type = 'UNIQUE' AND constraint_name = 'allowed_accounts_user_id_insta_handle_key'
                ) THEN
                    ALTER TABLE allowed_accounts ADD CONSTRAINT allowed_accounts_user_id_insta_handle_key UNIQUE (user_id, insta_handle);
                END IF;
            END$$;
        """))

# Ensure allowed_accounts has id SERIAL PRIMARY KEY and correct unique constraints
async def migrate_allowed_accounts():
    async with engine.begin() as conn:
        # Drop PK on user_id if exists
        await conn.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE table_name = 'allowed_accounts' AND constraint_type = 'PRIMARY KEY' AND constraint_name = 'allowed_accounts_pkey'
                ) THEN
                    ALTER TABLE allowed_accounts DROP CONSTRAINT allowed_accounts_pkey;
                END IF;
            END$$;
        """))
        # Add id SERIAL PRIMARY KEY if not exists
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'allowed_accounts' AND column_name = 'id'
                ) THEN
                    ALTER TABLE allowed_accounts ADD COLUMN id SERIAL PRIMARY KEY;
                END IF;
            END$$;
        """))
        # Add unique constraint on (user_id, insta_handle) if not exists
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE table_name = 'allowed_accounts' AND constraint_type = 'UNIQUE' AND constraint_name = 'allowed_accounts_user_id_insta_handle_key'
                ) THEN
                    ALTER TABLE allowed_accounts ADD CONSTRAINT allowed_accounts_user_id_insta_handle_key UNIQUE (user_id, insta_handle);
                END IF;
            END$$;
        """))

@debug_handler
async def referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to link a referred user to their referrer"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    if len(context.args) != 2:
        return await update.message.reply_text(
            "Usage: /referral <referred_user_id> <referrer_id>\n"
            "Example: /referral 123456789 987654321\n\n"
            "This command links a user to their referrer for commission tracking."
        )
    
    try:
        referred_id = int(context.args[0])  # The user being referred
        referrer_id = int(context.args[1])  # The user doing the referring
        
        if referred_id == referrer_id:
            return await update.message.reply_text("❌ A user cannot refer themselves")
        
        async with AsyncSessionLocal() as s:
            # Check if referred user exists
            referred_user = await s.execute(
                text("SELECT 1 FROM users WHERE user_id = :u"),
                {"u": referred_id}
            )
            if not referred_user.scalar():
                return await update.message.reply_text("❌ Referred user not found in database")
            
            # Check if referrer exists
            referrer = await s.execute(
                text("SELECT 1 FROM users WHERE user_id = :u"),
                {"u": referrer_id}
            )
            if not referrer.scalar():
                return await update.message.reply_text("❌ Referrer not found in database")
            
            # Check if referred user already has a referrer
            existing = await s.execute(
                text("SELECT 1 FROM referrals WHERE user_id = :u"),
                {"u": referred_id}
            )
            if existing.scalar():
                return await update.message.reply_text("❌ This user already has a referrer")
            
            # Get global commission rate
            commission = await s.execute(
                text("""
                    SELECT value
                    FROM config
                    WHERE key = 'referral_commission_rate'
                """)
            )
            commission_rate = float(commission.scalar() or 0)
            
            # Create referral relationship
            await s.execute(
                text("""
                    INSERT INTO referrals (user_id, referrer_id)
                    VALUES (:u, :r)
                """),
                {"u": referred_id, "r": referrer_id}
            )
            await s.commit()
            
            # Get usernames for confirmation message
            try:
                referred_chat = await context.bot.get_chat(referred_id)
                referrer_chat = await context.bot.get_chat(referrer_id)
                referred_name = referred_chat.username or str(referred_id)
                referrer_name = referrer_chat.username or str(referrer_id)
            except:
                referred_name = str(referred_id)
                referrer_name = str(referrer_id)
            
            # Build confirmation message
            msg = [
                f"✅ Referral relationship created:",
                f"• Referred User: @{referred_name}",
                f"• Referrer: @{referrer_name}",
                f"• Global Commission Rate: {commission_rate:.2f}%"
            ]
            
            await update.message.reply_text("\n".join(msg))
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user IDs")

@debug_handler
async def referralstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View referral statistics for all users"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    async with AsyncSessionLocal() as s:
        # Get all users with their referral data
        users = await s.execute(
            text("""
                SELECT DISTINCT u.user_id, u.username, u.total_views
                FROM users u
                LEFT JOIN referrals r1 ON u.user_id = r1.user_id
                LEFT JOIN referrals r2 ON u.user_id = r2.referrer_id
                WHERE r1.user_id IS NOT NULL OR r2.referrer_id IS NOT NULL
                ORDER BY u.total_views DESC
            """)
        )
        users_data = users.fetchall()
        
        if not users_data:
            return await update.message.reply_text("No referral data available.")
        
        # Get global commission rate
        commission = await s.execute(
            text("""
                SELECT value
                FROM config
                WHERE key = 'referral_commission_rate'
            """)
        )
        commission_rate = float(commission.scalar() or 0)
        
        # Process each user
        for user_id, username, total_views in users_data:
            try:
                # Get user's referrer
                referrer = await s.execute(
                    text("""
                        SELECT r.referrer_id, u.username
                        FROM referrals r
                        JOIN users u ON r.referrer_id = u.user_id
                        WHERE r.user_id = :u
                    """),
                    {"u": user_id}
                )
                referrer_data = referrer.fetchone()
                
                # Get users referred by this user
                referred = await s.execute(
                    text("""
                        SELECT r.user_id, u.username, u.total_views
                        FROM referrals r
                        JOIN users u ON r.user_id = u.user_id
                        WHERE r.referrer_id = :u
                    """),
                    {"u": user_id}
                )
                referred_users = referred.fetchall()
                
                # Get user info
                try:
                    chat = await context.bot.get_chat(user_id)
                    user_name = chat.username or str(user_id)
                except:
                    user_name = str(user_id)
                
                # Build message
                msg = [f"📊 <b>Referral Stats for @{user_name}</b>"]
                
                # Add commission rate
                msg.append(f"\n💰 <b>Commission Rate:</b> {commission_rate:.2f}%")
                
                # Add referrer info
                if referrer_data:
                    referrer_id, referrer_username = referrer_data
                    try:
                        referrer_chat = await context.bot.get_chat(referrer_id)
                        referrer_name = referrer_chat.username or str(referrer_id)
                    except:
                        referrer_name = str(referrer_id)
                    msg.append(f"\n👤 <b>Referred by:</b> @{referrer_name}")
                else:
                    msg.append("\n👤 <b>Referred by:</b> No referrer")
                
                # Add referred users info
                if referred_users:
                    msg.append("\n👥 <b>Referred Users:</b>")
                    total_referral_views = 0
                    for ref_id, ref_username, views in referred_users:
                        try:
                            ref_chat = await context.bot.get_chat(ref_id)
                            ref_name = ref_chat.username or str(ref_id)
                        except:
                            ref_name = str(ref_id)
                        views = views or 0
                        total_referral_views += views
                        msg.append(f"• @{ref_name}: {views:,} views")
                    msg.append(f"\n📈 <b>Total Views from Referrals:</b> {total_referral_views:,}")
                    
                    # Calculate potential commission
                    commission_amount = (total_referral_views / 1000) * 0.025 * (commission_rate / 100)
                    msg.append(f"💰 <b>Potential Commission:</b> ${commission_amount:,.2f}")
                else:
                    msg.append("\n👥 <b>Referred Users:</b> None")
                
                msg.append("\n━━━━━━━━━━━━━━━━━━━━")
                
                await context.bot.send_message(
                    update.effective_chat.id,
                    "\n".join(msg),
                    parse_mode=ParseMode.HTML
                )
                await asyncio.sleep(0.5)  # Small delay between messages
                
            except Exception as e:
                logger.error(f"Error processing user {user_id}: {e}")
                continue

@debug_handler
async def setcommission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set global commission rate for all referrers"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /setcommission <rate>\n"
            "Example: /setcommission 5.00\n"
            "Rate should be a percentage (e.g., 5.00 for 5%)"
        )
    
    try:
        rate = float(context.args[0])
        
        if rate < 0 or rate > 100:
            return await update.message.reply_text("❌ Commission rate must be between 0 and 100")
        
        async with AsyncSessionLocal() as s:
            # Update global commission rate
            await s.execute(
                text("""
                    UPDATE config 
                    SET value = :r 
                    WHERE key = 'referral_commission_rate'
                """),
                {"r": str(rate)}
            )
            await s.commit()
            
            await update.message.reply_text(
                f"✅ Global commission rate set to {rate:.2f}%\n"
                f"This rate will apply to all referrers."
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid rate")

@debug_handler
async def getcommission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View current global commission rate"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 This command is for admins only")
    
    async with AsyncSessionLocal() as s:
        # Get global commission rate
        rate = await s.execute(
            text("""
                SELECT value
                FROM config
                WHERE key = 'referral_commission_rate'
            """)
        )
        rate_data = rate.fetchone()
        
        if rate_data:
            rate_value = rate_data[0]
            await update.message.reply_text(
                f"📊 Global Commission Rate:\n"
                f"• Rate: {float(rate_value):.2f}%"
            )
        else:
            await update.message.reply_text("ℹ️ No commission rate set")

@debug_handler
async def remove_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove USDT ERC20 address"""
    user_id = update.effective_user.id
    
    async with AsyncSessionLocal() as s:
        # Check if payment details exist
        existing = await s.execute(
            text("SELECT usdt_address FROM payment_details WHERE user_id = :u"),
            {"u": user_id}
        ).fetchone()
        
        if not existing or not existing[0]:
            return await update.message.reply_text("❌ No USDT address found to remove.")
        
        # Remove USDT address
        await s.execute(
            text("UPDATE payment_details SET usdt_address = NULL WHERE user_id = :u"),
            {"u": user_id}
        )
        await s.commit()
        
        await update.message.reply_text("✅ USDT address has been removed.")

@debug_handler
async def remove_paypal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove PayPal email"""
    user_id = update.effective_user.id
    
    async with AsyncSessionLocal() as s:
        # Check if payment details exist
        existing = await s.execute(
            text("SELECT paypal_email FROM payment_details WHERE user_id = :u"),
            {"u": user_id}
        ).fetchone()
        
        if not existing or not existing[0]:
            return await update.message.reply_text("❌ No PayPal email found to remove.")
        
        # Remove PayPal email
        await s.execute(
            text("UPDATE payment_details SET paypal_email = NULL WHERE user_id = :u"),
            {"u": user_id}
        )
        await s.commit()
        
        await update.message.reply_text("✅ PayPal email has been removed.")

@debug_handler
async def remove_upi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove UPI address"""
    user_id = update.effective_user.id
    
    async with AsyncSessionLocal() as s:
        # Check if payment details exist
        existing = await s.execute(
            text("SELECT upi_address FROM payment_details WHERE user_id = :u"),
            {"u": user_id}
        ).fetchone()
        
        if not existing or not existing[0]:
            return await update.message.reply_text("❌ No UPI address found to remove.")
        
        # Remove UPI address
        await s.execute(
            text("UPDATE payment_details SET upi_address = NULL WHERE user_id = :u"),
            {"u": user_id}
        )
        await s.commit()
        
        await update.message.reply_text("✅ UPI address has been removed.")


#Invoice
def format_views(n):
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    elif n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)

# Calculate 12% tax
def calculate_tax(gross):
    return round(gross * 0.12, 2)

# Telegram bot command: /invoice <user_id>
async def invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /invoice <user_id>")

    try:
        user_id = int(context.args[0])
    except ValueError:
        return await update.message.reply_text("❌ Invalid user ID.")

    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT username, total_views FROM users WHERE user_id = :u"), {"u": user_id})
        row = result.first()
        if not row:
            return await update.message.reply_text("❌ User not found.")
        
        username, total_views = row
        gross_payout = round((total_views / 1000) * 0.025, 2)
        tax = calculate_tax(gross_payout)
        net_payout = round(gross_payout - tax, 2)
        invoice_id = f"INV-{user_id}-{datetime.now().strftime('%m%y')}"

    # Load the invoice template
    bg = Image.open("invoice_template.jpg").convert("RGB")
    draw = ImageDraw.Draw(bg)

    # Load fonts (adjust paths if needed)
    bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
    regular = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
    small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)

    # Fill the template
    draw.text((1070, 313), invoice_id, font=small, fill="#000")                    # Invoice ID
    draw.text((160, 520), f"@{username}", font=regular, fill="#000")              # Username
    draw.text((915, 835), format_views(total_views), font=regular, fill="#000")   # Views
    draw.text((900, 1315), f"${gross_payout:,.2f}", font=bold, fill="#000")        # Subtotal
    draw.text((900, 1380), f"${tax:,.2f}", font=regular, fill="#000")              # Tax (12%)
    draw.text((880, 1480), f"${net_payout:,.2f}", font=bold, fill="#FFF")         # Total

    # Save image to buffer
    buffer = BytesIO()
    bg.save(buffer, format="JPEG")
    buffer.seek(0)

    # Send back to user
    await update.message.reply_photo(photo=buffer, caption=f"🧾 Invoice for @{username}")
    
#Profile
def format_millions(n):
    return f"{n/1_000_000:.1f}M" if n >= 1_000_000 else f"{int(n/1_000)}K"

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT username, total_views FROM users WHERE user_id = :u"), {"u": user_id})
        row = result.first()
        if not row:
            return await update.message.reply_text("❌ User not found.")
        
        username, total_views = row
        total_reels = (await s.execute(text("SELECT COUNT(*) FROM reels WHERE user_id = :u"), {"u": user_id})).scalar()
        payout = round((total_views / 1000) * 0.025, 2)


    # Load 1024x1024 background template
    bg = Image.open("template_profile_card.png").convert("RGB")
    draw = ImageDraw.Draw(bg)

    # Fonts
    bold_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 44)
    small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 30)

    # Load and process profile photo
    try:
        photos = await context.bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file = await context.bot.get_file(photos.photos[0][0].file_id)
            img_data = requests.get(file.file_path).content
            pfp = Image.open(BytesIO(img_data)).resize((250, 250)).convert("RGB")
        else:
            pfp = Image.new("RGB", (250, 250), "#ccc")
    except:
        pfp = Image.new("RGB", (250, 250), "#ccc")

    # Apply circular mask and paste at exact center of grey circle
    mask = Image.new("L", (250, 250), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, 250, 250), fill=255)
    pfp_x, pfp_y = 388, 280  # Centered at (408, 512)
    bg.paste(pfp, (pfp_x, pfp_y), mask)

    # ✅ Username - centered below PFP
    uname_y = 580
    uname_x = (1024 - draw.textlength(username, font=bold_font)) // 2
    draw.text((uname_x, uname_y), username, font=bold_font, fill="#222")
    
    #quote badge
    badge_texts = [
        "Top Creator", "Content Emperor", "Rising Star", "Content King",
        "View Magnet", "Reel Master", "Reel Master", "Aura Farmer",
        "Engage More", "Watch Me Grow", "Trendsetter", "Viral Genius",
        "Storyteller", "Next Big Thing", "Content Wizard", "Social Butterfly",
        "Power Poster", "Daily Hustler", "Creative Beast", "Fan Favorite",
        "Mastermind", "Audience Magnet", "Game Changer", "Hit Maker",
        "Visionary", "Bold & Brave",
    ]

    # Pick 2 unique badges randomly
    text1, text2 = random.sample(badge_texts, 2)
    separator = " | "
    full_text = text1 + separator + text2
    total_width = draw.textlength(full_text, font=small_font)
    start_x = (1024 - total_width) // 2
    badge_y = uname_y + 60  # position a bit below username

    # Draw first badge
    draw.text((start_x, badge_y), text1, font=small_font, fill="#222")

    # Draw separator
    sep_x = start_x + draw.textlength(text1, font=small_font)
    draw.text((sep_x, badge_y), separator, font=small_font, fill="#555")

    # Draw second badge
    text2_x = sep_x + draw.textlength(separator, font=small_font)
    draw.text((text2_x, badge_y), text2, font=small_font, fill="#222")


    # ✅ Stats (keep as-is, just move down slightly)
    stats = [
        (format_millions(total_views), "VIEWS"),
        (str(total_reels), "REELS"),
        (f"${payout:,.2f}", "PAYOUT")
    ]
    stat_xs = [160, 430, 700]
    stat_y = 760  # previously ~700

    for i, (val, label) in enumerate(stats):
        draw.text((stat_xs[i], stat_y), val, font=bold_font, fill="#111")
        draw.text((stat_xs[i], stat_y + 40), label, font=small_font, fill="#666")

    # Send image
    buffer = BytesIO()
    bg.save(buffer, format="PNG")
    buffer.seek(0)
    await update.message.reply_photo(photo=buffer, caption="📇 Your Creator Profile Card")

    
@debug_handler
async def slotdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show submissions for a specific slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /slotdata <slot_number>\n"
            "Example: /slotdata 1\n"
            "This will show all current submissions in the specified slot."
        )
    
    try:
        slot_number = int(context.args[0])
        if slot_number not in [1, 2]:
            return await update.message.reply_text("❌ Slot number must be 1 or 2")
        
        async with AsyncSessionLocal() as s:
            # Get all submissions for this slot
            submissions = await s.execute(
                text("""
                    SELECT shortcode, insta_handle, view_count, submitted_at
                    FROM slot_submissions
                    WHERE slot_number = :s
                    ORDER BY submitted_at DESC
                """),
                {"s": slot_number}
            )
            submissions_data = submissions.fetchall()
            
            if not submissions_data:
                return await update.message.reply_text(f"ℹ️ No submissions found in slot {slot_number}")
            
            # Build message with submission details
            msg = [f"📊 <b>Slot {slot_number} Submissions</b>"]
            
            total_views = 0
            total_videos = len(submissions_data)
            
            msg.append(f"\n📈 <b>Total Videos:</b> {total_videos}")
            
            for shortcode, handle, views, submitted_at in submissions_data:
                views = views or 0
                total_views += views
                msg.append(
                    f"\n• @{handle}\n"
                    f"  - Link: https://www.instagram.com/reel/{shortcode}/\n"
                    f"  - Views: {views:,}\n"
                    f"  - Submitted: {submitted_at.strftime('%Y-%m-%d %H:%M:%S')}"
                )
            
            msg.append(f"\n📈 <b>Total Views:</b> {total_views:,}")
            
            # Send the message
            await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)
            
    except ValueError:
        await update.message.reply_text("❌ Invalid slot number")

# End cycle 
async def generate_invoice_image(user_id, username, total_views):
    gross_payout = round((total_views / 1000) * 0.025, 2)
    tax = round(gross_payout * 0.12, 2)
    net_payout = round(gross_payout - tax, 2)
    invoice_id = f"INV-{user_id}-{datetime.now().strftime('%m%y')}"
    bg = Image.open("invoice_template.jpg").convert("RGB")
    draw = ImageDraw.Draw(bg)
    bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
    regular = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
    small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    draw.text((1070, 313), invoice_id, font=small, fill="#000")
    draw.text((160, 520), f"@{username}", font=regular, fill="#000")
    draw.text((915, 835), format_views(total_views), font=regular, fill="#000")
    draw.text((900, 1315), f"${gross_payout:,.2f}", font=bold, fill="#000")
    draw.text((900, 1380), f"${tax:,.2f}", font=regular, fill="#000")
    draw.text((880, 1480), f"${net_payout:,.2f}", font=bold, fill="#FFF")
    buffer = BytesIO()
    bg.save(buffer, format="JPEG")
    buffer.seek(0)
    return buffer

# helper to reuse your profile card builder
async def generate_profile_image(user_id, username, total_views, total_reels, payout, context):
    bg = Image.open("template_profile_card.png").convert("RGB")
    draw = ImageDraw.Draw(bg)
    bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 44)
    small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 30)
    try:
        photos = await context.bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file = await context.bot.get_file(photos.photos[0][0].file_id)
            img_data = requests.get(file.file_path).content
            pfp = Image.open(BytesIO(img_data)).resize((250, 250)).convert("RGB")
        else:
            pfp = Image.new("RGB", (250, 250), "#ccc")
    except:
        pfp = Image.new("RGB", (250, 250), "#ccc")
    mask = Image.new("L", (250, 250), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, 250, 250), fill=255)
    bg.paste(pfp, (388, 280), mask)
    uname_y = 580
    uname_x = (1024 - draw.textlength(username, font=bold)) // 2
    draw.text((uname_x, uname_y), username, font=bold, fill="#222")

    badges = random.sample([
        "Top Creator", "Content Emperor", "Rising Star", "Content King", "View Magnet", "Reel Master",
        "Aura Farmer", "Engage More", "Watch Me Grow", "Trendsetter", "Viral Genius", "Storyteller",
        "Next Big Thing", "Content Wizard", "Social Butterfly", "Power Poster", "Daily Hustler",
        "Creative Beast", "Fan Favorite", "Mastermind", "Audience Magnet", "Game Changer", "Hit Maker",
        "Visionary", "Bold & Brave"
    ], 2)
    full = badges[0] + " | " + badges[1]
    x = (1024 - draw.textlength(full, font=small)) // 2
    draw.text((x, uname_y + 60), full, font=small, fill="#222")

    # stats
    stats = [(format_millions(total_views), "VIEWS"),
             (str(total_reels), "REELS"),
             (f"${payout:,.2f}", "PAYOUT")]
    for i, (val, label) in enumerate(stats):
        x = [160, 430, 700][i]
        draw.text((x, 760), val, font=bold, fill="#111")
        draw.text((x, 800), label, font=small, fill="#666")

    buffer = BytesIO()
    bg.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer

# MAIN command
async def endcycle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("❌ Unauthorized.")

    await update.message.reply_text("🚀 Sending invoices and profiles...")

    sent = 0
    skipped = 0

    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT user_id, username, total_views FROM users"))
        users = result.fetchall()

        for user_id, username, total_views in users:
            try:
                if total_views >= 4_000_000:
                    total_reels = (await s.execute(
                        text("SELECT COUNT(*) FROM reels WHERE user_id = :u"), {"u": user_id}
                    )).scalar()
                    payout = round((total_views / 1000) * 0.025, 2)

                    invoice_img = await generate_invoice_image(user_id, username, total_views)
                    profile_img = await generate_profile_image(user_id, username, total_views, total_reels, payout, context)

                    await context.bot.send_photo(user_id, photo=invoice_img, caption="🧾 Your monthly invoice")
                    await context.bot.send_photo(user_id, photo=profile_img, caption="📇 Your profile card")
                    sent += 1
                else:
                    await context.bot.send_message(
                        user_id,
                        "⚠️ Sorry, you didn't meet the required views criteria (4M) this cycle. Better Try Next Time!"
                    )
                    skipped += 1
            except Exception as e:
                print(f"Failed to send to {user_id}: {e}")
                continue

    await update.message.reply_text(f"✅ End cycle complete:\n📤 Sent: {sent}\n⏭️ Skipped: {skipped}")
    
@debug_handler
async def clearslot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear submissions for a specific slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 1:
        return await update.message.reply_text(
            "Usage: /clearslot <slot_number>\n"
            "Example: /clearslot 1\n"
            "This will clear all submissions in the specified slot."
        )
    
    try:
        slot_number = int(context.args[0])
        if slot_number not in [1, 2]:
            return await update.message.reply_text("❌ Slot number must be 1 or 2")
        
        async with AsyncSessionLocal() as s:
            # Get count of submissions before clearing
            count = await s.execute(
                text("SELECT COUNT(*) FROM slot_submissions WHERE slot_number = :s"),
                {"s": slot_number}
            )
            submission_count = count.scalar() or 0
            
            # Clear the slot submissions
            await s.execute(
                text("DELETE FROM slot_submissions WHERE slot_number = :s"),
                {"s": slot_number}
            )
            await s.commit()
            
            # Send confirmation
            await update.message.reply_text(
                f"✅ Slot {slot_number} has been cleared.\n"
                f"Removed {submission_count} submissions.\n"
                f"The slot is now ready for new submissions."
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid slot number")

async def run_bot():
    await init_db()
    await ensure_config_table()
    await migrate_allowed_accounts()
    asyncio.create_task(start_health_check_server())
    
    app = ApplicationBuilder().token(TOKEN).build()
    
    handlers = [
        ("start", start_cmd), 
        ("addaccount", addaccount), 
        ("banuser", banuser), 
        ("unban", unban),
        ("removeallaccs", removeallaccs), 
        ("removeacc", removeacc), 
        ("addviews", addviews_custom),
        ("submit", submit), 
        ("remove", remove), 
        ("cleardata", cleardata), 
        ("currentstats", currentstats), 
        ("creatorstats", creatorstats),
        ("broadcast", broadcast), 
        ("export", export), 
        ("invoice", invoice),
        ("addusdt", add_usdt), 
        ("userinfo", userinfo),
        ("currentaccounts", currentaccounts),
        ("addpaypal", add_paypal), 
        ("addupi", add_upi), 
        ("review", review), 
        ("userstats", userstats),
        ("forceupdate", forceupdate), 
        ("addadmin", addadmin), 
        ("removeadmin", removeadmin),
        ("clearbad", clearbad), 
        ("addslot", addslot), 
        ("removeslot", removeslot),
        ("slotstats", slotstats), 
        ("slotdata", slotdata), 
        ("clearslot", clearslot), 
        ("lbpng", lbpng),
        ("endcycle", endcycle),
        ("setmindate", setmindate), 
        ("getmindate", getmindate),
        ("referral", referral), 
        ("profile", profile),
        ("removeviews", removeviews),
        ("referralstats", referralstats),
        ("setcommission", setcommission), 
        ("getcommission", getcommission),
        ("removeusdt", remove_usdt),
        ("removepaypal", remove_paypal),
        ("removeupi", remove_upi),
        ("taskstatus", taskstatus),
        ("debugadmins", debug_admins),
    ]
    for cmd, h in handlers:
        app.add_handler(CommandHandler(cmd, h))
    
    # Add callback query handler for review buttons
    app.add_handler(CallbackQueryHandler(handle_userstats_page, pattern=r"^userstats_\d+$"))
    app.add_handler(CallbackQueryHandler(handle_creatorstats_page, pattern=r"^creatorstats_\d+$"))
    app.add_handler(CallbackQueryHandler(handle_currentaccounts_page, pattern=r"^currentaccounts_\d+$"))
    app.add_handler(CallbackQueryHandler(handle_review_callback, pattern=r"^(approve|reject)_\d+$"))
    app.add_handler(CallbackQueryHandler(handle_userinfo_page, pattern=r"^userinfo_\d+_\d+$"))

    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        await asyncio.Event().wait()
    finally:
        await app.stop()
        await app.shutdown()

@debug_handler
async def taskstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check Apify task system status (Admin only)"""
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ You don't have permission to use this command.")
        return
    
    try:
        apify_client = get_apify_client()
        status = await apify_client.get_task_status()
        
        status_text = f"📋 **Apify Task System Status**\n\n"
        status_text += f"🔄 Active tasks: {status['active_tasks']}\n"
        status_text += f"⏳ Queued tasks: {status['queued_tasks']}\n"
        
        if status['tasks']:
            status_text += f"\n**Active Task IDs:**\n"
            for task_id in status['tasks'][:5]:  # Show first 5
                status_text += f"• `{task_id}`\n"
            
            if len(status['tasks']) > 5:
                status_text += f"... and {len(status['tasks']) - 5} more\n"
        
        await update.message.reply_text(status_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in taskstatus: {str(e)}")
        await update.message.reply_text(f"❌ Error getting task status: {str(e)}")

@debug_handler
async def debug_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug admin configuration (Admin only)"""
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ You don't have permission to use this command.")
        return
    
    try:
        async with AsyncSessionLocal() as s:
            # Get admins from database
            db_admins = await s.execute(
                text("SELECT user_id FROM admins")
            )
            db_admin_ids = [row[0] for row in db_admins.fetchall()]
            
            # Get admin info
            admin_info = []
            for admin_id in ADMIN_IDS:
                try:
                    chat = await context.bot.get_chat(admin_id)
                    admin_info.append(f"• {chat.full_name} (@{chat.username}) - ID: {admin_id}")
                except Exception as e:
                    admin_info.append(f"• Unknown User - ID: {admin_id} (Error: {e})")
            
            # Build message
            msg = [
                "🔧 <b>Admin Configuration Debug</b>",
                "",
                "📋 <b>Environment Variables:</b>",
                f"• ADMIN_IDS from .env: {ADMIN_IDS}",
                f"• Total admin IDs: {len(ADMIN_IDS)}",
                "",
                "🗄️ <b>Database Admins:</b>",
                f"• Total in database: {len(db_admin_ids)}",
                f"• Database admin IDs: {db_admin_ids}",
                "",
                "👥 <b>Admin Details:</b>"
            ]
            msg.extend(admin_info)
            
            # Check for mismatches
            env_only = set(ADMIN_IDS) - set(db_admin_ids)
            db_only = set(db_admin_ids) - set(ADMIN_IDS)
            
            if env_only:
                msg.append(f"\n⚠️ <b>Only in .env:</b> {list(env_only)}")
            if db_only:
                msg.append(f"\n⚠️ <b>Only in database:</b> {list(db_only)}")
            
            if not env_only and not db_only:
                msg.append("\n✅ <b>Admin configuration is consistent</b>")
            
            await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"Error in debug_admins: {str(e)}")
        await update.message.reply_text(f"❌ Error getting admin info: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_bot())
