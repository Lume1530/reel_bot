import os
import re
import asyncio
import logging
import requests
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
import io
import time

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

from fastapi import FastAPI
import uvicorn

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, BigInteger, text

# ─── Load config ─────────────────────────────────────────────────────────────
load_dotenv()
TOKEN        = os.getenv("TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS    = set(map(int, os.getenv("ADMIN_ID", "").split(",")))
PORT         = int(os.getenv("PORT", 8000))
LOG_GROUP_ID = int(os.getenv("LOG_GROUP_ID", 0))
ENSEMBLE_TOKEN = os.getenv("ENSEMBLE_TOKEN")

if not all([TOKEN, DATABASE_URL, ENSEMBLE_TOKEN]):
    print("❌ TOKEN, DATABASE_URL, and ENSEMBLE_TOKEN must be set in .env")
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

        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS total_views BIGINT DEFAULT 0"
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
    id        = Column(Integer, primary_key=True)
    user_id   = Column(BigInteger, nullable=False)
    shortcode = Column(String, nullable=False)

class User(Base):
    __tablename__ = "users"
    user_id     = Column(BigInteger, primary_key=True)
    username    = Column(String, nullable=True)
    registered  = Column(Integer, default=0)
    total_views = Column(BigInteger, default=0)

# ─── Utilities ─────────────────────────────────────────────────────────────────
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
    """Get reel data from EnsembleData API"""
    try:
        # Clean up the URL/input
        shortcode = shortcode.strip()
        
        # Extract shortcode from URL if full URL is provided
        if 'instagram.com' in shortcode:
            # Try different URL patterns
            patterns = [
                r'instagram\.com/(?:([^/]+)/)?reel/([^/?#&]+)',  # Standard format
                r'reel/([^/?#&]+)',  # Just the reel part
                r'([A-Za-z0-9_-]+)$'  # Just the shortcode
            ]
            
            for pattern in patterns:
                match = re.search(pattern, shortcode)
                if match:
                    shortcode = match.group(1) if len(match.groups()) == 1 else match.group(2)
                    break
            else:
                raise Exception("Could not extract shortcode from URL")
        else:
            # If it's not a URL, assume it's just the shortcode
            shortcode = shortcode.strip('/')

        # Get reel data using post details endpoint
        api_url = 'https://ensembledata.com/apis/instagram/post/details'
        params = {
            'code': shortcode,
            'n_comments_to_fetch': 0,
            'token': ENSEMBLE_TOKEN
        }
        
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        if not data.get('data'):
            raise Exception("No reel data found")
            
        reel_data = data['data']
        
        # Log the raw data for debugging
        logger.info(f"Raw reel data: {reel_data}")
        
        # Get view count based on media type
        view_count = 0
        if reel_data.get('is_video', False):
            # For videos, get view count from video_play_count
            view_count = reel_data.get('video_play_count', 0)
            logger.info(f"Video play count: {view_count}")
        else:
            # For images, get view count from edge_media_preview_like
            view_count = reel_data.get('edge_media_preview_like', {}).get('count', 0)
            logger.info(f"Image like count: {view_count}")
        
        # Log the extracted counts
        logger.info(f"Extracted view_count: {view_count}")
        
        return {
    'owner_username': reel_data.get('owner', {}).get('username', ''),
    'view_count': view_count,
    'play_count': view_count,
    'taken_at_timestamp': reel_data.get('taken_at_timestamp') or reel_data.get('taken_at') or reel_data.get('timestamp')
}

                
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 422:
            raise Exception("Invalid reel URL or shortcode")
        raise Exception(f"API Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in get_reel_data: {str(e)}")
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
    cmds = [
        "👋 <b>Welcome to Reel Tracker Bot!</b>",
        "",
        "📋 <b>User Commands:</b>",
        "• <code>/addaccount &lt;@handle&gt;</code> - Request to link your Instagram account (max 15 accounts, 5 pending)",
        "• <code>/submit &lt;link&gt;</code> - Submit your reel to track",
        "• <code>/remove &lt;shortcode or URL&gt;</code> - Remove an added reel",
        "• <code>/creatorstats</code> - Show your statistics and payment info",
        "",
        "💰 <b>Payment Commands:</b>",
        "• <code>/addusdt &lt;address&gt;</code> - Add your USDT ERC20 address",
        "• <code>/addpaypal &lt;email&gt;</code> - Add your PayPal email",
        "• <code>/addupi &lt;address&gt;</code> - Add your UPI address",
    ]
    
    if await is_admin(update.effective_user.id):
        cmds += [
            "",
            "👑 <b>Admin Commands:</b>",
            "• <code>/banuser &lt;user_id&gt;</code> - Ban a user and delete all data",
            "• <code>/unban &lt;user_id&gt;</code> - Unban a user",
            "• <code>/addviews &lt;user_id&gt; &lt;views&gt;</code> - Add views manually",
            "• <code>/currentaccounts</code> - List all IG accounts",
            "• <code>/userinfo &lt;user_id&gt;</code> - Lists stats for all creators with payment info",
            "• <code>/review &lt;user_id&gt;</code> - Review account link requests",
            "• <code>/removeallaccs &lt;user_id&gt;</code> - Remove all accounts for a user",
            "• <code>/removeacc &lt;user_id&gt; &lt;@handle&gt;</code> - Remove specific account",
            "• <code>/userinfo</code> - View Users Reels",
            "• <code>/currentstats</code> - Show total views and working accounts",
            "• <code>/broadcast &lt;message&gt;</code> - Send message to all users",
            "• <code>/cleardata</code> - Clear all reel links",
            "• <code>/export</code> - Export stats.txt for all users",
            "• <code>/forceupdate</code> - Force update all reel views",
            "• <code>/clearbad</code> - Remove submissions with less than 10k views after 7 days",
            "• <code>/setmindate &lt;YYYY-MM-DD&gt;</code> - Set minimum allowed video upload date",
            "• <code>/getmindate</code> - Show current minimum allowed video date",
            "",
            "🎯 <b>Slot Management:</b>",
            "• <code>/addslot &lt;1|2&gt; &lt;handle1&gt; &lt;handle2&gt; ...</code> - Add handles to slot",
            "• <code>/removeslot &lt;1|2&gt; &lt;handle&gt;</code> - Remove handle from slot",
            "• <code>/slotstats</code> - Show current handles and submissions in slots",
            "• <code>/slotdata &lt;1|2&gt;</code> - Show and clear slot submissions",
            "• <code>/addadmin &lt;user_id&gt;</code> - Add new admin",
            "• <code>/removeadmin &lt;user_id&gt;</code> - Remove admin",
            "",
            "👥 <b>Referral Management:</b>",
            "• <code>/referral &lt;user_id&gt; &lt;referrer_id&gt;</code> - Assign referral relationship",
            "• <code>/referralstats</code> - View referral statistics for all users",
            "• <code>/setcommission &lt;rate&gt;</code> - Set global commission rate",
            "• <code>/getcommission</code> - View current commission rate",
        ]
    await update.message.reply_text("\n".join(cmds), parse_mode=ParseMode.HTML)

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
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"🔔 New account link request:\n"
                    f"• User: {update.effective_user.full_name} (@{update.effective_user.username})\n"
                    f"• Instagram: @{handle}\n"
                    f"• User ID: {user_id}\n"
                    f"• Current accounts: {account_count}/15\n"
                    f"• Pending requests: {pending_count + 1}/5\n\n"
                    f"Use <code>/review {user_id}</code> to approve/reject",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
        
        await update.message.reply_text(
            f"✅ Your request to link @{handle} has been submitted.\n"
            f"Current accounts: {account_count}/15\n"
            f"Pending requests: {pending_count + 1}/5\n"
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
    """Remove all Instagram accounts for a user"""
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
            count = (await s.execute(
                text("SELECT COUNT(*) FROM allowed_accounts WHERE user_id = :u"),
                {"u": user_id}
            )).scalar() or 0
            
            if count == 0:
                return await update.message.reply_text("❌ User has no linked accounts")
            
            # Delete all accounts
            await s.execute(
                text("DELETE FROM allowed_accounts WHERE user_id = :u"),
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
                f"✅ Removed all {count} accounts for @{user_name}"
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")

@debug_handler
async def removeacc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a specific Instagram account for a user"""
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
            await s.commit()
            
            # Get username for confirmation
            try:
                chat = await context.bot.get_chat(user_id)
                user_name = chat.username or str(user_id)
            except:
                user_name = str(user_id)
            
            await update.message.reply_text(
                f"✅ Removed @{handle} from @{user_name}"
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
    import time
    from datetime import datetime
    
    user_id = update.effective_user.id
    now = time.time()
    
    # Check if force update is running
    if force_update_lock.locked():
        return await update.message.reply_text(
            "⏳ Please wait a moment. The system is currently updating view counts.\n"
            "Try submitting again in a few seconds."
        )
    
    # Cooldown check
    last_time = submit_cooldowns.get(user_id, 0)
    if now - last_time < 30:
        wait = int(30 - (now - last_time))
        return await update.message.reply_text(f"⏳ Please wait {wait} seconds before submitting again.")
    submit_cooldowns[user_id] = now

    if not context.args:
        return await update.message.reply_text("❗ Provide up to 5 reel links, separated by commas.")
    # Join all args and split by comma
    raw = " ".join(context.args)
    links = [l.strip() for l in raw.split(",") if l.strip()]
    if not links or len(links) > 5:
        return await update.message.reply_text("❗ You can submit up to 5 links at once, separated by commas.")

    uid = user_id
    results = []
    min_date_str = await get_min_date()
    min_date = parse_date(min_date_str) if min_date_str else None
    async with AsyncSessionLocal() as s:
        # Check if user exists, if not create it
        user = (await s.execute(text("SELECT total_views FROM users WHERE user_id = :u"), {"u": uid})).fetchone()
        if not user:
            await s.execute(
                text("INSERT INTO users (user_id, username, total_views) VALUES (:u, :n, 0)"),
                {"u": uid, "n": update.effective_user.username}
            )
            await s.commit()
            current_views = 0
        else:
            current_views = user[0] or 0
        # Fetch all linked Instagram handles for the user
        accounts = [r[0].lower() for r in (await s.execute(
            text("SELECT insta_handle FROM allowed_accounts WHERE user_id=:u"), {"u": uid}
        )).fetchall()]
        if not accounts:
            return await update.message.reply_text("🚫 No IG linked—ask admin to /addaccount.")
        for link in links:
            m = re.search(r"^(?:https?://)?(?:www\.|m\.)?instagram\.com/(?:(?P<sup>[^/]+)/)?reel/(?P<code>[^/?#&]+)", link)
            if not m:
                results.append(f"❌ Invalid: {link}")
                continue
            sup, code = m.group("sup"), m.group("code")
            try:
                reel_data = await get_reel_data(code)
                # Check min date
                upload_date = None
                if 'taken_at_timestamp' in reel_data:
                    upload_date = datetime.utcfromtimestamp(reel_data['taken_at_timestamp']).date()
                elif 'upload_date' in reel_data:
                    upload_date = datetime.strptime(reel_data['upload_date'], "%Y-%m-%d").date()
                if min_date and upload_date and upload_date < min_date:
                    results.append(f"🚫 Too old: {link} (uploaded {upload_date}, min allowed {min_date})")
                    continue
                # Check if the reel owner matches any linked account
                if reel_data['owner_username'].lower() not in accounts:
                    results.append(f"🚫 Not your reel: {link}")
                    continue
                # Update total views in users table
                new_views = current_views + reel_data['view_count']
                await s.execute(
                    text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                    {"v": new_views, "u": uid}
                )
                # Check if this handle belongs to a slot
                slot = await s.execute(text("""
                    SELECT slot_number FROM slot_accounts 
                    WHERE LOWER(insta_handle) = LOWER(:h)
                """), {"h": reel_data['owner_username']})
                slot_result = slot.fetchone()
                if slot_result:
                    slot_number = slot_result[0]
                    await s.execute(text("""
                        INSERT INTO slot_submissions (slot_number, shortcode, insta_handle, view_count)
                        VALUES (:s, :c, :h, :v)
                    """), {
                        "s": slot_number,
                        "c": code,
                        "h": reel_data['owner_username'],
                        "v": reel_data['view_count']
                    })
                # Check for duplicate
                dup = (await s.execute(text("SELECT 1 FROM reels WHERE shortcode=:c"), {"c": code})).scalar()
                if dup:
                    results.append(f"⚠️ Already added: {link}")
                    continue
                await s.execute(text("INSERT INTO reels(user_id,shortcode) VALUES(:u,:c)"), {"u": uid, "c": code})
                
                # Log the submission
                logger.info(f"User {uid} submitted reel {code} with {reel_data['view_count']} views")
                
                results.append(f"✅ Added: {link}")
            except Exception as e:
                results.append(f"❌ Error: {link} ({str(e)})")
        await s.commit()
    await update.message.reply_text("\n".join(results))

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

        msg = [
            "👤 <b>Your Creator Statistics</b>",
            f"• Linked Instagram: {', '.join(f'@{h}' for h in handles) if handles else '—'}",
            f"• Total Reels: <b>{len(links)}</b>",
            f"• Total Views: <b>{int(total_views):,}</b>",
            f"• Payable: <b>${payable_amount:.2f}</b>",
            "• Rate: $25 per 1M Views",
            "",
            "💳 <b>Payment Methods:</b>"
        ]
        if usdt: msg.append(f"• USDT: <code>{usdt}</code>")
        if paypal: msg.append(f"• PayPal: <code>{paypal}</code>")
        if upi: msg.append(f"• UPI: <code>{upi}</code>")
        if not any([usdt, paypal, upi]):
            msg.append("• No payment methods added")

        # Pagination of reel links
        links_page, total_pages = paginate_list(links, page, page_size)
        msg.append(f"\n🎥 <b>Your Reels (Page {page}/{total_pages})</b>")
        if links_page:
            msg += [f"• https://www.instagram.com/reel/{sc}/" for sc in links_page]
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
    """Force update all user view counts based on all their reels"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")

    if force_update_lock.locked():
        return await update.message.reply_text("⏳ Another update is already in progress.")

    await force_update_lock.acquire()
    try:
        await update.message.reply_text("🔄 Starting full force update...")

        async with AsyncSessionLocal() as s:
            users = (await s.execute(text("SELECT DISTINCT user_id FROM reels"))).fetchall()
            total_updated = 0

            for idx, (user_id,) in enumerate(users, 1):
                total_views = 0
                reels = (await s.execute(
                    text("SELECT shortcode FROM reels WHERE user_id = :u"),
                    {"u": user_id}
                )).fetchall()

                for (shortcode,) in reels:
                    try:
                        reel_data = await get_reel_data(shortcode)
                        total_views += reel_data['view_count']
                    except Exception as e:
                        logger.warning(f"Could not update reel {shortcode}: {e}")

                await s.execute(
                    text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                    {"v": total_views, "u": user_id}
                )
                logger.info(f"User {user_id} updated to {total_views} views")
                total_updated += 1

                if idx % 10 == 0:
                    await context.bot.send_message(
                        update.effective_chat.id,
                        f"🔄 Progress: {idx}/{len(users)} users updated..."
                    )

            await s.commit()
            await update.message.reply_text(
                f"✅ Force update completed!\nUsers updated: {total_updated}"
            )

    except Exception as e:
        logger.error(f"Force update error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        force_update_lock.release()

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
    """Generate a private campaign leaderboard image with Telegram names and total views."""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    from PIL import Image, ImageDraw, ImageFont
    import io

    # Load new background
    bg_path = "private_leaderboard_bg.png"
    try:
        bg = Image.open(bg_path).convert("RGBA")
    except Exception as e:
        return await update.message.reply_text(f"❌ Could not load leaderboard background: {e}")

    # Colors from reference
    red = (220, 38, 54)
    white = (255, 255, 255)
    black = (0, 0, 0)

    # Fonts (use DejaVu Sans for Linux compatibility)
    try:
        font_title = ImageFont.truetype("DejaVuSans-Bold.ttf", 48)
        font_box = ImageFont.truetype("DejaVuSans-Bold.ttf", 28)
        font_views = ImageFont.truetype("DejaVuSans.ttf", 22)
    except:
        font_title = ImageFont.load_default()
        font_box = ImageFont.load_default()
        font_views = ImageFont.load_default()

    async with AsyncSessionLocal() as s:
        # Get all creators by views
        creators = (await s.execute(text("""
            SELECT 
                u.user_id,
                u.username,
                u.total_views
            FROM users u
            WHERE u.total_views > 0
            ORDER BY u.total_views DESC
        """))).fetchall()
        if not creators:
            return await update.message.reply_text("No creator statistics available.")

        leaderboard = []
        for user_id, username, views in creators:
            # Try to get Telegram username
            try:
                chat = await context.bot.get_chat(user_id)
                if chat.username:
                    uname = chat.username
                    name = uname
                else:
                    name = str(user_id)
                    uname = None
            except Exception:
                name = str(user_id)
                uname = None
            # Censor username: show first 2 and last 2 chars, rest as * (no @)
            if uname and len(uname) > 5:
                censored = f"{uname[:2]}{'*' * (len(uname) - 4)}{uname[-2:]}"
            else:
                censored = name
            leaderboard.append({
                "name": censored,
                "views": int(views)
            })

        # Grid settings (from reference)
        cols = 6
        box_w, box_h = 210, 70
        pad_x, pad_y = 20, 18
        margin_x, margin_y = 30, 180  # left/top margin for first box
        spacing_x, spacing_y = 18, 18
        # Calculate rows needed
        rows = (len(leaderboard) + cols - 1) // cols
        # Calculate image height
        base_height = bg.height
        needed_height = margin_y + rows * (box_h + spacing_y) + 30
        img_height = max(base_height, needed_height)
        img = Image.new("RGBA", (bg.width, img_height), (0, 0, 0, 255))
        img.paste(bg, (0, 0))
        draw = ImageDraw.Draw(img)

        # Draw leaderboard title (centered, white, bold)
        title = "Leaderboard for\nprivate Campaign"
        bbox = draw.multiline_textbbox((0, 0), title, font=font_title, spacing=0)
        title_w = bbox[2] - bbox[0]
        title_h = bbox[3] - bbox[1]
        draw.multiline_text(
            ((img.width - title_w) // 2, 30),
            title,
            font=font_title,
            fill=white,
            align="center"
        )

        # Draw each user in a red rounded rectangle
        for idx, entry in enumerate(leaderboard):
            row = idx // cols
            col = idx % cols
            x = margin_x + col * (box_w + spacing_x)
            y = margin_y + row * (box_h + spacing_y)
            # Draw rounded rectangle
            draw.rounded_rectangle([x, y, x + box_w, y + box_h], radius=15, fill=red)
            # Draw rank, name, views (white)
            rank_str = f"{idx+1}."
            name_str = entry["name"]
            views_str = f"{entry['views']:,} views"
            # Rank + name (bold)
            draw.text((x + pad_x, y + 8), f"{rank_str} {name_str}", font=font_box, fill=white)
            # Views (smaller, white)
            draw.text((x + pad_x, y + 38), views_str, font=font_views, fill=white)

        # Save image to bytes
        img_byte_arr = io.BytesIO()
        img = img.convert("RGB")
        img.save(img_byte_arr, format='JPEG', quality=95)
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
        ("setmindate", setmindate), 
        ("getmindate", getmindate),
        ("referral", referral), 
        ("referralstats", referralstats),
        ("setcommission", setcommission), 
        ("getcommission", getcommission),
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

if __name__ == "__main__":
    asyncio.run(run_bot())
