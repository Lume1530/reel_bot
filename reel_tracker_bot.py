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
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
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
            except Exception:
                logger.warning("Failed to send log message")
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
            'play_count': view_count  # Use same count for play_count
        }
                
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 422:
            raise Exception("Invalid reel URL or shortcode")
        raise Exception(f"API Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in get_reel_data: {str(e)}")
        raise Exception(f"Error fetching reel data: {str(e)}")

async def update_all_reel_views():
    """Update view counts for all reels in the database"""
    logger.info("Starting daily view count update...")
    async with AsyncSessionLocal() as s:
        # Get all unique reels
        reels = (await s.execute(text("SELECT DISTINCT shortcode FROM reels"))).fetchall()
        total_updated = 0
        
        for (shortcode,) in reels:
            try:
                # Get current view count from API
                reel_data = await get_reel_data(shortcode)
                new_views = reel_data['view_count']
                
                # Get the user who owns this reel
                user = (await s.execute(
                    text("SELECT user_id FROM reels WHERE shortcode = :s"),
                    {"s": shortcode}
                )).fetchone()
                
                if user:
                    user_id = user[0]
                    # Get current total views
                    current = (await s.execute(
                        text("SELECT total_views FROM users WHERE user_id = :u"),
                        {"u": user_id}
                    )).fetchone()
                    
                    if current:
                        current_views = current[0] or 0
                        # Update total views
                        await s.execute(
                            text("UPDATE users SET total_views = :v WHERE user_id = :u"),
                            {"v": new_views, "u": user_id}
                        )
                        total_updated += 1
                        logger.info(f"Updated views for user {user_id}: {current_views} -> {new_views}")
                
            except Exception as e:
                logger.error(f"Error updating views for reel {shortcode}: {str(e)}")
                continue
        
        await s.commit()
        logger.info(f"Daily view update complete. Updated {total_updated} users.")

async def start_daily_updates():
    """Start the daily view update task"""
    while True:
        try:
            # Wait until next day at midnight
            now = datetime.now()
            next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = (next_run - now).total_seconds()
            
            logger.info(f"Next view update scheduled in {wait_seconds/3600:.1f} hours")
            await asyncio.sleep(wait_seconds)
            
            # Run the update
            await update_all_reel_views()
            
        except Exception as e:
            logger.error(f"Error in daily update task: {str(e)}")
            await asyncio.sleep(3600)  # Wait an hour before retrying

# Global dict for submit cooldowns
submit_cooldowns = {}

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
            "• <code>/review &lt;user_id&gt;</code> - Review account link requests",
            "• <code>/removeaccount &lt;user_id&gt;</code> - Unlink an IG account",
            "• <code>/allstats</code> - Lists stats for all creators with payment info",
            "• <code>/currentstats</code> - Show total views and working accounts",
            "• <code>/broadcast &lt;message&gt;</code> - Send message to all users",
            "• <code>/cleardata</code> - Clear all reel links",
            "• <code>/export</code> - Export stats.txt for all users",
            "• <code>/forceupdate</code> - Force update all reel views",
            "• <code>/clearbad</code> - Remove submissions with less than 10k views after 7 days",
            "",
            "🎯 <b>Slot Management:</b>",
            "• <code>/addslot &lt;1|2&gt; &lt;handle1&gt; &lt;handle2&gt; ...</code> - Add handles to slot",
            "• <code>/removeslot &lt;1|2&gt; &lt;handle&gt;</code> - Remove handle from slot",
            "• <code>/slotstats</code> - Show current handles and submissions in slots",
            "• <code>/addadmin &lt;user_id&gt;</code> - Add new admin",
            "• <code>/removeadmin &lt;user_id&gt;</code> - Remove admin",
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
async def removeaccount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    if len(context.args) != 1:
        return await update.message.reply_text("Usage: /removeaccount <user_id>")
    uid = int(context.args[0])
    async with AsyncSessionLocal() as s:
        await s.execute(text("DELETE FROM allowed_accounts WHERE user_id=:u"), {"u": uid})
        await s.commit()
    await update.message.reply_text(f"🗑️ Unlinked {uid}")

@debug_handler
async def submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import time
    from datetime import datetime
    user_id = update.effective_user.id
    now = time.time()
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
        acc = (await s.execute(text("SELECT insta_handle FROM allowed_accounts WHERE user_id=:u"), {"u": uid})).fetchone()
        if not acc:
            return await update.message.reply_text("🚫 No IG linked—ask admin to /addaccount.")
        expected = acc[0]
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
                if reel_data['owner_username'].lower() != expected.lower():
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
                """), {"h": expected})
                slot_result = slot.fetchone()
                if slot_result:
                    slot_number = slot_result[0]
                    await s.execute(text("""
                        INSERT INTO slot_submissions (slot_number, shortcode, insta_handle, view_count)
                        VALUES (:s, :c, :h, :v)
                    """), {
                        "s": slot_number,
                        "c": code,
                        "h": expected,
                        "v": reel_data['view_count']
                    })
                dup = (await s.execute(text("SELECT 1 FROM reels WHERE shortcode=:c"), {"c": code})).scalar()
                if dup:
                    results.append(f"⚠️ Already added: {link}")
                    continue
                await s.execute(text("INSERT INTO reels(user_id,shortcode) VALUES(:u,:c)"), {"u": uid, "c": code})
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
async def addviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    if len(context.args) != 2:
        return await update.message.reply_text("Usage: /addviews <user_id> <views>")
    tid, v = map(int, context.args)
    async with AsyncSessionLocal() as s:
        exists = (await s.execute(text("SELECT 1 FROM users WHERE user_id=:u"), {"u": tid})).scalar()
        if exists:
            await s.execute(text("UPDATE users SET total_views=total_views+:v WHERE user_id=:u"), {"v": v, "u": tid})
        else:
            await s.execute(text("INSERT INTO users(user_id,username,total_views) VALUES(:u,NULL,:v)"), {"u": tid, "v": v})
        await s.commit()
    await update.message.reply_text(f"✅ Added {v} views to {tid}")

@debug_handler
async def removeviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    if len(context.args) != 2:
        return await update.message.reply_text("Usage: /removeviews <user_id> <views>")
    tid, v = map(int, context.args)
    async with AsyncSessionLocal() as s:
        await s.execute(text("UPDATE users SET total_views=GREATEST(total_views-:v,0) WHERE user_id=:u"), {"v": v, "u": tid})
        await s.commit()
    await update.message.reply_text(f"✅ Removed {v} views from {tid}")

@debug_handler
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    if len(context.args) != 1:
        return await update.message.reply_text("Usage: /stats <user_id>")
    tid = int(context.args[0])
    try:
        chat = await context.bot.get_chat(tid)
        full_name = " ".join(filter(None, [chat.first_name, chat.last_name]))
    except:
        full_name = str(tid)
    async with AsyncSessionLocal() as s:
        row = (await s.execute(text("SELECT total_views FROM users WHERE user_id=:u"), {"u": tid})).fetchone()
        views = row[0] if row else 0
        reels = [r[0] for r in (await s.execute(text("SELECT shortcode FROM reels WHERE user_id=:u"), {"u": tid})).fetchall()]
        acc = (await s.execute(text("SELECT insta_handle FROM allowed_accounts WHERE user_id=:u"), {"u": tid})).fetchone()
        handle = acc[0] if acc else "—"
    msg = [
        f"📊 <b>Stats for {full_name} (@{handle})</b>",
        f"• Total views: <b>{views}</b>",
        "🎥 <b>Reels:</b>",
    ] + [f"• https://www.instagram.com/reel/{sc}/" for sc in reels]
    await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)

@debug_handler
async def allstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics for all creators (except their links)"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    async with AsyncSessionLocal() as s:
        # Get all users with their stats and payment details
        users = (await s.execute(text("""
            SELECT 
                u.user_id, 
                u.username, 
                u.total_views, 
                a.insta_handle,
                p.usdt_address,
                p.paypal_email,
                p.upi_address
            FROM users u
            LEFT JOIN allowed_accounts a ON u.user_id = a.user_id
            LEFT JOIN payment_details p ON u.user_id = p.user_id
            WHERE u.total_views > 0
            ORDER BY u.total_views DESC
        """))).fetchall()
        
        if not users:
            return await update.message.reply_text("No creator statistics available.")
        
        # Send stats for each creator
        for user_id, username, views, insta_handle, usdt, paypal, upi in users:
            views = float(views)
            # Calculate payable amount (views * 0.025 per 1000 views)
            payable_amount = (views / 1000) * 0.025
            
            try:
                # Try to get user's full name
                chat = await context.bot.get_chat(user_id)
                full_name = " ".join(filter(None, [chat.first_name, chat.last_name]))
            except:
                full_name = str(user_id)
            
            msg = [
                f"👤 <b>Creator: {full_name}</b>",
                f"• Username: @{username or '—'}",
                f"• Instagram: @{insta_handle or '—'}",
                f"• Total Views: <b>{int(views):,}</b>",
                f"• Payable Amount: <b>${payable_amount:,.2f}</b>",
                f"• Rate: <b>$25 per 1M Views</b>",
                "",
                "💰 <b>Payment Details:</b>"
            ]
            
            # Add payment methods if they exist
            if usdt:
                msg.append(f"• USDT: <code>{usdt}</code>")
            if paypal:
                msg.append(f"• PayPal: <code>{paypal}</code>")
            if upi:
                msg.append(f"• UPI: <code>{upi}</code>")
            if not any([usdt, paypal, upi]):
                msg.append("• No payment methods added")
            
            msg.append("━━━━━━━━━━━━━━━━━━━━")
            
            await context.bot.send_message(
                update.effective_chat.id,
                "\n".join(msg),
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(0.5)  # Small delay between messages

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

@debug_handler
async def creatorstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show creator's total views, payable amount, payment details, and submitted links"""
    uid = update.effective_user.id
    async with AsyncSessionLocal() as s:
        # Get total views for this creator
        total_views = (await s.execute(
            text("SELECT COALESCE(total_views, 0) FROM users WHERE user_id = :u"),
            {"u": uid}
        )).scalar() or 0
        total_views = float(total_views)
        
        # Calculate payable amount (views * 0.025 per 1000 views)
        payable_amount = (total_views / 1000) * 0.025
        
        # Get all links submitted by this creator
        links = [r[0] for r in (await s.execute(
            text("SELECT shortcode FROM reels WHERE user_id = :u"),
            {"u": uid}
        )).fetchall()]
        
        # Get total videos count
        total_videos = len(links)
        
        # Get Instagram handle
        insta_handle = (await s.execute(
            text("SELECT insta_handle FROM allowed_accounts WHERE user_id = :u"),
            {"u": uid}
        )).scalar()
        
        # Get payment details
        payment_details = (await s.execute(
            text("SELECT usdt_address, paypal_email, upi_address FROM payment_details WHERE user_id = :u"),
            {"u": uid}
        )).fetchone()
        
        usdt, paypal, upi = payment_details or (None, None, None)
        
        # Format the message
        msg = [
            "👤 <b>Creator Statistics</b>",
            f"• Instagram: @{insta_handle or '—'}",
            f"• Total Videos: <b>{total_videos}</b>",
            f"• Total Views: <b>{int(total_views):,}</b>",
            f"• Payable Amount: <b>${payable_amount:,.2f}</b>",
            f"• Rate: <b>$25 per 1M Views</b>",
            "",
            "💳 <b>Your Payment Methods:</b>"
        ]
        
        # Add payment methods if they exist
        if usdt:
            msg.append(f"• USDT: <code>{usdt}</code>")
        if paypal:
            msg.append(f"• PayPal: <code>{paypal}</code>")
        if upi:
            msg.append(f"• UPI: <code>{upi}</code>")
        if not any([usdt, paypal, upi]):
            msg.append("• No payment methods added")
        
        msg.append("")
        msg.append("🎥 <b>Your Submitted Links:</b>")
        
        # Add all links
        if links:
            msg += [f"• https://www.instagram.com/reel/{sc}/" for sc in links]
        else:
            msg.append("• No links submitted yet")
        
        await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)

@debug_handler
async def currentstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current total views, payable amount, and working accounts"""
    async with AsyncSessionLocal() as s:
        # Get total views across all users
        total_views = (await s.execute(text("SELECT COALESCE(SUM(total_views), 0) FROM users"))).scalar() or 0
        total_views = float(total_views)
        
        # Get total working accounts (users with linked Instagram accounts)
        working_accounts = (await s.execute(text("SELECT COUNT(DISTINCT user_id) FROM allowed_accounts"))).scalar() or 0
        
        # Calculate payable amount (views * 0.025 per 1000 views)
        payable_amount = (total_views / 1000) * 0.025
        
        # Format the message
        msg = [
            "📊 <b>Current Statistics</b>",
            f"• Total Views: <b>{int(total_views):,}</b>",
            f"• Working Accounts: <b>{working_accounts}</b>",
            f"• Payable Amount: <b>${payable_amount:,.2f}</b>",
            "",
            "<i>Pay Rate: <b>$25 per 1M Views</b></i>"
        ]
        
        await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)

@debug_handler
async def statsdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics for all creators (except their links)"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
        
    async with AsyncSessionLocal() as s:
        # Get all users with their stats
        users = (await s.execute(text("""
            SELECT u.user_id, u.username, u.total_views, a.insta_handle
            FROM users u
            LEFT JOIN allowed_accounts a ON u.user_id = a.user_id
            WHERE u.total_views > 0
            ORDER BY u.total_views DESC
        """))).fetchall()
        
        if not users:
            return await update.message.reply_text("No creator statistics available.")
            
        # Send stats for each creator
        for user_id, username, views, insta_handle in users:
            views = float(views)
            # Calculate payable amount (views * 0.025 per 1000 views)
            payable_amount = (views / 1000) * 0.025
            
            try:
                # Try to get user's full name
                chat = await context.bot.get_chat(user_id)
                full_name = " ".join(filter(None, [chat.first_name, chat.last_name]))
            except:
                full_name = str(user_id)
            
            msg = [
                f"👤 <b>Creator: {full_name}</b>",
                f"• Username: @{username or '—'}",
                f"• Instagram: @{insta_handle or '—'}",
                f"• Total Views: <b>{int(views):,}</b>",
                f"• Payable Amount: <b>${payable_amount:,.2f}</b>",
                "━━━━━━━━━━━━━━━━━━━━"
            ]
            
            await context.bot.send_message(
                update.effective_chat.id,
                "\n".join(msg),
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(0.5)  # Small delay between messages

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
    """Force update all reel views"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    await update.message.reply_text("🔄 Starting view count update...")
    try:
        await update_all_reel_views()
        await update.message.reply_text("✅ View count update completed!")
    except Exception as e:
        logger.error(f"Error in forceupdate: {str(e)}")
        await update.message.reply_text(f"❌ Error updating views: {str(e)}")

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
    """Remove submissions with less than 10k views after 7 days and update user total views."""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    async with AsyncSessionLocal() as s:
        # Get submissions older than 7 days
        old_submissions = await s.execute(text("""
            SELECT r.shortcode, r.user_id, u.total_views
            FROM reels r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.created_at < NOW() - INTERVAL '7 days'
        """))
        removed_count = 0
        affected_users = set()
        for shortcode, user_id, views in old_submissions:
            if views < 10000:
                # Remove the submission
                await s.execute(
                    text("DELETE FROM reels WHERE shortcode = :s AND user_id = :u"),
                    {"s": shortcode, "u": user_id}
                )
                removed_count += 1
                affected_users.add(user_id)
                # Notify user
                try:
                    await context.bot.send_message(
                        user_id,
                        f"⚠️ Your submission https://www.instagram.com/reel/{shortcode}/ "
                        f"has been removed as it didn't reach 10k views within 7 days."
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {user_id}: {e}")
        # Recalculate total_views for affected users
        for user_id in affected_users:
            # Sum views for all remaining reels for this user
            reels = await s.execute(text("SELECT shortcode FROM reels WHERE user_id = :u"), {"u": user_id})
            codes = [r[0] for r in reels.fetchall()]
            total_views = 0
            for code in codes:
                try:
                    reel_data = await get_reel_data(code)
                    total_views += reel_data['view_count']
                except Exception:
                    pass
            await s.execute(text("UPDATE users SET total_views = :v WHERE user_id = :u"), {"v": total_views, "u": user_id})
        await s.commit()
        await update.message.reply_text(f"✅ Removed {removed_count} submissions with low views and updated user view counts.")

@debug_handler
async def addslot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add Instagram handles to a slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) < 2:
        return await update.message.reply_text(
            "Usage: /addslot <slot_number> <handle1> <handle2> ...\n"
            "Example: /addslot 1 johndoe janedoe"
        )
    
    try:
        slot_number = int(context.args[0])
        if slot_number not in [1, 2]:
            return await update.message.reply_text("❌ Slot number must be 1 or 2")
        
        handles = [h.lstrip('@') for h in context.args[1:]]
        
        async with AsyncSessionLocal() as s:
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
            
            await update.message.reply_text(
                f"✅ Added {len(handles)} handles to slot {slot_number}:\n" +
                "\n".join(f"• @{h}" for h in handles)
            )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid slot number")

@debug_handler
async def removeslot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a handle from a specific slot"""
    if not await is_admin(update.effective_user.id):
        return await update.message.reply_text("🚫 Unauthorized")
    
    if len(context.args) != 2:
        return await update.message.reply_text(
            "Usage: /removeslot <slot_number> <handle>\n"
            "Example: /removeslot 1 johndoe"
        )
    
    try:
        slot_number = int(context.args[0])
        if slot_number not in [1, 2]:
            return await update.message.reply_text("❌ Slot number must be 1 or 2")
        
        handle = context.args[1].lstrip('@')
        
        async with AsyncSessionLocal() as s:
            # Check if handle exists in the slot
            existing = await s.execute(
                text("""
                    SELECT 1 FROM slot_accounts 
                    WHERE slot_number = :s AND insta_handle = :h
                """),
                {"s": slot_number, "h": handle}
            )
            
            if not existing.scalar():
                return await update.message.reply_text(
                    f"❌ @{handle} is not in slot {slot_number}"
                )
            
            # Remove handle from slot
            await s.execute(
                text("""
                    DELETE FROM slot_accounts 
                    WHERE slot_number = :s AND insta_handle = :h
                """),
                {"s": slot_number, "h": handle}
            )
            await s.commit()
            
            await update.message.reply_text(
                f"✅ Removed @{handle} from slot {slot_number}"
            )
            
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

async def process_slots(bot):
    """Process slot submissions every 5 minutes (for testing)"""
    while True:
        try:
            # Wait 5 minutes
            await asyncio.sleep(300)  # 5 minutes = 300 seconds
            
            logger.info("Processing slot submissions...")
            
            # Process slots
            async with AsyncSessionLocal() as s:
                for slot_number in [1, 2]:
                    # Get all submissions for this slot
                    submissions = await s.execute(text("""
                        SELECT shortcode, insta_handle, view_count
                        FROM slot_submissions
                        WHERE slot_number = :s
                        ORDER BY submitted_at
                    """), {"s": slot_number})
                    
                    if submissions:
                        # Create text file
                        lines = []
                        for shortcode, handle, views in submissions:
                            lines.append(f"@{handle}: https://www.instagram.com/reel/{shortcode}/ ({views:,} views)")
                        
                        # Send to admins
                        for admin_id in ADMIN_IDS:
                            try:
                                import io
                                buf = io.BytesIO("\n".join(lines).encode())
                                buf.name = f"slot{slot_number}_submissions.txt"
                                
                                await bot.send_document(
                                    admin_id,
                                    document=buf,
                                    caption=f"📋 Slot {slot_number} submissions (5min test)"
                                )
                            except Exception as e:
                                logger.error(f"Failed to send slot {slot_number} to admin {admin_id}: {e}")
                        
                        # Clear slot submissions
                        await s.execute(
                            text("DELETE FROM slot_submissions WHERE slot_number = :s"),
                            {"s": slot_number}
                        )
                
                await s.commit()
            
        except Exception as e:
            logger.error(f"Error in slot processing: {str(e)}")
            await asyncio.sleep(60)  # Wait a minute before retrying if there's an error

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

async def run_bot():
    await init_db()
    await ensure_config_table()
    await migrate_allowed_accounts()
    asyncio.create_task(start_health_check_server())
    asyncio.create_task(start_daily_updates())
    
    app = ApplicationBuilder().token(TOKEN).build()
    # Start slot processing with bot instance
    asyncio.create_task(process_slots(app.bot))
    
    handlers = [
        ("start", start_cmd), ("addaccount", addaccount), ("removeaccount", removeaccount),
        ("submit", submit), ("remove", remove), ("cleardata", cleardata),
        ("allstats", allstats), ("currentstats", currentstats), ("creatorstats", creatorstats),
        ("broadcast", broadcast), ("export", export), ("addusdt", add_usdt),
        ("addpaypal", add_paypal), ("addupi", add_upi), ("review", review),
        ("forceupdate", forceupdate), ("addadmin", addadmin), ("removeadmin", removeadmin),
        ("clearbad", clearbad), ("addslot", addslot), ("removeslot", removeslot),
        ("slotstats", slotstats), ("lbpng", lbpng), ("setmindate", setmindate), ("getmindate", getmindate),
    ]
    for cmd, h in handlers:
        app.add_handler(CommandHandler(cmd, h))
    
    # Add callback query handler for review buttons
    app.add_handler(CallbackQueryHandler(handle_review_callback))
    
    await app.initialize(); await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(run_bot()) 
