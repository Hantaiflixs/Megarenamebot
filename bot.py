import os
import re
import asyncio
import logging
import subprocess
import posixpath
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from dotenv import load_dotenv
import database as db
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading


load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory session just for Mega login state (User DB mein safe hai)
user_sessions = {}
rename_jobs = {}

# ==========================================
# MegaCMD Helper Engine (Crash-Free)
# ==========================================
def run_cmd(args):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=120)
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return "", str(e), 1

def mega_login(email, password):
    out, err, code = run_cmd(["mega-login", email, password])
    if code != 0 and "Already logged in" not in out:
        raise Exception(err or out)

def mega_get_all_files():
    out, err, code = run_cmd(["mega-find", "/", "--type=f"])
    if code == 0:
        return [line.strip() for line in out.strip().split('\n') if line.strip()]
    raise Exception(err or out)

def mega_rename(old_path, new_path):
    out, err, code = run_cmd(["mega-mv", old_path, new_path])
    if code != 0:
        raise Exception(err or out)

# ==========================================
# Rename Pattern Logic (From Original Code)
# ==========================================
def build_new_name(old_name: str, pattern: str, replacement: str, index: int) -> str:
    name, ext = posixpath.splitext(old_name)
    if pattern == "prefix": return f"{replacement}{old_name}"
    elif pattern == "suffix": return f"{name}{replacement}{ext}"
    elif pattern == "replace":
        parts = replacement.split("|", 1)
        if len(parts) == 2: return old_name.replace(parts[0], parts[1])
    elif pattern == "regex":
        parts = replacement.split("|", 1)
        if len(parts) == 2:
            try: return re.sub(parts[0], parts[1], old_name)
            except re.error: pass
    elif pattern == "template": return replacement.replace("{n}", name).replace("{i}", str(index)).replace("{ext}", ext)
    elif pattern == "number": return f"{str(index).zfill(5)}{ext}"
    return old_name

# ==========================================
# Telegram Commands & UI
# ==========================================
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await db.add_user(user_id) # Save user to Database
    
    msg = (
        "🚀 *MEGA.NZ PRO RENAMER BOT*\n\n"
        "Advanced & Crash-Free Engine. Database Connected! 💾\n\n"
        "📌 *Commands:*\n"
        "  `/login email password` — Mega.nz login\n"
        "  `/stats` — Database Stats & Quota\n"
        "  `/renameall` — Start Rename Process\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def login_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(ctx.args) < 2:
        await update.message.reply_text("❌ Usage: `/login email pass`", parse_mode="Markdown")
        return

    email, password = ctx.args[0], ctx.args[1]
    msg = await update.message.reply_text("🔄 MegaCMD Engine se login ho raha hai...")

    try:
        await asyncio.to_thread(mega_login, email, password)
        user_sessions[uid] = {"email": email}
        await msg.edit_text(f"✅ *Login Sucessful!*\n📧 `{email}`\n\nAb `/renameall` use karein.", parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ Login Failed!\nError: `{e}`", parse_mode="Markdown")

async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = await db.get_user(user_id)
    
    if not user_data:
        await update.message.reply_text("Pehle /start type karke DB account banayein.")
        return
        
    stats_text = (
        f"📊 *User Database Stats*\n\n"
        f"👤 UID: `{user_data['_id']}`\n"
        f"🔄 Lifetime Renamed: `{user_data['lifetime_renamed']}`\n"
        f"⚡ Daily Limit Remaining: `{user_data['daily_limit']}`\n"
        f"👑 Premium: `{'Yes' if user_data['is_premium'] else 'No'}`"
    )
    await update.message.reply_text(stats_text, parse_mode="Markdown")

async def renameall_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in user_sessions:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🔤 Add Prefix", callback_data="pattern_prefix")],
        [InlineKeyboardButton("🔡 Add Suffix", callback_data="pattern_suffix")],
        [InlineKeyboardButton("🔄 Replace Text", callback_data="pattern_replace")],
        [InlineKeyboardButton("🔢 Sequential Number", callback_data="pattern_number")],
    ]
    await update.message.reply_text("🎯 *Rename Pattern Select Karein:*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if data.startswith("pattern_"):
        pattern = data.replace("pattern_", "")
        ctx.user_data["rename_pattern"] = pattern

        if pattern == "number":
            ctx.user_data["rename_replacement"] = ""
            keyboard = [[InlineKeyboardButton("✅ Start", callback_data="confirm_rename")]]
            await query.edit_message_text("🔢 Sabhi files `00001.ext` pattern me rename hongi.", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text(f"✏️ Ab apna {pattern} text type karke bhejein:")
            ctx.user_data["awaiting_input"] = True

    elif data == "confirm_rename":
        await query.edit_message_text("🚀 Rename Processing Started...")
        asyncio.create_task(do_bulk_rename(query.message, query.from_user.id, ctx))

async def message_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.user_data.get("awaiting_input"): return
    
    text = update.message.text.strip()
    ctx.user_data["rename_replacement"] = text
    ctx.user_data["awaiting_input"] = False
    pattern = ctx.user_data.get("rename_pattern", "")

    example_new = build_new_name("Example_File.mp4", pattern, text, 1)

    keyboard = [[InlineKeyboardButton("✅ Start Renaming!", callback_data="confirm_rename")]]
    await update.message.reply_text(f"👁 *Preview:*\n\n📄 Naya Naam: `{example_new}`\n\nConfirm?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def do_bulk_rename(message, uid: int, ctx: ContextTypes.DEFAULT_TYPE):
    pattern = ctx.user_data.get("rename_pattern", "prefix")
    replacement = ctx.user_data.get("rename_replacement", "")
    
    # DB se user limit check karein
    user_data = await db.get_user(uid)
    if user_data['daily_limit'] <= 0:
        await message.reply_text("❌ Aapki daily limit khatam ho chuki hai.")
        return

    try:
        files = await asyncio.to_thread(mega_get_all_files)
        total = len(files)
        if total == 0:
            await message.reply_text("📂 Koi file nahi mili.")
            return

        status_msg = await message.reply_text(f"🔄 *Renaming Started!*\n📊 Total Files: `{total}`", parse_mode="Markdown")
        done = 0

        for idx, file_path in enumerate(files, start=1):
            if done >= user_data['daily_limit']:
                await message.reply_text("⚠️ Aapki daily limit hit ho gayi hai. Process ruk gaya.")
                break

            old_name = posixpath.basename(file_path)
            new_name = build_new_name(old_name, pattern, replacement, idx)

            if new_name != old_name:
                parent = posixpath.dirname(file_path)
                new_path = f"{parent}/{new_name}" if parent not in ["", "/"] else f"/{new_name}"
                
                await asyncio.to_thread(mega_rename, file_path, new_path)
                done += 1

            if idx % 5 == 0 or idx == total:
                try: await status_msg.edit_text(f"🔄 *Processing...*\n✅ Done: `{done}/{total}`", parse_mode="Markdown")
                except: pass

        # DB me limit update karein
        await db.update_rename_stats(uid, done)
        await status_msg.edit_text(f"🎉 *Success!*\n✅ Renamed: `{done}` files.\n💾 Database updated! `/stats` check karein.", parse_mode="Markdown")

    except Exception as e:
        await message.reply_text(f"❌ Error: `{e}`")
# ==========================================
# Koyeb Health Check Server
# ==========================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")
    
    # HTTP requests ke spam logs band karne ke liye
    def log_message(self, format, *args): 
        pass

def start_health_server():
    port = int(os.getenv("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()

if __name__ == "__main__":
    # Health check server ko background thread me start karna
    threading.Thread(target=start_health_server, daemon=True).start()
    
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("login", login_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("renameall", renameall_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    print("🤖 Pro Mega Bot Backend is Running with Health Checks...")
    app.run_polling(drop_pending_updates=True)

