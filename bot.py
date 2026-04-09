import os
import re
import time
import asyncio
import logging
import posixpath
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import database as db

# Mega Web API Modules (Fast Engine)
from mega import Mega
from mega.crypto import base64_url_encode, encrypt_attr

# ==========================================
# Config
# ==========================================
BOT_TOKEN      = "8467428373:AAGh5NuSkPkTWkZL_ytqz9qunrJLkZWrkCk"
OWNER_ID       = 8493596199
OWNER_USERNAME = "@Sourav00876"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

user_sessions = {}

# ==========================================
# Auth Helpers
# ==========================================
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def check_auth(user_id: int) -> bool:
    return is_owner(user_id) or await db.is_authorised(user_id)

async def send_unauthorized(update: Update):
    user = update.effective_user
    await update.message.reply_text(
        f"🚫 *Access Denied!*\n\nAap authorized nahi hain.\n👤 ID: `{user.id}`\n✅ Access ke liye contact karein: {OWNER_USERNAME}",
        parse_mode="Markdown"
    )

# ==========================================
# SUPER-FAST MEGA API BATCH ENGINE
# ==========================================
def mega_login(email, password):
    m = Mega()
    m.login(email, password)
    return m

def prepare_api_reqs(m, chunk_list):
    """Files/Folders ka encrypted chunk banata hai"""
    reqs = []
    for node, new_name in chunk_list:
        try:
            attributes = m._api.decrypt_attr(node['a'], node['k'])
            attributes['n'] = new_name
            encrypted_attr = encrypt_attr(attributes, node['k'])
            reqs.append({
                'a': 'a',
                'attr': base64_url_encode(encrypted_attr),
                'n': node['h']
            })
        except Exception:
            pass
    return reqs

def execute_api_reqs(m, reqs):
    """Chunk ko Mega server par direct hit karta hai (Bypasses Storage limit)"""
    if reqs:
        m._api.api_request(reqs)

# ==========================================
# Smart Suffix Logic
# ==========================================
def build_suffix_name(old_name: str, suffix_text: str, is_folder: bool) -> str:
    """
    Agar is_folder True hai, toh extension check nahi karega.
    Agar is_folder False hai (matlab file hai), toh extension preserve karega.
    """
    if is_folder:
        # Folder ke liye seedha space dekar suffix laga do
        if not suffix_text.startswith((' ', '_', '-')):
            return f"{old_name} {suffix_text}"
        return f"{old_name}{suffix_text}"
    else:
        # File ke liye extension alag karo, suffix lagao, phir extension wapas jodo
        name, ext = posixpath.splitext(old_name)
        if not suffix_text.startswith((' ', '_', '-')):
            return f"{name} {suffix_text}{ext}"
        return f"{name}{suffix_text}{ext}"

# ==========================================
# /start
# ==========================================
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await db.add_user(user_id)

    if not await check_auth(user_id):
        await send_unauthorized(update)
        return

    msg = (
        "🚀 *MEGA.NZ PRO RENAMER BOT*\n\n"
        "⚡ *Lightspeed Web API Engine* - Bypasses Storage Locks!\n\n"
        "📌 *Commands:*\n"
        "  `/login email password` — Mega.nz login\n"
        "  `/logout` — Logout current session\n"
        "  `/suffix @name` — Smart renaming (Files/Folders)\n"
        "  `/stats` — Database Stats & Quota\n"
        "  `/premium` — Check premium status\n"
        "  `/check <link>` — Check MEGA link details\n"
        "  `/megainfo` — Account storage & file stats\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==========================================
# /login & /logout
# ==========================================
async def login_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await db.add_user(uid)

    if not await check_auth(uid):
        await send_unauthorized(update)
        return

    if len(ctx.args) < 2:
        await update.message.reply_text("❌ Usage: `/login email pass`", parse_mode="Markdown")
        return

    email, password = ctx.args[0], ctx.args[1]
    msg = await update.message.reply_text("🔄 API Server se login ho raha hai...")

    try:
        m = await asyncio.to_thread(mega_login, email, password)
        user_sessions[uid] = {"email": email, "m": m}
        await db.save_session(uid, email)
        await msg.edit_text(f"✅ *Login Successful!*\n📧 `{email}`\n\nAb `/suffix @channelname` use karein.", parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ Login Failed!\nError: `{e}`", parse_mode="Markdown")

async def logout_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in user_sessions:
        del user_sessions[uid]
        await update.message.reply_text("✅ *Logout Successful!* Ab naya account `/login` kar sakte ho.", parse_mode="Markdown")
    else:
        await update.message.reply_text("⚠️ Aap pehle se hi logged out hain.", parse_mode="Markdown")

# ==========================================
# Stats & Premium
# ==========================================
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update)
        return

    user_data = await db.get_user(uid)
    if not user_data:
        await update.message.reply_text("Pehle /start type karke DB account banayein.")
        return

    session = user_sessions.get(uid) or await db.get_session(uid)
    email = session.get("email", "Not logged in") if session else "Not logged in"

    stats_text = (
        f"📊 *User Database Stats*\n\n"
        f"👤 UID: `{user_data['_id']}`\n"
        f"📧 Email: `{email}`\n"
        f"🔄 Lifetime Renamed: `{user_data['lifetime_renamed']}`\n"
        f"⚡ Daily Limit Remaining: `{user_data['daily_limit']}`\n"
        f"👑 Premium: `{'Yes ✨' if user_data['is_premium'] else 'No'}`"
    )
    await update.message.reply_text(stats_text, parse_mode="Markdown")

async def premium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return

    user_data = await db.get_user(uid)
    is_prem = user_data.get("is_premium", False) if user_data else False
    text = (
        f"⭐ *Premium Status*\n\n"
        f"{'✅ Aapke paas Premium access hai!' if is_prem else '❌ Aapke paas Premium nahi hai.'}\n\n"
        f"{'🎉 Unlimited daily renames enjoy karein!' if is_prem else f'Premium ke liye contact karein: {OWNER_USERNAME}'}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# ==========================================
# /suffix (Core Logic)
# ==========================================
async def suffix_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return

    session = user_sessions.get(uid)
    if not session or "m" not in session:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown")
        return

    if not ctx.args:
        await update.message.reply_text("❌ *Usage:* `/suffix @channelname`\n\nExample: `/suffix @adult_flix_official`", parse_mode="Markdown")
        return

    suffix_text = " ".join(ctx.args)
    ctx.user_data["rename_replacement"] = suffix_text

    keyboard = [
        [InlineKeyboardButton("📁 Folders Only", callback_data="mode_folders")],
        [InlineKeyboardButton("📄 Files Only", callback_data="mode_files")],
        [InlineKeyboardButton("🔄 Both (Files + Folders)", callback_data="mode_both")],
    ]
    await update.message.reply_text(
        f"🎯 *Suffix:* `{suffix_text}`\n\nKisko rename karna chahte ho?", 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode="Markdown"
    )

# ==========================================
# Button & Bulk Rename Handler
# ==========================================
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid  = query.from_user.id

    if data in ["mode_folders", "mode_files", "mode_both"]:
        ctx.user_data["rename_mode"] = data
        await query.edit_message_text("🚀 Rename Processing Started...")
        asyncio.create_task(do_bulk_rename(query.message, uid, ctx))

async def do_bulk_rename(message, uid: int, ctx: ContextTypes.DEFAULT_TYPE):
    suffix_text = ctx.user_data.get("rename_replacement", "")
    mode = ctx.user_data.get("rename_mode", "mode_files")
    
    user_data = await db.get_user(uid)
    if user_data['daily_limit'] <= 0 and not user_data.get('is_premium'):
        await message.reply_text("❌ Aapki daily limit khatam ho chuki hai.")
        return

    session = user_sessions.get(uid)
    m = session["m"]
    
    status_msg = await message.reply_text("⏳ Scanning Mega Account...", parse_mode="Markdown")
    start_time = time.time()

    try:
        # File fetch karna
        all_nodes = await asyncio.to_thread(m.get_files)
        
        files_to_process = []
        for fid, node in all_nodes.items():
            t = node.get('t')
            # t == 0 means file, t == 1 means folder
            if mode == "mode_files" and t == 0:
                files_to_process.append(node)
            elif mode == "mode_folders" and t == 1 and node.get('a'):
                files_to_process.append(node)
            elif mode == "mode_both" and t in (0, 1) and node.get('a'):
                files_to_process.append(node)
                
        total = len(files_to_process)
        
        if total == 0:
            await status_msg.edit_text("📂 Koi file ya folder nahi mila.")
            return

        # Limits Check
        max_allowed = user_data['daily_limit'] if not user_data.get('is_premium') else total
        files_to_process = files_to_process[:max_allowed]
        
        rename_list = []
        skipped = 0

        # Pattern apply karna
        for idx, node in enumerate(files_to_process, start=1):
            old_name = node.get('a', {}).get('n', '')
            if not old_name:
                skipped += 1
                continue
            
            is_folder = (node.get('t') == 1)
            new_name = build_suffix_name(old_name, suffix_text, is_folder)
            
            if new_name != old_name:
                rename_list.append((node, new_name))
            else:
                skipped += 1
                
        # Main Lightspeed API Call with Progress Bar
        total_reqs = len(rename_list)
        done = 0
        
        if total_reqs > 0:
            chunk_size = 500
            for i in range(0, total_reqs, chunk_size):
                chunk = rename_list[i:i+chunk_size]
                
                reqs = await asyncio.to_thread(prepare_api_reqs, m, chunk)
                await asyncio.to_thread(execute_api_reqs, m, reqs)
                done += len(chunk)
                
                percent = int((done / total_reqs) * 100)
                bar_filled = percent // 10
                bar = "▰" * bar_filled + "▱" * (10 - bar_filled)
                
                try:
                    await status_msg.edit_text(
                        f"🔄 *Renaming in Progress...*\n\n"
                        f"`[{bar}]` *{percent}%*\n\n"
                        f"📦 *Processed:* `{done} / {total_reqs}`\n"
                        f"🎯 *Mode:* `{mode.split('_')[1].capitalize()}`",
                        parse_mode="Markdown"
                    )
                except Exception: pass
                await asyncio.sleep(0.5)

        elapsed = round(time.time() - start_time, 2)

        # Output Summary
        final_text = (
            f"⚙️ *Rename Processing Completed*\n\n"
            f"• *Total Found:* `{total}`\n"
            f"• *Renamed:* `{done}`\n"
            f"• *Skipped:* `{skipped}`\n"
            f"• *Mode:* `{mode.split('_')[1].capitalize()}`\n"
            f"• *Time:* `{elapsed}s`\n\n"
        )
        
        if done >= user_data['daily_limit'] and not user_data.get('is_premium'):
            final_text += "⚠️ Free limit reached. Upgrade to Premium for unlimited access."
        else:
            final_text += "💾 Database updated! `/stats` check karein."

        await db.update_rename_stats(uid, done)
        await status_msg.edit_text(final_text, parse_mode="Markdown")

    except Exception as e:
        await message.reply_text(f"❌ Error: `{e}`")

# ==========================================
# Owner Commands (Auth, SetPremium)
# ==========================================
async def auth_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not ctx.args:
        await update.message.reply_text("Usage: `/auth user_id`", parse_mode="Markdown"); return
    try:
        uid = int(ctx.args[0])
        await db.add_user(uid)
        await db.add_auth(uid)
        await update.message.reply_text(f"✅ User `{uid}` ko authorize kar diya!", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def unauth_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not ctx.args:
        await update.message.reply_text("Usage: `/unauth user_id`", parse_mode="Markdown"); return
    try:
        uid = int(ctx.args[0])
        await db.remove_auth(uid)
        await update.message.reply_text(f"✅ User `{uid}` ka authorization hata diya.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def setpremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not ctx.args:
        await update.message.reply_text("Usage: `/setpremium user_id`", parse_mode="Markdown"); return
    try:
        uid = int(ctx.args[0])
        await db.set_premium(uid, True)
        await db.reset_daily_limit(uid, 999999)
        await update.message.reply_text(f"⭐ User `{uid}` ko Premium de diya!", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

# ==========================================
# Koyeb Health Check Server & Main
# ==========================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")
    def log_message(self, format, *args): pass

def start_health_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()

if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("login", login_cmd))
    app.add_handler(CommandHandler("logout", logout_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("premium", premium_cmd))
    app.add_handler(CommandHandler("suffix", suffix_cmd))
    
    # Owner cmds
    app.add_handler(CommandHandler("auth", auth_cmd))
    app.add_handler(CommandHandler("unauth", unauth_cmd))
    app.add_handler(CommandHandler("setpremium", setpremium_cmd))
    
    app.add_handler(CallbackQueryHandler(button_handler))

    print("🚀 Lightspeed API Bot Backend is Running...")
    app.run_polling(drop_pending_updates=True)
