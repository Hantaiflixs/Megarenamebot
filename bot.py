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
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import database as db

# ==========================================
# Config
# ==========================================
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "8467428373:AAGh5NuSkPkTWkZL_ytqz9qunrJLkZWrkCk")
OWNER_ID       = int(os.environ.get("OWNER_ID", "8493596199"))   # <-- apna Telegram ID daalo
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "@Sourav00876")

CMD_TIMEOUT    = 60    # seconds for each mega command
BATCH_SIZE     = 50    # files per batch for large renames
MAX_FILES      = 10000 # max files per session

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

user_sessions = {}   # in-memory fast cache (also backed in MongoDB)

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
        f"🚫 *Access Denied!*\n\n"
        f"Aap is bot ko use karne ke liye authorized nahi hain.\n\n"
        f"👤 Aapka ID: `{user.id}`\n"
        f"📛 Naam: {user.first_name}\n\n"
        f"✅ Access ke liye owner se contact karein:\n"
        f"➡️ {OWNER_USERNAME}",
        parse_mode="Markdown"
    )

# ==========================================
# MegaCMD Helper Engine
# ==========================================

def run_cmd(args, timeout=CMD_TIMEOUT):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", f"Command timed out after {timeout}s", 1
    except Exception as e:
        return "", str(e), 1

def mega_login(email, password):
    out, err, code = run_cmd(["mega-login", email, password])
    if code != 0 and "Already logged in" not in out:
        raise Exception(err or out)

def mega_logout():
    run_cmd(["mega-logout"])

def mega_get_all_files():
    out, err, code = run_cmd(["mega-find", "/", "--type=f"], timeout=CMD_TIMEOUT * 3)
    if code == 0:
        files = [line.strip() for line in out.strip().split('\n') if line.strip()]
        return files[:MAX_FILES]
    raise Exception(err or out)

def mega_rename(old_path, new_path):
    out, err, code = run_cmd(["mega-mv", old_path, new_path])
    if code != 0:
        raise Exception(err or out)

def mega_check_link(link: str) -> dict:
    """Check a MEGA link and return its info."""
    out, err, code = run_cmd(["mega-ls", link], timeout=CMD_TIMEOUT)
    if code != 0:
        raise Exception(err or out or "Invalid or expired link")
    
    # Get file info
    info_out, _, info_code = run_cmd(["mega-ls", "-l", link], timeout=CMD_TIMEOUT)
    lines = [l.strip() for l in info_out.strip().split('\n') if l.strip()]
    
    file_count = len([l for l in lines if not l.startswith('/')])
    total_size = 0
    
    for line in lines:
        parts = line.split()
        if len(parts) >= 4:
            try:
                total_size += int(parts[2])
            except (ValueError, IndexError):
                pass
    
    return {
        "name":       lines[0] if lines else "Unknown",
        "file_count": file_count,
        "raw_output": info_out[:500] if info_out else "No details"
    }

def mega_account_info() -> dict:
    """Get MEGA account storage and file statistics."""
    # Get quota info
    quota_out, _, _ = run_cmd(["mega-quota"], timeout=CMD_TIMEOUT)
    
    # Count files and folders
    files_out, _, fcode = run_cmd(["mega-find", "/", "--type=f"], timeout=CMD_TIMEOUT * 2)
    dirs_out,  _, dcode = run_cmd(["mega-find", "/", "--type=d"], timeout=CMD_TIMEOUT * 2)
    
    file_list   = [l for l in files_out.strip().split('\n') if l.strip()] if fcode == 0 else []
    folder_list = [l for l in dirs_out.strip().split('\n')  if l.strip()] if dcode == 0 else []
    
    return {
        "quota_raw":    quota_out.strip(),
        "file_count":   len(file_list),
        "folder_count": len(folder_list),
    }

# ==========================================
# Rename Pattern Logic
# ==========================================

def build_new_name(old_name: str, pattern: str, replacement: str, index: int) -> str:
    name, ext = posixpath.splitext(old_name)
    if pattern == "prefix":
        return f"{replacement}{old_name}"
    elif pattern == "suffix":
        return f"{name}{replacement}{ext}"
    elif pattern == "replace":
        parts = replacement.split("|", 1)
        if len(parts) == 2:
            return old_name.replace(parts[0], parts[1])
    elif pattern == "regex":
        parts = replacement.split("|", 1)
        if len(parts) == 2:
            try:
                return re.sub(parts[0], parts[1], old_name)
            except re.error:
                pass
    elif pattern == "template":
        return replacement.replace("{n}", name).replace("{i}", str(index)).replace("{ext}", ext)
    elif pattern == "number":
        return f"{str(index).zfill(5)}{ext}"
    return old_name

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
        "Advanced & Crash-Free Engine. Database Connected! 💾\n\n"
        "📌 *Commands:*\n"
        "  `/login email password` — Mega.nz login\n"
        "  `/renameall` — Start Rename Process\n"
        "  `/prefix` — Add Prefix to files\n"
        "  `/suffix` — Add Suffix to files\n"
        "  `/replace` — Replace text in names\n"
        "  `/check <link>` — Check MEGA link details\n"
        "  `/megainfo` — Account storage & file stats\n"
        "  `/stats` — Database Stats & Quota\n"
        "  `/premium` — Check premium status\n"
        "  `/lang` — Change language\n"
        "  `/help` — Help & support\n\n"
        "👑 *Owner Commands:*\n"
        "  `/auth user_id` — Authorize user\n"
        "  `/unauth user_id` — Remove authorization\n"
        "  `/authlist` — View authorized users\n"
        "  `/broadcast message` — Message all users"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==========================================
# /login
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
    msg = await update.message.reply_text("🔄 MegaCMD Engine se login ho raha hai...")

    try:
        await asyncio.wait_for(
            asyncio.to_thread(mega_login, email, password),
            timeout=CMD_TIMEOUT
        )
        user_sessions[uid] = {"email": email}
        await db.save_session(uid, email)
        await msg.edit_text(
            f"✅ *Login Successful!*\n📧 `{email}`\n\nAb `/renameall` use karein.",
            parse_mode="Markdown"
        )
    except asyncio.TimeoutError:
        await msg.edit_text("⏱️ Login timed out. Please try again.")
    except Exception as e:
        await msg.edit_text(f"❌ Login Failed!\nError: `{e}`", parse_mode="Markdown")

# ==========================================
# /stats
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

    session = await db.get_session(uid)
    email = session.get("email", "Not logged in") if session else "Not logged in"

    stats_text = (
        f"📊 *User Database Stats*\n\n"
        f"👤 UID: `{user_data['_id']}`\n"
        f"📧 Email: `{email}`\n"
        f"🔄 Lifetime Renamed: `{user_data['lifetime_renamed']}`\n"
        f"🔗 Links Checked: `{user_data.get('links_checked', 0)}`\n"
        f"⚡ Daily Limit Remaining: `{user_data['daily_limit']}`\n"
        f"👑 Premium: `{'Yes ✨' if user_data['is_premium'] else 'No'}`"
    )
    await update.message.reply_text(stats_text, parse_mode="Markdown")

# ==========================================
# /check  — Check MEGA link details
# ==========================================

async def check_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not await check_auth(uid):
        await send_unauthorized(update)
        return

    if not ctx.args:
        await update.message.reply_text(
            "🔗 *Check MEGA Link*\n\nUsage: `/check <mega_link>`\n\n"
            "Example:\n`/check https://mega.nz/file/XXXXX`",
            parse_mode="Markdown"
        )
        return

    link = ctx.args[0]
    msg  = await update.message.reply_text("🔍 MEGA link check ho raha hai...")

    try:
        info = await asyncio.wait_for(
            asyncio.to_thread(mega_check_link, link),
            timeout=CMD_TIMEOUT
        )
        text = (
            f"✅ *MEGA Link Details*\n\n"
            f"📄 *Contents:*\n`{info['name']}`\n\n"
            f"🗂️ *Items Found:* `{info['file_count']}`\n\n"
            f"📋 *Raw Info:*\n```\n{info['raw_output'][:400]}\n```"
        )
        await msg.edit_text(text, parse_mode="Markdown")
        await db.add_user(uid)
        await db.increment_links_checked(uid)
    except asyncio.TimeoutError:
        await msg.edit_text("⏱️ Request timed out. Please try again.")
    except Exception as e:
        await msg.edit_text(f"❌ Link check failed!\nError: `{e}`", parse_mode="Markdown")

# ==========================================
# /megainfo  — Account storage & file stats
# ==========================================

async def megainfo_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not await check_auth(uid):
        await send_unauthorized(update)
        return

    session = user_sessions.get(uid) or await db.get_session(uid)
    if not session:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown")
        return

    msg = await update.message.reply_text("📊 Aapka MEGA account info fetch ho raha hai...")

    try:
        info = await asyncio.wait_for(
            asyncio.to_thread(mega_account_info),
            timeout=CMD_TIMEOUT * 3
        )
        text = (
            f"☁️ *MEGA Account Info*\n\n"
            f"📧 *Email:* `{session.get('email', 'N/A')}`\n\n"
            f"📄 *Total Files:*   `{info['file_count']:,}`\n"
            f"📁 *Total Folders:* `{info['folder_count']:,}`\n\n"
            f"💾 *Storage Quota:*\n```\n{info['quota_raw'][:400] or 'N/A'}\n```"
        )
        await msg.edit_text(text, parse_mode="Markdown")
    except asyncio.TimeoutError:
        await msg.edit_text("⏱️ Request timed out. Please try again.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: `{e}`", parse_mode="Markdown")

# ==========================================
# /renameall
# ==========================================

async def renameall_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not await check_auth(uid):
        await send_unauthorized(update)
        return

    session = user_sessions.get(uid) or await db.get_session(uid)
    if not session:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown")
        return
    user_sessions[uid] = session  # restore cache

    keyboard = [
        [InlineKeyboardButton("🔤 Add Prefix",         callback_data="pattern_prefix")],
        [InlineKeyboardButton("🔡 Add Suffix",         callback_data="pattern_suffix")],
        [InlineKeyboardButton("🔄 Replace Text",       callback_data="pattern_replace")],
        [InlineKeyboardButton("🔢 Sequential Number",  callback_data="pattern_number")],
    ]
    await update.message.reply_text(
        "🎯 *Rename Pattern Select Karein:*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# Convenience shortcuts
async def prefix_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return
    session = user_sessions.get(uid) or await db.get_session(uid)
    if not session:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown"); return
    user_sessions[uid] = session
    ctx.user_data["rename_pattern"] = "prefix"
    ctx.user_data["awaiting_input"] = True
    await update.message.reply_text("✏️ Prefix text type karke bhejein:")

async def suffix_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return
    session = user_sessions.get(uid) or await db.get_session(uid)
    if not session:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown"); return
    user_sessions[uid] = session
    ctx.user_data["rename_pattern"] = "suffix"
    ctx.user_data["awaiting_input"] = True
    await update.message.reply_text("✏️ Suffix text type karke bhejein:")

async def replace_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return
    session = user_sessions.get(uid) or await db.get_session(uid)
    if not session:
        await update.message.reply_text("❌ Pehle `/login` karein.", parse_mode="Markdown"); return
    user_sessions[uid] = session
    ctx.user_data["rename_pattern"] = "replace"
    ctx.user_data["awaiting_input"] = True
    await update.message.reply_text(
        "🔄 Format: `purana_text|naya_text`\n\nExample: `OldName|NewName`",
        parse_mode="Markdown"
    )

# ==========================================
# /premium
# ==========================================

async def premium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return

    user_data = await db.get_user(uid)
    if not user_data:
        await update.message.reply_text("Pehle /start karein."); return

    is_prem = user_data.get("is_premium", False)
    text = (
        f"⭐ *Premium Status*\n\n"
        f"{'✅ Aapke paas Premium access hai!' if is_prem else '❌ Aapke paas Premium nahi hai.'}\n\n"
        f"{'🎉 Unlimited daily renames enjoy karein!' if is_prem else f'Premium ke liye contact karein: {OWNER_USERNAME}'}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# ==========================================
# /lang
# ==========================================

async def lang_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return

    keyboard = [
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
         InlineKeyboardButton("🇮🇳 Hindi",   callback_data="lang_hi")],
        [InlineKeyboardButton("🇸🇦 Arabic",  callback_data="lang_ar"),
         InlineKeyboardButton("🇪🇸 Spanish", callback_data="lang_es")],
    ]
    await update.message.reply_text("🌐 Apni language choose karein:", reply_markup=InlineKeyboardMarkup(keyboard))

# ==========================================
# /help
# ==========================================

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await check_auth(uid):
        await send_unauthorized(update); return

    await update.message.reply_text(
        f"❓ *Help & Support*\n\n"
        f"*How to use:*\n"
        f"1️⃣ `/login email password`\n"
        f"2️⃣ `/renameall` — pattern choose karein\n"
        f"3️⃣ `/check <link>` — koi bhi MEGA link inspect karein\n"
        f"4️⃣ `/megainfo` — storage & file stats\n"
        f"5️⃣ `/stats` — apni usage stats\n\n"
        f"⚡ Supports up to *{MAX_FILES:,}* files!\n\n"
        f"📩 Need help? Contact {OWNER_USERNAME}",
        parse_mode="Markdown"
    )

# ==========================================
# Owner: /auth /unauth /authlist /broadcast /setpremium /resetlimit
# ==========================================

async def auth_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Sirf owner yeh command use kar sakta hai.")
        return
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
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Sirf owner yeh command use kar sakta hai.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/unauth user_id`", parse_mode="Markdown"); return
    try:
        uid = int(ctx.args[0])
        await db.remove_auth(uid)
        await update.message.reply_text(f"✅ User `{uid}` ka authorization hata diya.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def authlist_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Sirf owner yeh command use kar sakta hai.")
        return
    users = await db.get_auth_list()
    if not users:
        await update.message.reply_text("📋 Abhi koi authorized user nahi hai.")
        return
    text = "📋 *Authorized Users:*\n\n" + "\n".join(f"• `{uid}`" for uid in users)
    await update.message.reply_text(text, parse_mode="Markdown")

async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Sirf owner yeh command use kar sakta hai.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/broadcast message`", parse_mode="Markdown"); return

    msg_text = " ".join(ctx.args)
    all_users = await db.get_all_users()
    status_msg = await update.message.reply_text(f"📢 Broadcasting to {len(all_users)} users...")
    sent = failed = 0

    for uid in all_users:
        try:
            await ctx.bot.send_message(uid, f"📢 *Broadcast:*\n\n{msg_text}", parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1

    await status_msg.edit_text(f"✅ Broadcast complete!\n✉️ Sent: {sent} | ❌ Failed: {failed}")

async def setpremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Sirf owner."); return
    if not ctx.args:
        await update.message.reply_text("Usage: `/setpremium user_id`", parse_mode="Markdown"); return
    try:
        uid = int(ctx.args[0])
        await db.set_premium(uid, True)
        await db.reset_daily_limit(uid, 99999)
        await update.message.reply_text(f"⭐ User `{uid}` ko Premium de diya!", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

# ==========================================
# Button handler
# ==========================================

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid  = query.from_user.id

    if not await check_auth(uid):
        await query.edit_message_text(
            f"🚫 Access Denied!\n\nOwner se contact karein: {OWNER_USERNAME}"
        )
        return

    if data.startswith("pattern_"):
        pattern = data.replace("pattern_", "")
        ctx.user_data["rename_pattern"] = pattern

        if pattern == "number":
            ctx.user_data["rename_replacement"] = ""
            keyboard = [[InlineKeyboardButton("✅ Start", callback_data="confirm_rename")]]
            await query.edit_message_text(
                "🔢 Sabhi files `00001.ext` pattern me rename hongi.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            hint = {
                "prefix":  "prefix text (e.g. `AYUPRIME `)",
                "suffix":  "suffix text (e.g. ` HD`)",
                "replace": "format `purana|naya` (e.g. `OldName|NewName`)",
            }.get(pattern, "text")
            await query.edit_message_text(f"✏️ Ab apna {hint} type karke bhejein:")
            ctx.user_data["awaiting_input"] = True

    elif data == "confirm_rename":
        await query.edit_message_text("🚀 Rename Processing Started...")
        asyncio.create_task(do_bulk_rename(query.message, uid, ctx))

    elif data.startswith("lang_"):
        lang_code = data.split("_")[1]
        await db.set_language(uid, lang_code)
        await query.edit_message_text(f"✅ Language `{lang_code}` set kar di!", parse_mode="Markdown")

# ==========================================
# Message handler
# ==========================================

async def message_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not await check_auth(uid):
        await send_unauthorized(update)
        return

    if not ctx.user_data.get("awaiting_input"):
        return

    text = update.message.text.strip()
    ctx.user_data["rename_replacement"] = text
    ctx.user_data["awaiting_input"]     = False
    pattern = ctx.user_data.get("rename_pattern", "")

    example_new = build_new_name("Example_File.mp4", pattern, text, 1)
    keyboard    = [[InlineKeyboardButton("✅ Start Renaming!", callback_data="confirm_rename")]]
    await update.message.reply_text(
        f"👁 *Preview:*\n\n📄 Naya Naam: `{example_new}`\n\nConfirm?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# ==========================================
# Core bulk rename — handles 5k-10k files
# ==========================================

async def do_bulk_rename(message, uid: int, ctx: ContextTypes.DEFAULT_TYPE):
    pattern     = ctx.user_data.get("rename_pattern", "prefix")
    replacement = ctx.user_data.get("rename_replacement", "")

    user_data = await db.get_user(uid)
    if not user_data:
        await message.reply_text("❌ Pehle /start karein.")
        return

    if user_data['daily_limit'] <= 0 and not user_data.get('is_premium'):
        await message.reply_text(
            f"❌ Aapki daily limit khatam ho chuki hai.\n"
            f"Premium ke liye contact karein: {OWNER_USERNAME}"
        )
        return

    try:
        # Step 1: Fetch all files with timeout
        fetch_msg = await message.reply_text("⏳ Files fetch ho rahi hain...")
        try:
            files = await asyncio.wait_for(
                asyncio.to_thread(mega_get_all_files),
                timeout=CMD_TIMEOUT * 3
            )
        except asyncio.TimeoutError:
            await fetch_msg.edit_text("⏱️ Files fetch timed out. Please try again.")
            return

        total = len(files)
        if total == 0:
            await fetch_msg.edit_text("📂 Koi file nahi mili.")
            return

        # Apply daily limit cap for non-premium
        max_allowed = user_data['daily_limit'] if not user_data.get('is_premium') else MAX_FILES
        files = files[:max_allowed]

        await fetch_msg.edit_text(
            f"🔄 *Renaming Started!*\n"
            f"📊 Total Files: `{total:,}` | Processing: `{len(files):,}`\n"
            f"_(Batches of {BATCH_SIZE} — progress updates har 5 batch pe)_",
            parse_mode="Markdown"
        )

        done = failed = skipped = 0
        batches = [files[i:i+BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]
        total_batches = len(batches)

        for batch_num, batch in enumerate(batches, 1):
            for idx_in_batch, file_path in enumerate(batch):
                global_idx = (batch_num - 1) * BATCH_SIZE + idx_in_batch + 1
                old_name   = posixpath.basename(file_path)
                new_name   = build_new_name(old_name, pattern, replacement, global_idx)

                if new_name == old_name:
                    skipped += 1
                    continue

                parent   = posixpath.dirname(file_path)
                new_path = f"{parent}/{new_name}" if parent not in ["", "/"] else f"/{new_name}"

                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(mega_rename, file_path, new_path),
                        timeout=CMD_TIMEOUT
                    )
                    done += 1
                except asyncio.TimeoutError:
                    failed += 1
                    logger.warning(f"Rename timeout: {file_path}")
                except Exception as e:
                    failed += 1
                    logger.warning(f"Rename failed {file_path}: {e}")

            # Progress update every 5 batches or last batch
            if batch_num % 5 == 0 or batch_num == total_batches:
                pct      = round(batch_num / total_batches * 100)
                bar      = "█" * (pct // 10) + "░" * (10 - pct // 10)
                total_processed = done + failed + skipped
                try:
                    await message.reply_text(
                        f"⏳ Progress: `{bar}` {pct}%\n"
                        f"✅ Renamed: `{done}` | ❌ Failed: `{failed}` | ⏭️ Skipped: `{skipped}`\n"
                        f"📊 Processed: `{total_processed}/{len(files)}`",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass

        # Final update
        await db.update_rename_stats(uid, done)
        await message.reply_text(
            f"🎉 *Success!*\n\n"
            f"✅ Renamed:  `{done}` files\n"
            f"❌ Failed:   `{failed}` files\n"
            f"⏭️ Skipped:  `{skipped}` files\n"
            f"📊 Total:    `{len(files)}` files\n\n"
            f"💾 Database updated! `/stats` check karein.",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"do_bulk_rename error uid={uid}: {e}")
        await message.reply_text(f"❌ Error: `{e}`", parse_mode="Markdown")

# ==========================================
# Koyeb Health Check Server
# ==========================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")
    def log_message(self, format, *args):
        pass

def start_health_server():
    port = int(os.getenv("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()

# ==========================================
# Main
# ==========================================

if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).build()

    # Core commands
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("login",       login_cmd))
    app.add_handler(CommandHandler("stats",       stats_cmd))
    app.add_handler(CommandHandler("renameall",   renameall_cmd))
    app.add_handler(CommandHandler("prefix",      prefix_cmd))
    app.add_handler(CommandHandler("suffix",      suffix_cmd))
    app.add_handler(CommandHandler("replace",     replace_cmd))
    app.add_handler(CommandHandler("check",       check_cmd))
    app.add_handler(CommandHandler("megainfo",    megainfo_cmd))
    app.add_handler(CommandHandler("premium",     premium_cmd))
    app.add_handler(CommandHandler("lang",        lang_cmd))
    app.add_handler(CommandHandler("help",        help_cmd))

    # Owner commands
    app.add_handler(CommandHandler("auth",        auth_cmd))
    app.add_handler(CommandHandler("unauth",      unauth_cmd))
    app.add_handler(CommandHandler("authlist",    authlist_cmd))
    app.add_handler(CommandHandler("broadcast",   broadcast_cmd))
    app.add_handler(CommandHandler("setpremium",  setpremium_cmd))

    # Inline buttons & text input
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    print("🤖 Pro Mega Bot Backend is Running with Health Checks...")
    app.run_polling(drop_pending_updates=True)
