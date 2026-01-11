import os
import asyncio
import random
import string
import re
from datetime import datetime, timedelta
from telethon import TelegramClient, Button, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    ChannelPrivateError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    MessageNotModifiedError
)
from telethon.tl.functions.messages import ForwardMessagesRequest
from telethon.tl.types import Channel, Chat, User, InputPeerChannel, InputPeerChat
from cryptography.fernet import Fernet
from pymongo import MongoClient
import time

# Config - Using environment variables (fallback to hardcoded for local testing)
CONFIG = {
    'api_id': int(os.getenv('TELEGRAM_API_ID', '23131964')),
    'api_hash': os.getenv('TELEGRAM_API_HASH', '1f383b963dd342881edce03bd1e686a5'),
    'bot_token': os.getenv('BOT_TOKEN', '7299749946:AAGRCJKh08FZ_xVPk6ZoUk3t_721_VSicDw'),
    'owner_id': int(os.getenv('OWNER_ID', '7089574265')),
    'access_password': os.getenv('ACCESS_PASSWORD', 'ADSREACHOP'),
    'mongo_uri': os.getenv(
        'MONGO_URI',
        'mongodb+srv://dbotp60_db_user:ARSHTYAGI@cluster0.4b8xz3a.mongodb.net/?appName=Cluster0'
    ),
    'db_name': os.getenv('MONGO_DB_NAME', 'ads_bot_db'),
    'logger_bot_token': os.getenv('LOGGER_BOT_TOKEN', '8572068771:AAEq5WVx5G8NadqP0JWfIwWAJyoG3JnEn3I'),
    'logger_bot_username': os.getenv('LOGGER_BOT_USERNAME', 'Logsadsreachbot'),
    'auto_reply': os.getenv('AUTO_REPLY', 'Check @Axcneog'),
    'reply_cooldown': int(os.getenv('REPLY_COOLDOWN', '120')),
}

# Encryption key
if not os.path.exists('encryption.key'):
    key = Fernet.generate_key().decode()
    with open('encryption.key', 'w') as f:
        f.write(key)
else:
    with open('encryption.key', 'r') as f:
        key = f.read().strip()
cipher_suite = Fernet(key.encode())

# MongoDB setup
mongo_client = MongoClient(CONFIG['mongo_uri'])
db = mongo_client[CONFIG['db_name']]

# Collections
users_col = db['users']
accounts_col = db['accounts']
account_topics_col = db['account_topics']
account_settings_col = db['account_settings']
account_stats_col = db['account_stats']
account_auto_groups_col = db['account_auto_groups']
account_failed_groups_col = db['account_failed_groups']
account_flood_waits_col = db['account_flood_waits']
logger_tokens_col = db['logger_tokens']

# Topics
TOPICS = ['instagram', 'exchange', 'twitter', 'telegram', 'minecraft', 'tiktok', 'youtube', 'whatsapp', 'other']

# Telegram clients
# Use in-memory sessions to avoid SQLite `.session` file locking issues ("database is locked").
# Bots authenticate via token each run, so persistence is not required.
main_bot = TelegramClient(StringSession(), CONFIG['api_id'], CONFIG['api_hash'])
logger_bot = TelegramClient(StringSession(), CONFIG['api_id'], CONFIG['api_hash'])

# Global state
user_states = {}
forwarding_tasks = {}
auto_reply_clients = {}
last_replied = {}

ACCOUNTS_PER_PAGE = 7

# ==================== HELPER FUNCTIONS ====================

def is_admin(user_id):
    return int(user_id) == int(CONFIG['owner_id'])

def is_approved(user_id):
    if is_admin(user_id):
        return True
    user = users_col.find_one({'user_id': int(user_id)})
    return user and user.get('approved', False)

def approve_user(user_id):
    users_col.update_one(
        {'user_id': int(user_id)},
        {'$set': {'user_id': int(user_id), 'approved': True, 'approved_at': datetime.now()}},
        upsert=True
    )

def get_user_accounts(user_id):
    return list(accounts_col.find({'owner_id': user_id}).sort('added_at', 1))

def get_account_by_id(account_id):
    from bson.objectid import ObjectId
    try:
        return accounts_col.find_one({'_id': ObjectId(account_id)})
    except:
        return None

def get_account_by_index(user_id, index):
    accounts = get_user_accounts(user_id)
    if 0 < index <= len(accounts):
        return accounts[index - 1]
    return None

def get_account_settings(account_id):
    settings = account_settings_col.find_one({'account_id': account_id})
    if not settings:
        settings = {
            'account_id': account_id,
            'group_delay': 90,
            'msg_delay': 30,
            'round_delay': 3600,
            'auto_reply': CONFIG['auto_reply'],
            'reply_cooldown': 300,
            'logs_chat_id': None
        }
        account_settings_col.insert_one(settings)
    return settings

def update_account_settings(account_id, updates):
    account_settings_col.update_one(
        {'account_id': account_id},
        {'$set': updates},
        upsert=True
    )

def get_account_stats(account_id):
    stats = account_stats_col.find_one({'account_id': account_id})
    if not stats:
        stats = {'account_id': account_id, 'total_sent': 0, 'total_failed': 0, 'last_forward': None}
        account_stats_col.insert_one(stats)
    return stats

def update_account_stats(account_id, sent=0, failed=0):
    account_stats_col.update_one(
        {'account_id': account_id},
        {'$inc': {'total_sent': sent, 'total_failed': failed}, '$set': {'last_forward': datetime.now()}},
        upsert=True
    )

def is_group_failed(account_id, group_key):
    failed = account_failed_groups_col.find_one({'account_id': account_id, 'group_key': group_key})
    return failed is not None

def mark_group_failed(account_id, group_key, error):
    account_failed_groups_col.update_one(
        {'account_id': account_id, 'group_key': group_key},
        {'$set': {'error': str(error)[:200], 'failed_at': datetime.now()}},
        upsert=True
    )

def clear_failed_groups(account_id):
    account_failed_groups_col.delete_many({'account_id': account_id})

def get_flood_wait(account_id, group_key):
    """Check if group has active flood wait, return remaining seconds or 0"""
    doc = account_flood_waits_col.find_one({'account_id': account_id, 'group_key': group_key})
    if doc:
        wait_until = doc.get('wait_until')
        if wait_until and wait_until > datetime.now():
            remaining = (wait_until - datetime.now()).total_seconds()
            return int(remaining)
        else:
            account_flood_waits_col.delete_one({'account_id': account_id, 'group_key': group_key})
    return 0

def set_flood_wait(account_id, group_key, group_name, seconds):
    """Store flood wait for a group"""
    wait_until = datetime.now() + timedelta(seconds=seconds)
    account_flood_waits_col.update_one(
        {'account_id': account_id, 'group_key': group_key},
        {'$set': {
            'group_name': group_name,
            'wait_seconds': seconds,
            'wait_until': wait_until,
            'created_at': datetime.now()
        }},
        upsert=True
    )

def clear_flood_waits(account_id):
    """Clear all flood waits for an account"""
    account_flood_waits_col.delete_many({'account_id': account_id})

def get_active_flood_waits(account_id):
    """Get count of active flood waits"""
    now = datetime.now()
    return account_flood_waits_col.count_documents({
        'account_id': account_id,
        'wait_until': {'$gt': now}
    })

def generate_token(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def parse_link(link):
    topic_id = None
    match = re.search(r'/(\d+)$', link)
    if match:
        topic_id = int(match.group(1))
    base = re.sub(r'/\d+$', '', link).rstrip('/')
    if '/c/' in base:
        cid = base.split('/c/')[-1]
        peer = int('-100' + cid)
        url = f"https://t.me/c/{cid}"
    else:
        username = base.split('t.me/')[-1]
        peer = username
        url = f"https://t.me/{username}"
    return peer, url, topic_id

async def send_log(account_id, message):
    try:
        settings = get_account_settings(account_id)
        chat_id = settings.get('logs_chat_id')
        if chat_id and CONFIG['logger_bot_token']:
            await logger_bot.send_message(chat_id, message)
    except Exception as e:
        print(f"Log error: {e}")

async def forward_message(client, to_entity, msg_id, from_peer, topic_id=None):
    random_id = random.randint(1, 2147483647)
    await client(ForwardMessagesRequest(
        from_peer=from_peer,
        id=[msg_id],
        random_id=[random_id],
        to_peer=to_entity,
        top_msg_id=topic_id
    ))

async def fetch_groups(client, account_id, phone):
    try:
        dialogs = await client.get_dialogs(limit=None)
        groups = []
        for d in dialogs:
            e = d.entity
            if isinstance(e, User):
                continue
            if not isinstance(e, (Channel, Chat)):
                continue
            if isinstance(e, Channel) and e.broadcast:
                continue
            title = getattr(e, 'title', 'Unknown')
            if title and title != 'Unknown':
                group_id = e.id
                access_hash = getattr(e, 'access_hash', None)
                username = getattr(e, 'username', None)
                is_channel = isinstance(e, Channel)
                
                if access_hash is None and is_channel:
                    try:
                        full_entity = await client.get_entity(e)
                        access_hash = getattr(full_entity, 'access_hash', None)
                    except:
                        pass
                
                groups.append({
                    'account_id': account_id,
                    'phone': phone,
                    'group_id': group_id,
                    'title': title,
                    'username': username,
                    'access_hash': access_hash,
                    'is_channel': is_channel,
                    'added_at': datetime.now()
                })
        if groups:
            account_auto_groups_col.delete_many({'account_id': account_id})
            account_auto_groups_col.insert_many(groups)
        return len(groups)
    except Exception as e:
        print(f"Fetch groups error: {e}")
        return 0

# ==================== KEYBOARDS ====================

def dashboard_keyboard(user_id, page=0):
    accounts = get_user_accounts(user_id)
    total = len(accounts)
    pages = max(1, (total + ACCOUNTS_PER_PAGE - 1) // ACCOUNTS_PER_PAGE)
    
    start = page * ACCOUNTS_PER_PAGE
    end = min(start + ACCOUNTS_PER_PAGE, total)
    page_accounts = accounts[start:end]
    
    buttons = []
    for i, acc in enumerate(page_accounts):
        idx = start + i + 1
        phone = acc['phone']
        name = acc.get('name', 'Unknown')[:12]
        status = "🟢" if acc.get('is_forwarding') else "🔴"
        buttons.append([Button.inline(f"{status} #{idx} {phone[-4:]} - {name}", f"acc_{acc['_id']}")])
    
    nav = []
    if page > 0:
        nav.append(Button.inline("◀️ Prev", f"page_{page-1}"))
    if page < pages - 1:
        nav.append(Button.inline("Next ▶️", f"page_{page+1}"))
    if nav:
        buttons.append(nav)
    
    buttons.append([Button.inline("➕ Host Account", b"host")])
    if is_admin(user_id):
        buttons.append([Button.inline("👑 Admin", b"admin")])
    
    return buttons

def account_menu_keyboard(account_id, acc):
    fwd = acc.get('is_forwarding', False)
    btn = "⛔ Stop" if fwd else "▶️ Start"
    data = f"stop_{account_id}" if fwd else f"fwd_select_{account_id}"
    
    return [
        [Button.inline("📂 Topics", f"topics_{account_id}"), Button.inline("⚙️ Settings", f"settings_{account_id}")],
        [Button.inline("📊 Stats", f"stats_{account_id}"), Button.inline("🔄 Refresh", f"refresh_{account_id}")],
        [Button.inline(btn, data)],
        [Button.inline("📝 Logs", f"logs_{account_id}"), Button.inline("🗑️ Delete", f"delete_{account_id}")],
        [Button.inline("⬅️ Dashboard", b"dashboard")]
    ]

def topics_menu_keyboard(account_id):
    buttons = []
    for t in TOPICS:
        count = account_topics_col.count_documents({'account_id': account_id, 'topic': t})
        buttons.append([Button.inline(f"📁 {t.capitalize()} ({count})", f"topic_{account_id}_{t}")])
    auto = account_auto_groups_col.count_documents({'account_id': account_id})
    buttons.append([Button.inline(f"🤖 Auto Groups ({auto})", f"auto_{account_id}")])
    buttons.append([Button.inline("⬅️ Back", f"acc_{account_id}")])
    return buttons

def forwarding_select_keyboard(account_id):
    buttons = []
    for t in TOPICS:
        count = account_topics_col.count_documents({'account_id': account_id, 'topic': t})
        if count > 0:
            buttons.append([Button.inline(f"📁 {t.capitalize()} ({count})", f"startfwd_{account_id}_{t}")])
    buttons.append([Button.inline("🤖 All Groups Only", f"startfwd_{account_id}_all")])
    buttons.append([Button.inline("⬅️ Cancel", f"acc_{account_id}")])
    return buttons

def settings_keyboard(account_id):
    return [
        [Button.inline("⏱️ Msg Delay", f"setmsg_{account_id}"), Button.inline("⏱️ Group Delay", f"setgrp_{account_id}")],
        [Button.inline("⏱️ Round Delay", f"setround_{account_id}")],
        [Button.inline("💬 Auto-Reply", f"setreply_{account_id}")],
        [Button.inline("🔄 Clear Failed", f"clearfailed_{account_id}")],
        [Button.inline("⬅️ Back", f"acc_{account_id}")]
    ]

def otp_keyboard():
    return [
        [Button.inline("1", b"otp_1"), Button.inline("2", b"otp_2"), Button.inline("3", b"otp_3")],
        [Button.inline("4", b"otp_4"), Button.inline("5", b"otp_5"), Button.inline("6", b"otp_6")],
        [Button.inline("7", b"otp_7"), Button.inline("8", b"otp_8"), Button.inline("9", b"otp_9")],
        [Button.inline("⌫", b"otp_back"), Button.inline("0", b"otp_0"), Button.inline("❌", b"otp_cancel")],
        [Button.url("📱 Get Code", "tg://openmessage?user_id=777000")]
    ]

# ==================== COMMANDS ====================

@main_bot.on(events.NewMessage(pattern='/start$'))
async def cmd_start(event):
    uid = event.sender_id
    
    if is_admin(uid):
        approve_user(uid)
    
    if not is_approved(uid):
        await event.respond(
            "🔐 **Welcome to Ads Bot**\n\n"
            "Enter password to access:\n"
            "`/access <password>`\n\n"
            "Commands: /help"
        )
        return
    
    accounts = get_user_accounts(uid)
    active = sum(1 for a in accounts if a.get('is_forwarding'))
    
    await event.respond(
        f"📊 **Dashboard**\n\n"
        f"👤 Accounts: {len(accounts)}\n"
        f"🟢 Active: {active} | 🔴 Inactive: {len(accounts) - active}",
        buttons=dashboard_keyboard(uid)
    )

@main_bot.on(events.NewMessage(pattern='/access (.+)'))
async def cmd_access(event):
    uid = event.sender_id
    pwd = event.pattern_match.group(1).strip()
    
    if pwd == CONFIG['access_password'] or is_admin(uid):
        approve_user(uid)
        await event.respond("✅ Access granted!", buttons=dashboard_keyboard(uid))
    else:
        await event.respond("❌ Wrong password!")

@main_bot.on(events.NewMessage(pattern='/help'))
async def cmd_help(event):
    uid = event.sender_id
    text = "📚 **Commands**\n\n"
    text += "/start - Dashboard\n"
    text += "/access <pass> - Get access\n"
    text += "/add - Host new account\n"
    text += "/list - List accounts\n"
    text += "/start <n> - Start forwarding #n\n"
    text += "/stop <n> - Stop forwarding #n\n"
    text += "/stats <n> - Stats for #n\n"
    text += "/logout <n> - Delete account #n\n"
    text += "/help - This help"
    
    if is_admin(uid):
        text += "\n\n👑 **Admin**\n"
        text += "/clearusers - Revoke all access\n"
        text += "/users - List approved users"
    
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern='/clearusers'))
async def cmd_clearusers(event):
    uid = event.sender_id
    if not is_admin(uid):
        await event.respond(f"❌ Admin only! Your ID: `{uid}`\nOwner ID: `{CONFIG['owner_id']}`")
        return
    
    result = users_col.delete_many({'user_id': {'$ne': int(uid)}})
    approve_user(uid)
    
    await event.respond(f"✅ Cleared {result.deleted_count} users!\n\nOnly you have access now.")

@main_bot.on(events.NewMessage(pattern='/myid'))
async def cmd_myid(event):
    uid = event.sender_id
    is_owner = "✅ Yes" if is_admin(uid) else "❌ No"
    is_app = "✅ Yes" if is_approved(uid) else "❌ No"
    await event.respond(f"🆔 Your ID: `{uid}`\n👑 Admin: {is_owner}\n🔓 Approved: {is_app}")

@main_bot.on(events.NewMessage(pattern='/users'))
async def cmd_users(event):
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    users = list(users_col.find({'approved': True}))
    if not users:
        await event.respond("No approved users.")
        return
    
    text = "👤 **Approved Users**\n\n"
    for u in users:
        user_id = u.get('user_id')
        approved_at = u.get('approved_at', 'Unknown')
        if hasattr(approved_at, 'strftime'):
            approved_at = approved_at.strftime('%Y-%m-%d')
        is_owner = " 👑" if user_id == CONFIG['owner_id'] else ""
        text += f"• `{user_id}`{is_owner} - {approved_at}\n"
    
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern='/add'))
async def cmd_add(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        return
    
    user_states[uid] = {'action': 'phone'}
    await event.respond("📱 Send phone number with country code:\n\nExample: `+919876543210`")

@main_bot.on(events.NewMessage(pattern='/list'))
async def cmd_list(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        return
    
    accounts = get_user_accounts(uid)
    if not accounts:
        await event.respond("No accounts. Use /add")
        return
    
    text = "📱 **Your Accounts**\n\n"
    for i, acc in enumerate(accounts, 1):
        status = "🟢" if acc.get('is_forwarding') else "🔴"
        text += f"{status} #{i} - {acc['phone']} ({acc.get('name', 'Unknown')})\n"
    
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern=r'/start (\d+)'))
async def cmd_start_n(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        return
    
    n = int(event.pattern_match.group(1))
    acc = get_account_by_index(uid, n)
    
    if not acc:
        await event.respond(f"❌ Account #{n} not found!")
        return
    
    account_id = str(acc['_id'])
    
    topics = account_topics_col.count_documents({'account_id': account_id})
    groups = account_auto_groups_col.count_documents({'account_id': account_id})
    
    if topics == 0 and groups == 0:
        await event.respond("❌ No groups! Add topics or refresh groups first.")
        return
    
    await event.respond(
        f"▶️ **Start Forwarding #{n}**\n\nSelect where to forward:",
        buttons=forwarding_select_keyboard(account_id)
    )

@main_bot.on(events.NewMessage(pattern=r'/stop (\d+)'))
async def cmd_stop_n(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        return
    
    n = int(event.pattern_match.group(1))
    acc = get_account_by_index(uid, n)
    
    if not acc:
        await event.respond(f"❌ Account #{n} not found!")
        return
    
    account_id = str(acc['_id'])
    
    accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': False}})
    
    if account_id in forwarding_tasks:
        forwarding_tasks[account_id].cancel()
        del forwarding_tasks[account_id]
    
    if account_id in auto_reply_clients:
        try:
            await auto_reply_clients[account_id].disconnect()
        except:
            pass
        del auto_reply_clients[account_id]
    
    await event.respond(f"⛔ Stopped forwarding for #{n}")
    await send_log(account_id, f"⛔ Forwarding stopped for {acc['phone']}")

@main_bot.on(events.NewMessage(pattern=r'/stats (\d+)'))
async def cmd_stats_n(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        return
    
    n = int(event.pattern_match.group(1))
    acc = get_account_by_index(uid, n)
    
    if not acc:
        await event.respond(f"❌ Account #{n} not found!")
        return
    
    account_id = str(acc['_id'])
    stats = get_account_stats(account_id)
    failed = account_failed_groups_col.count_documents({'account_id': account_id})
    
    text = f"📊 **Stats #{n}** ({acc['phone']})\n\n"
    text += f"✅ Sent: {stats.get('total_sent', 0)}\n"
    text += f"❌ Failed: {stats.get('total_failed', 0)}\n"
    text += f"🚫 Skipped Groups: {failed}\n"
    
    last = stats.get('last_forward')
    text += f"🕐 Last: {last.strftime('%Y-%m-%d %H:%M') if last else 'Never'}"
    
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern=r'/logout (\d+)'))
async def cmd_logout_n(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        return
    
    n = int(event.pattern_match.group(1))
    acc = get_account_by_index(uid, n)
    
    if not acc:
        await event.respond(f"❌ Account #{n} not found!")
        return
    
    user_states[uid] = {'action': 'confirm_delete', 'account_id': str(acc['_id']), 'index': n}
    await event.respond(
        f"⚠️ Delete account #{n} ({acc['phone']})?\n\nType `YES` to confirm:"
    )

# ==================== CALLBACK HANDLERS ====================

@main_bot.on(events.CallbackQuery)
async def callback(event):
    uid = event.sender_id
    data = event.data.decode()
    
    if not is_approved(uid):
        await event.answer("Use /access first!", alert=True)
        return
    
    try:
        if data == "dashboard":
            accounts = get_user_accounts(uid)
            active = sum(1 for a in accounts if a.get('is_forwarding'))
            await event.edit(
                f"📊 **Dashboard**\n\n👤 Accounts: {len(accounts)}\n🟢 Active: {active} | 🔴 Inactive: {len(accounts) - active}",
                buttons=dashboard_keyboard(uid)
            )
        
        elif data.startswith("page_"):
            page = int(data.split("_")[1])
            accounts = get_user_accounts(uid)
            active = sum(1 for a in accounts if a.get('is_forwarding'))
            await event.edit(
                f"📊 **Dashboard** (Page {page+1})\n\n👤 Accounts: {len(accounts)}",
                buttons=dashboard_keyboard(uid, page)
            )
        
        elif data.startswith("acc_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            if not acc:
                await event.answer("Not found!", alert=True)
                return
            
            stats = get_account_stats(account_id)
            settings = get_account_settings(account_id)
            topics = account_topics_col.count_documents({'account_id': account_id})
            groups = account_auto_groups_col.count_documents({'account_id': account_id})
            failed = account_failed_groups_col.count_documents({'account_id': account_id})
            
            status = "🟢 Active" if acc.get('is_forwarding') else "🔴 Inactive"
            
            text = f"📱 **{acc['phone']}** ({acc.get('name', 'Unknown')})\n"
            text += f"Status: {status}\n\n"
            text += f"📁 Topics: {topics} | 🤖 Groups: {groups}\n"
            text += f"✅ Sent: {stats.get('total_sent', 0)} | ❌ Failed: {stats.get('total_failed', 0)}\n"
            text += f"🚫 Skipped: {failed}\n\n"
            text += f"⏱️ Delays: {settings.get('msg_delay', 30)}s / {settings.get('group_delay', 90)}s / {settings.get('round_delay', 3600)}s"
            
            await event.edit(text, buttons=account_menu_keyboard(account_id, acc))
        
        elif data.startswith("topics_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            await event.edit(f"📂 **Topics** - {acc['phone']}", buttons=topics_menu_keyboard(account_id))
        
        elif data.startswith("topic_"):
            parts = data.split("_")
            account_id, topic = parts[1], parts[2]
            
            links = list(account_topics_col.find({'account_id': account_id, 'topic': topic}))
            text = f"📁 **{topic.capitalize()}** ({len(links)} links)\n\n"
            
            for i, l in enumerate(links[:15], 1):
                text += f"{i}. {l['url']}\n"
            if len(links) > 15:
                text += f"...+{len(links)-15} more"
            
            if not links:
                text += "No links yet."
            
            await event.edit(text, buttons=[
                [Button.inline("➕ Add", f"add_{account_id}_{topic}"), Button.inline("🗑️ Clear", f"clear_{account_id}_{topic}")],
                [Button.inline("⬅️ Back", f"topics_{account_id}")]
            ])
        
        elif data.startswith("auto_"):
            account_id = data.split("_")[1]
            groups = list(account_auto_groups_col.find({'account_id': account_id}))
            
            text = f"🤖 **Auto Groups** ({len(groups)})\n\n"
            for i, g in enumerate(groups[:15], 1):
                u = f"@{g['username']}" if g.get('username') else "Private"
                text += f"{i}. {g['title'][:20]} ({u})\n"
            if len(groups) > 15:
                text += f"...+{len(groups)-15} more"
            
            await event.edit(text, buttons=[[Button.inline("⬅️ Back", f"topics_{account_id}")]])
        
        elif data.startswith("add_"):
            parts = data.split("_")
            account_id, topic = parts[1], parts[2]
            user_states[uid] = {'action': 'add_links', 'account_id': account_id, 'topic': topic}
            await event.respond(f"📎 Send links for **{topic}** (one per line):")
        
        elif data.startswith("clear_"):
            parts = data.split("_")
            account_id, topic = parts[1], parts[2]
            result = account_topics_col.delete_many({'account_id': account_id, 'topic': topic})
            await event.answer(f"Deleted {result.deleted_count} links!")
        
        elif data.startswith("settings_"):
            account_id = data.split("_")[1]
            settings = get_account_settings(account_id)
            
            text = "⚙️ **Settings**\n\n"
            text += f"⏱️ Message Delay: {settings.get('msg_delay', 30)}s\n"
            text += f"⏱️ Group Delay: {settings.get('group_delay', 90)}s (every 10 msgs)\n"
            text += f"⏱️ Round Delay: {settings.get('round_delay', 3600)}s\n"
            text += f"💬 Auto-Reply: {settings.get('auto_reply', 'Default')[:40]}..."
            
            failed = account_failed_groups_col.count_documents({'account_id': account_id})
            text += f"\n🚫 Failed Groups: {failed}"
            
            await event.edit(text, buttons=settings_keyboard(account_id))
        
        elif data.startswith("setmsg_"):
            account_id = data.split("_")[1]
            user_states[uid] = {'action': 'set_msg_delay', 'account_id': account_id}
            await event.respond("⏱️ Enter message delay (5-300 seconds):")
        
        elif data.startswith("setgrp_"):
            account_id = data.split("_")[1]
            user_states[uid] = {'action': 'set_grp_delay', 'account_id': account_id}
            await event.respond("⏱️ Enter group delay (10-600 seconds):")
        
        elif data.startswith("setround_"):
            account_id = data.split("_")[1]
            user_states[uid] = {'action': 'set_round_delay', 'account_id': account_id}
            await event.respond("⏱️ Enter round delay (60-86400 seconds):")
        
        elif data.startswith("setreply_"):
            account_id = data.split("_")[1]
            user_states[uid] = {'action': 'set_reply', 'account_id': account_id}
            await event.respond("💬 Send new auto-reply message:")
        
        elif data.startswith("clearfailed_"):
            account_id = data.split("_")[1]
            clear_failed_groups(account_id)
            await event.answer("Cleared failed groups!")
        
        elif data.startswith("stats_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            stats = get_account_stats(account_id)
            failed = account_failed_groups_col.count_documents({'account_id': account_id})
            
            text = f"📊 **Stats** - {acc['phone']}\n\n"
            text += f"✅ Sent: {stats.get('total_sent', 0)}\n"
            text += f"❌ Failed: {stats.get('total_failed', 0)}\n"
            text += f"🚫 Skipped: {failed}\n"
            
            last = stats.get('last_forward')
            text += f"🕐 Last: {last.strftime('%Y-%m-%d %H:%M') if last else 'Never'}"
            
            await event.edit(text, buttons=[
                [Button.inline("🔄 Reset", f"reset_{account_id}")],
                [Button.inline("⬅️ Back", f"acc_{account_id}")]
            ])
        
        elif data.startswith("reset_"):
            account_id = data.split("_")[1]
            account_stats_col.update_one(
                {'account_id': account_id},
                {'$set': {'total_sent': 0, 'total_failed': 0}},
                upsert=True
            )
            await event.answer("Stats reset!")
        
        elif data.startswith("refresh_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            
            await event.answer("🔄 Refreshing...", alert=False)
            
            try:
                session = cipher_suite.decrypt(acc['session'].encode()).decode()
                client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                await client.connect()
                
                if await client.is_user_authorized():
                    count = await fetch_groups(client, account_id, acc['phone'])
                    await client.disconnect()
                    await event.answer(f"✅ Found {count} groups!", alert=True)
                else:
                    await event.answer("❌ Session expired!", alert=True)
            except Exception as e:
                await event.answer(f"❌ Error!", alert=True)
        
        elif data.startswith("fwd_select_"):
            account_id = data.split("_")[2]
            await event.edit("▶️ **Start Forwarding**\n\nSelect where to forward:", buttons=forwarding_select_keyboard(account_id))
        
        elif data.startswith("startfwd_"):
            parts = data.split("_")
            account_id = parts[1]
            topic = parts[2] if len(parts) > 2 else "all"
            
            acc = get_account_by_id(account_id)
            accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': True, 'fwd_topic': topic}})
            
            if account_id not in forwarding_tasks:
                forwarding_tasks[account_id] = asyncio.create_task(forwarder_loop(account_id, topic))
            
            await event.answer("✅ Started!")
            await event.edit(f"▶️ Forwarding started!\n\nTopic: {topic}", buttons=[[Button.inline("⬅️ Back", f"acc_{account_id}")]])
        
        elif data.startswith("stop_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            
            accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': False}})
            
            if account_id in forwarding_tasks:
                forwarding_tasks[account_id].cancel()
                del forwarding_tasks[account_id]
            
            if account_id in auto_reply_clients:
                try:
                    await auto_reply_clients[account_id].disconnect()
                except:
                    pass
                del auto_reply_clients[account_id]
            
            await event.answer("⛔ Stopped!")
            await send_log(account_id, f"⛔ Forwarding stopped")
            await event.edit(f"⛔ Forwarding stopped!", buttons=[[Button.inline("⬅️ Back", f"acc_{account_id}")]])
        
        elif data.startswith("logs_"):
            account_id = data.split("_")[1]
            
            token = logger_tokens_col.find_one({'account_id': account_id})
            if not token:
                new_token = generate_token()
                logger_tokens_col.insert_one({'account_id': account_id, 'token': new_token, 'created_at': datetime.now()})
                token = {'token': new_token}
            
            settings = get_account_settings(account_id)
            logs_chat = settings.get('logs_chat_id')
            status = f"✅ Connected (ID: {logs_chat})" if logs_chat else "❌ Not configured"
            
            deep_link = f"https://t.me/{CONFIG['logger_bot_username']}?start={token['token']}"
            
            text = f"📝 **Logs Configuration**\n\nStatus: {status}\n\nClick below to setup:"
            
            await event.edit(text, buttons=[
                [Button.url("📝 Setup Logs", deep_link)],
                [Button.inline("⬅️ Back", f"acc_{account_id}")]
            ])
        
        elif data.startswith("delete_"):
            account_id = data.split("_")[1]
            await event.edit(
                "⚠️ **Delete this account?**\n\nAll data will be removed!",
                buttons=[
                    [Button.inline("✅ Yes", f"confirm_{account_id}"), Button.inline("❌ No", f"acc_{account_id}")]
                ]
            )
        
        elif data.startswith("confirm_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            
            if acc:
                from bson.objectid import ObjectId
                accounts_col.delete_one({'_id': ObjectId(account_id)})
                account_topics_col.delete_many({'account_id': account_id})
                account_settings_col.delete_many({'account_id': account_id})
                account_stats_col.delete_many({'account_id': account_id})
                account_auto_groups_col.delete_many({'account_id': account_id})
                account_failed_groups_col.delete_many({'account_id': account_id})
                logger_tokens_col.delete_many({'account_id': account_id})
                
                if account_id in forwarding_tasks:
                    forwarding_tasks[account_id].cancel()
                    del forwarding_tasks[account_id]
                
                if account_id in auto_reply_clients:
                    try:
                        await auto_reply_clients[account_id].disconnect()
                    except:
                        pass
                    del auto_reply_clients[account_id]
            
            await event.answer("Deleted!")
            await event.edit("📊 **Dashboard**", buttons=dashboard_keyboard(uid))
        
        elif data == "host":
            user_states[uid] = {'action': 'phone'}
            await event.respond("📱 Send phone with country code:\n\nExample: `+919876543210`")
        
        elif data.startswith("otp_"):
            if uid not in user_states or user_states[uid].get('action') != 'otp':
                return
            
            digit = data.split("_")[1]
            otp = user_states[uid].get('otp', '')
            
            if digit == "cancel":
                if 'client' in user_states[uid]:
                    await user_states[uid]['client'].disconnect()
                del user_states[uid]
                await event.answer("Cancelled!")
                await event.delete()
                return
            elif digit == "back":
                otp = otp[:-1]
            else:
                otp += digit
            
            user_states[uid]['otp'] = otp
            
            if len(otp) == 5:
                await event.edit(f"🔐 Code: `{otp}`\n\nVerifying...")
                
                try:
                    client = user_states[uid]['client']
                    await client.sign_in(user_states[uid]['phone'], otp, phone_code_hash=user_states[uid]['hash'])
                    
                    me = await client.get_me()
                    session = client.session.save()
                    encrypted = cipher_suite.encrypt(session.encode()).decode()
                    
                    result = accounts_col.insert_one({
                        'owner_id': uid,
                        'phone': user_states[uid]['phone'],
                        'name': me.first_name or 'Unknown',
                        'session': encrypted,
                        'is_forwarding': False,
                        'added_at': datetime.now()
                    })
                    
                    account_id = str(result.inserted_id)
                    count = await fetch_groups(client, account_id, user_states[uid]['phone'])
                    await client.disconnect()
                    
                    del user_states[uid]
                    
                    await event.edit(
                        f"✅ **Account Added!**\n\n📱 {me.first_name}\n🤖 Found {count} groups",
                        buttons=dashboard_keyboard(uid)
                    )
                    
                except SessionPasswordNeededError:
                    user_states[uid]['action'] = '2fa'
                    await event.edit("🔐 **2FA Required**\n\nSend your password:")
                except PhoneCodeInvalidError:
                    user_states[uid]['otp'] = ''
                    await event.edit("❌ Wrong code! Try again:", buttons=otp_keyboard())
                except Exception as e:
                    await event.edit(f"❌ Error: {str(e)[:100]}")
                    if 'client' in user_states[uid]:
                        await user_states[uid]['client'].disconnect()
                    del user_states[uid]
            else:
                await event.edit(f"🔐 Code: `{otp}{'_' * (5-len(otp))}`", buttons=otp_keyboard())
        
        elif data == "admin":
            if not is_admin(uid):
                return
            
            total_users = users_col.count_documents({})
            total_accounts = accounts_col.count_documents({})
            active = accounts_col.count_documents({'is_forwarding': True})
            
            await event.edit(
                f"👑 **Admin Panel**\n\n👤 Users: {total_users}\n📱 Accounts: {total_accounts}\n🟢 Active: {active}",
                buttons=[[Button.inline("⬅️ Back", b"dashboard")]]
            )
    
    except MessageNotModifiedError:
        pass
    except Exception as e:
        print(f"Callback error: {e}")
        await event.answer("Error!", alert=True)

# ==================== TEXT HANDLER ====================

@main_bot.on(events.NewMessage)
async def text_handler(event):
    uid = event.sender_id
    text = event.text.strip()
    
    if text.startswith('/'):
        return
    
    if uid not in user_states:
        return
    
    if not is_approved(uid):
        await event.respond("❌ Use /access first!")
        if uid in user_states:
            del user_states[uid]
        return
    
    state = user_states[uid]
    action = state.get('action') if isinstance(state, dict) else None
    
    if action == 'phone':
        if not re.match(r'^\+\d{10,15}$', text):
            await event.respond("❌ Invalid! Use: `+919876543210`")
            return
        
        try:
            client = TelegramClient(StringSession(), CONFIG['api_id'], CONFIG['api_hash'])
            await client.connect()
            
            sent = await client.send_code_request(text)
            
            user_states[uid] = {
                'action': 'otp',
                'client': client,
                'phone': text,
                'hash': sent.phone_code_hash,
                'otp': ''
            }
            
            await event.respond("📨 Code sent! Enter:", buttons=otp_keyboard())
            
        except PhoneNumberInvalidError:
            await event.respond("❌ Invalid phone!")
            del user_states[uid]
        except Exception as e:
            await event.respond(f"❌ Error: {str(e)[:100]}")
            del user_states[uid]
    
    elif action == '2fa':
        try:
            client = state['client']
            await client.sign_in(password=text)
            
            me = await client.get_me()
            session = client.session.save()
            encrypted = cipher_suite.encrypt(session.encode()).decode()
            
            result = accounts_col.insert_one({
                'owner_id': uid,
                'phone': state['phone'],
                'name': me.first_name or 'Unknown',
                'session': encrypted,
                'is_forwarding': False,
                'added_at': datetime.now()
            })
            
            account_id = str(result.inserted_id)
            count = await fetch_groups(client, account_id, state['phone'])
            await client.disconnect()
            
            del user_states[uid]
            
            await event.respond(
                f"✅ **Account Added!**\n\n📱 {me.first_name}\n🤖 Found {count} groups",
                buttons=dashboard_keyboard(uid)
            )
            
        except PasswordHashInvalidError:
            await event.respond("❌ Wrong password! Try again:")
        except Exception as e:
            await event.respond(f"❌ Error: {str(e)[:100]}")
            if 'client' in state:
                await state['client'].disconnect()
            del user_states[uid]
    
    elif action == 'add_links':
        account_id = state['account_id']
        topic = state['topic']
        
        links = [l.strip() for l in text.splitlines() if 't.me/' in l][:100]
        added = 0
        
        for link in links:
            try:
                peer, url, topic_id = parse_link(link)
                account_topics_col.insert_one({
                    'account_id': account_id,
                    'topic': topic,
                    'url': url,
                    'peer': peer,
                    'topic_id': topic_id
                })
                added += 1
            except:
                continue
        
        del user_states[uid]
        
        total = account_topics_col.count_documents({'account_id': account_id, 'topic': topic})
        await event.respond(f"✅ Added {added} links!\nTotal: {total}")
    
    elif action == 'set_msg_delay':
        try:
            v = int(text)
            if 5 <= v <= 300:
                update_account_settings(state['account_id'], {'msg_delay': v})
                del user_states[uid]
                await event.respond(f"✅ Message delay: {v}s")
            else:
                await event.respond("❌ Must be 5-300!")
        except:
            await event.respond("❌ Invalid number!")
    
    elif action == 'set_grp_delay':
        try:
            v = int(text)
            if 10 <= v <= 600:
                update_account_settings(state['account_id'], {'group_delay': v})
                del user_states[uid]
                await event.respond(f"✅ Group delay: {v}s")
            else:
                await event.respond("❌ Must be 10-600!")
        except:
            await event.respond("❌ Invalid number!")
    
    elif action == 'set_round_delay':
        try:
            v = int(text)
            if 60 <= v <= 86400:
                update_account_settings(state['account_id'], {'round_delay': v})
                del user_states[uid]
                await event.respond(f"✅ Round delay: {v}s")
            else:
                await event.respond("❌ Must be 60-86400!")
        except:
            await event.respond("❌ Invalid number!")
    
    elif action == 'set_reply':
        update_account_settings(state['account_id'], {'auto_reply': text})
        del user_states[uid]
        await event.respond("✅ Auto-reply updated!")
    
    elif action == 'confirm_delete':
        if text.upper() == 'YES':
            account_id = state['account_id']
            acc = get_account_by_id(account_id)
            
            if acc:
                from bson.objectid import ObjectId
                accounts_col.delete_one({'_id': ObjectId(account_id)})
                account_topics_col.delete_many({'account_id': account_id})
                account_settings_col.delete_many({'account_id': account_id})
                account_stats_col.delete_many({'account_id': account_id})
                account_auto_groups_col.delete_many({'account_id': account_id})
                account_failed_groups_col.delete_many({'account_id': account_id})
                logger_tokens_col.delete_many({'account_id': account_id})
                
                if account_id in forwarding_tasks:
                    forwarding_tasks[account_id].cancel()
                    del forwarding_tasks[account_id]
            
            del user_states[uid]
            await event.respond(f"✅ Account #{state['index']} deleted!")
        else:
            del user_states[uid]
            await event.respond("❌ Cancelled!")

# ==================== LOGGER BOT ====================

@logger_bot.on(events.NewMessage(pattern=r'/start ?(.*)'))
async def logger_start(event):
    uid = event.sender_id
    args = event.pattern_match.group(1)
    
    if args:
        token_doc = logger_tokens_col.find_one({'token': args})
        if token_doc:
            user_states[f"log_{uid}"] = {'account_id': token_doc['account_id']}
            await event.respond(
                "📝 **Logger Setup**\n\n"
                "1. Add me to a channel/group as admin\n"
                "2. Forward any message from that chat here\n\n"
                "Or send the chat ID directly."
            )
            return
    
    await event.respond("📝 Use the link from main bot to configure logs.")

@logger_bot.on(events.NewMessage)
async def logger_handler(event):
    uid = event.sender_id
    key = f"log_{uid}"
    
    if key not in user_states:
        return
    
    state = user_states[key]
    
    if event.forward:
        chat_id = event.forward.chat_id
    else:
        try:
            chat_id = int(event.text.strip())
        except:
            await event.respond("❌ Forward a message from target chat or send ID!")
            return
    
    try:
        await logger_bot.send_message(chat_id, "✅ Logger connected! You'll receive forwarding logs here.")
        
        update_account_settings(state['account_id'], {'logs_chat_id': chat_id})
        
        del user_states[key]
        await event.respond("✅ Logs configured!")
        
    except Exception as e:
        await event.respond(f"❌ Cannot send to that chat!\nMake sure I'm admin.\n\nError: {str(e)[:50]}")

# ==================== FORWARDER ====================

async def forwarder_loop(account_id, selected_topic):
    print(f"[{account_id}] Starting forwarder (topic: {selected_topic})")
    
    acc = get_account_by_id(account_id)
    if not acc:
        return
    
    await send_log(account_id, f"▶️ Forwarding started\nAccount: {acc['phone']}\nTopic: {selected_topic}")
    
    while True:
        try:
            acc = get_account_by_id(account_id)
            if not acc or not acc.get('is_forwarding'):
                print(f"[{account_id}] Stopped")
                break
            
            settings = get_account_settings(account_id)
            msg_delay = settings.get('msg_delay', 30)
            group_delay = settings.get('group_delay', 90)
            round_delay = settings.get('round_delay', 3600)
            auto_reply_msg = settings.get('auto_reply', CONFIG['auto_reply'])
            reply_cooldown = settings.get('reply_cooldown', 300)
            
            try:
                session = cipher_suite.decrypt(acc['session'].encode()).decode()
                client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                await client.connect()
                
                if not await client.is_user_authorized():
                    print(f"[{account_id}] Session expired")
                    await send_log(account_id, "❌ Session expired!")
                    await asyncio.sleep(60)
                    continue
                
                await client.start()
                
                @client.on(events.NewMessage(incoming=True))
                async def auto_reply(event):
                    if event.is_private:
                        sender = event.sender_id
                        cooldown_key = f"{account_id}_{sender}"
                        now = time.time()
                        
                        if cooldown_key in last_replied:
                            if now - last_replied[cooldown_key] < reply_cooldown:
                                return
                        
                        try:
                            await event.respond(auto_reply_msg)
                            last_replied[cooldown_key] = now
                            print(f"[{account_id}] Auto-replied to {sender}")
                        except:
                            pass
                
                auto_reply_clients[account_id] = client
                
                ads = []
                async for msg in client.iter_messages('me', limit=10):
                    if msg.text or msg.media:
                        ads.append(msg)
                ads.reverse()
                
                if not ads:
                    print(f"[{account_id}] No ads in Saved Messages")
                    await send_log(account_id, "⚠️ No ads found in Saved Messages!")
                    await client.disconnect()
                    await asyncio.sleep(60)
                    continue
                
                all_targets = []
                
                if selected_topic != "all":
                    topic_links = list(account_topics_col.find({'account_id': account_id, 'topic': selected_topic}))
                    for t in topic_links:
                        group_key = t['url']
                        group_name = t.get('url', 'Unknown')
                        if not is_group_failed(account_id, group_key):
                            all_targets.append({'type': 'topic', 'data': t, 'key': group_key, 'name': group_name})
                
                auto_groups = list(account_auto_groups_col.find({'account_id': account_id}))
                topic_peers = set()
                
                if selected_topic != "all":
                    for t in all_targets:
                        if 'peer' in t['data']:
                            topic_peers.add(str(t['data']['peer']))
                
                for g in auto_groups:
                    group_key = str(g['group_id'])
                    group_name = g.get('title', 'Unknown')
                    if group_key not in topic_peers and not is_group_failed(account_id, group_key):
                        all_targets.append({'type': 'auto', 'data': g, 'key': group_key, 'name': group_name})
                
                active_waits = get_active_flood_waits(account_id)
                print(f"[{account_id}] Forwarding to {len(all_targets)} groups (flood waits: {active_waits})")
                await send_log(account_id, f"🔄 Starting round\n📊 Groups: {len(all_targets)}\n⏳ Flood waits: {active_waits}")
                
                sent = 0
                failed = 0
                skipped = 0
                
                for i, target in enumerate(all_targets):
                    try:
                        acc_check = get_account_by_id(account_id)
                        if not acc_check or not acc_check.get('is_forwarding'):
                            break
                        
                        group_name = target.get('name', 'Unknown')[:30]
                        group_key = target['key']
                        
                        wait_remaining = get_flood_wait(account_id, group_key)
                        if wait_remaining > 0:
                            skipped += 1
                            mins = wait_remaining // 60
                            print(f"[{account_id}] Skipped {group_name} (wait: {mins}m)")
                            await send_log(account_id, f"⏭️ Skipped: {group_name}\n⏳ Wait remaining: {mins} mins")
                            continue
                        
                        msg = ads[i % len(ads)]
                        
                        if target['type'] == 'topic':
                            data = target['data']
                            peer = data.get('peer')
                            topic_id = data.get('topic_id')
                            
                            if peer is None:
                                peer, _, topic_id = parse_link(data['url'])
                            
                            entity = await client.get_entity(peer)
                            group_name = getattr(entity, 'title', group_name)[:30]
                            
                            if topic_id:
                                await forward_message(client, entity, msg.id, msg.peer_id, topic_id)
                            else:
                                await client.forward_messages(entity, msg.id, 'me')
                        else:
                            data = target['data']
                            group_id = data['group_id']
                            access_hash = data.get('access_hash')
                            is_channel = data.get('is_channel', True)
                            username = data.get('username')
                            
                            entity = None
                            if username:
                                try:
                                    entity = await client.get_entity(username)
                                except:
                                    pass
                            
                            if entity is None and access_hash:
                                try:
                                    if is_channel:
                                        entity = InputPeerChannel(channel_id=group_id, access_hash=access_hash)
                                    else:
                                        entity = InputPeerChat(chat_id=group_id)
                                except:
                                    pass
                            
                            if entity is None:
                                try:
                                    entity = await client.get_entity(group_id)
                                except:
                                    entity = await client.get_entity(int('-100' + str(group_id)))
                            
                            await client.forward_messages(entity, msg.id, 'me')
                        
                        sent += 1
                        print(f"[{account_id}] ✅ Sent to {group_name} ({i+1}/{len(all_targets)})")
                        await send_log(account_id, f"✅ Sent to: {group_name}")
                        
                        await asyncio.sleep(msg_delay)
                        
                        if (i + 1) % 10 == 0:
                            print(f"[{account_id}] Group pause ({group_delay}s)")
                            await asyncio.sleep(group_delay)
                        
                    except FloodWaitError as e:
                        wait_secs = e.seconds
                        mins = wait_secs // 60
                        failed += 1
                        
                        set_flood_wait(account_id, group_key, group_name, wait_secs)
                        
                        print(f"[{account_id}] ⏳ FloodWait {mins}m in {group_name}")
                        await send_log(account_id, f"⏳ FloodWait: {group_name}\n⏱️ Wait: {mins} mins\n📝 Will retry after wait expires")
                        
                        await asyncio.sleep(msg_delay)
                        
                    except (ChannelPrivateError, ChatWriteForbiddenError, UserBannedInChannelError) as e:
                        failed += 1
                        mark_group_failed(account_id, target['key'], str(e))
                        error_type = type(e).__name__
                        print(f"[{account_id}] ❌ Failed {group_name}: {error_type}")
                        await send_log(account_id, f"❌ Failed: {group_name}\n📝 Error: {error_type}\n🚫 Marked as failed")
                        await asyncio.sleep(msg_delay)
                        
                    except Exception as e:
                        error_str = str(e)
                        
                        wait_match = re.search(r'wait of (\d+) seconds', error_str, re.IGNORECASE)
                        if wait_match:
                            wait_secs = int(wait_match.group(1))
                            mins = wait_secs // 60
                            failed += 1
                            set_flood_wait(account_id, group_key, group_name, wait_secs)
                            print(f"[{account_id}] ⏳ FloodWait {mins}m in {group_name}")
                            await send_log(account_id, f"⏳ FloodWait: {group_name}\n⏱️ Wait: {mins} mins\n📝 Will retry after wait expires")
                        elif 'Could not find' in error_str or 'entity' in error_str.lower():
                            failed += 1
                            mark_group_failed(account_id, target['key'], error_str[:100])
                            print(f"[{account_id}] ❌ Entity error {group_name}: {error_str[:40]}")
                            await send_log(account_id, f"❌ Not found: {group_name}\n🚫 Marked as failed")
                        else:
                            failed += 1
                            print(f"[{account_id}] ❌ Error {group_name}: {error_str[:50]}")
                            await send_log(account_id, f"❌ Error: {group_name}\n📝 {error_str[:50]}")
                        
                        await asyncio.sleep(msg_delay)
                
                update_account_stats(account_id, sent=sent, failed=failed)
                
                log_msg = f"✅ Round complete!\n\n📤 Sent: {sent}\n❌ Failed: {failed}\n⏭️ Skipped (flood): {skipped}\n⏱️ Next round: {round_delay}s"
                await send_log(account_id, log_msg)
                
                print(f"[{account_id}] Round done! Sent: {sent}, Failed: {failed}, Skipped: {skipped}")
                print(f"[{account_id}] Waiting {round_delay}s...")
                
                await asyncio.sleep(round_delay)
                await client.disconnect()
                
            except Exception as e:
                print(f"[{account_id}] Loop error: {e}")
                await send_log(account_id, f"⚠️ Loop error: {str(e)[:50]}")
                await asyncio.sleep(60)
                
        except Exception as e:
            print(f"[{account_id}] Outer error: {e}")
            await asyncio.sleep(60)
    
    if account_id in forwarding_tasks:
        del forwarding_tasks[account_id]
    if account_id in auto_reply_clients:
        try:
            await auto_reply_clients[account_id].disconnect()
        except:
            pass
        del auto_reply_clients[account_id]
    
    await send_log(account_id, "⛔ Forwarding ended")
    print(f"[{account_id}] Forwarder ended")

# ==================== MAIN ====================

async def main():
    print("\n" + "="*50)
    print("🚀 Starting Ads Bot...")
    print("="*50)
    
    try:
        await main_bot.start(bot_token=CONFIG['bot_token'])
        me = await main_bot.get_me()
        print(f"✅ Main: @{me.username}")
    except Exception as e:
        print(f"❌ Main bot failed: {e}")
        return
    
    try:
        if CONFIG['logger_bot_token']:
            await logger_bot.start(bot_token=CONFIG['logger_bot_token'])
            me = await logger_bot.get_me()
            print(f"✅ Logger: @{me.username}")
    except Exception as e:
        print(f"⚠️ Logger failed: {e}")
    
    print("="*50)
    print("✅ Bot running!")
    print("="*50 + "\n")
    
    await asyncio.gather(
        main_bot.run_until_disconnected(),
        logger_bot.run_until_disconnected() if CONFIG['logger_bot_token'] else asyncio.sleep(0)
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⛔ Stopped")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        mongo_client.close()
