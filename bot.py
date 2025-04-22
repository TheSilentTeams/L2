import asyncio
from contextlib import asynccontextmanager
import mimetypes
import math
import logging
from os import environ
import random
import time
from collections import OrderedDict

from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from telethon.sessions import StringSession
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.types import InputDocumentFileLocation, DocumentAttributeFilename

from config import API_ID, API_HASH, BIN_CHANNEL, SESSION_STRING, BOT_TOKENS  # <-- multiple tokens in list
from crypto import decrypt_msg_id

load_dotenv()

MAX_TELEGRAM_CHUNK = 512 * 1024
CACHE_CLEAN_INTERVAL = 30 * 60
MAX_CONCURRENT_DOWNLOADS = 5

random.seed(42)
active_ips = set()
cached_file_ids = {}
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# === MULTI BOT CLIENTS === #
bot_clients = []

async def init_bot_clients():
    for token in BOT_TOKENS:
        client = TelegramClient(f"bot_{token[:6]}", int(API_ID), API_HASH)
        await client.start(bot_token=token)
        bot_clients.append(client)
        logger.info(f"Bot client initialized for token ending with ...{token[-5:]}")

def get_random_client():
    return random.choice(bot_clients)


# === DC SESSION CACHE === #
class DCCache:
    def __init__(self, max_size=5, ttl=1800):
        self.sessions = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl

    def _evict_expired(self):
        now = time.time()
        to_delete = [dc_id for dc_id, (ts, _) in self.sessions.items() if now - ts > self.ttl]
        for dc_id in to_delete:
            _, client = self.sessions.pop(dc_id)
            asyncio.create_task(client.disconnect())
            logger.info(f"Evicted expired DC {dc_id}")

    def _evict_if_needed(self):
        while len(self.sessions) > self.max_size:
            dc_id, (_, client) = self.sessions.popitem(last=False)
            asyncio.create_task(client.disconnect())
            logger.info(f"Evicted LRU DC {dc_id}")

    def get(self, dc_id):
        self._evict_expired()
        if dc_id in self.sessions:
            ts, client = self.sessions.pop(dc_id)
            self.sessions[dc_id] = (time.time(), client)
            return client
        return None

    def set(self, dc_id, client):
        self.sessions[dc_id] = (time.time(), client)
        self._evict_if_needed()

media_sessions = DCCache(max_size=5, ttl=5000)


# === FLOODWAIT WRAPPER === #
def floodwait_retry(func):
    import functools

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        retries = 3
        for attempt in range(retries):
            try:
                return await func(*args, **kwargs)
            except FloodWaitError as e:
                wait = e.seconds + 1
                logger.warning(f"FloodWaitError: sleeping for {wait}s (attempt {attempt + 1})")
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"Unhandled error in {func.__name__}: {e}")
                raise
        raise RuntimeError("Too many FloodWaits, aborting.")
    return wrapper

@floodwait_retry
async def safe_get_messages(*args, **kwargs):
    return await get_random_client().get_messages(*args, **kwargs)

@floodwait_retry
async def safe_export_auth(dc_id):
    return await get_random_client()(ExportAuthorizationRequest(dc_id=dc_id))

@floodwait_retry
async def safe_import_auth(dc_client, auth):
    return await dc_client(ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes))


# === FASTAPI APP === #
@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("Starting bot pool...")
    await init_bot_clients()
    asyncio.create_task(clean_cache())
    yield
    logger.info("Shutting down all clients...")
    for client in bot_clients:
        await client.disconnect()
    for _, dc_client in media_sessions.sessions.values():
        try:
            await dc_client.disconnect()
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)


async def clean_cache():
    while True:
        await asyncio.sleep(CACHE_CLEAN_INTERVAL)
        cached_file_ids.clear()
        logger.debug("Cleaned file ID cache")


async def get_media_session(dc_id):
    client = media_sessions.get(dc_id)
    if client and await client.is_connected():
        return client

    logger.info(f"Creating new client for DC {dc_id}")
    session_name = f"client_dc_{dc_id}"
    client = TelegramClient(session_name, int(API_ID), API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        exported = await safe_export_auth(dc_id)
        await safe_import_auth(client, exported)

    media_sessions.set(dc_id, client)
    return client


@app.get("/stream/{msg_id}")
async def stream_handler(msg_id: str, request: Request):
    ip = request.client.host
    if ip in active_ips:
        raise HTTPException(status_code=429, detail="Download already in progress for this IP")
    active_ips.add(ip)

    try:
        async with download_semaphore:
            msg_id = decrypt_msg_id(msg_id)

            if msg_id in cached_file_ids:
                message = cached_file_ids[msg_id]
            else:
                message = await safe_get_messages(int(BIN_CHANNEL), ids=msg_id)
                if message:
                    cached_file_ids[msg_id] = message

            if not message or not message.document:
                raise HTTPException(status_code=404, detail="Invalid or non-document message")

            doc = message.document
            file_size = doc.size
            filename = next(
                (attr.file_name for attr in doc.attributes if isinstance(attr, DocumentAttributeFilename)),
                "file"
            )
            mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            disposition = "inline" if "video/" in mime_type or "audio/" in mime_type else "attachment"

            range_header = request.headers.get("range")
            start = 0
            end = file_size - 1

            if range_header:
                try:
                    units, range_spec = range_header.strip().split("=")
                    range_start, range_end = range_spec.split("-")
                    start = int(range_start)
                    if range_end:
                        end = int(range_end)
                except:
                    raise HTTPException(status_code=416, detail="Invalid Range header")

            length = end - start + 1
            status_code = 206 if range_header else 200

            async def iter_file():
                limit = MAX_TELEGRAM_CHUNK
                offset = start - (start % limit)
                first_cut = start - offset
                last_cut = end % limit + 1
                first_part = math.floor(offset / limit)
                last_part = math.ceil(end / limit)
                parts = last_part - first_part
                current = 1
                try:
                    async for chunk in get_random_client().iter_download(message, offset=offset, limit=parts, chunk_size=limit, file_size=file_size):
                        if current == 1:
                            yield chunk[first_cut:]
                        elif current == parts:
                            yield chunk[:last_cut]
                        else:
                            yield chunk
                        current += 1
                finally:
                    active_ips.remove(ip)

            headers = {
                "Content-Type": mime_type,
                "Content-Disposition": f'{disposition}; filename="{filename}"',
                "Accept-Ranges": "bytes",
                "Content-Length": str(length)
            }
            if range_header:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

            return StreamingResponse(iter_file(), headers=headers, status_code=status_code)

    except Exception as e:
        logger.error(f"Streaming error for IP {ip}: {e}")
        active_ips.discard(ip)
        raise e
