from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import aioboto3
import aiohttp
import aiomysql
from dotenv import find_dotenv, load_dotenv

env_path = find_dotenv(".env.copart")
load_dotenv(env_path)

def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip().strip('"').strip("'")

# --- DATABASE CONFIG ---
DB_HOST: str = _env("DB_HOST", "127.0.0.1")
DB_PORT: int = int(_env("DB_PORT", "3306"))
DB_USER: str = _env("DB_USER")
DB_PASSWORD: str = _env("DB_PASSWORD")
DB_NAME: str = _env("DB_NAME")
DB_TABLE: str = _env("DB_TABLE", "copart_lots_flat")

BATCH_SIZE: int = int(_env("BATCH_SIZE", "30"))
EMPTY_POLL_SLEEP: float = float(_env("EMPTY_POLL_SLEEP", "5.0"))

# --- MINIO & APP CONFIG ---
MINIO_ACCESS_KEY: str = _env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY: str = _env("MINIO_SECRET_KEY")
MINIO_REGION: str = _env("MINIO_REGION", "us-east-1")
MINIO_SECURE: bool = _env("MINIO_SECURE", "false").lower() == "true"

_raw_ep = _env("MINIO_ENDPOINT")
MINIO_ENDPOINT: str = (
    _raw_ep if _raw_ep.startswith("http")
    else f"{'https' if MINIO_SECURE else 'http'}://{_raw_ep}"
)

BUCKET: str = _env("MINIO_BUCKET_BASE")
IMG_PREFIX: str = _env("IMG_PREFIX")
AUCTION_NAME: str = _env("AUCTION_NAME", "copart")

MAX_CONCURRENT_DOWNLOADS: int = int(_env("MAX_CONCURRENT_DOWNLOADS", "50"))
AIOHTTP_CONNECTOR_LIMIT: int = int(_env("AIOHTTP_CONNECTOR_LIMIT", "100"))
AIOHTTP_LIMIT_PER_HOST: int = int(_env("AIOHTTP_LIMIT_PER_HOST", "0"))
DOWNLOAD_TIMEOUT_S: int = int(_env("DOWNLOAD_TIMEOUT_S", "30"))

log_dir = Path(f"{AUCTION_NAME}_images_downloader_logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(
            log_dir / f"worker_{datetime.now().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(f"{AUCTION_NAME}.db_reader")

# --- УПРАВЛІННЯ OFFSET-ОМ (Заміна комітів Kafka) ---
OFFSET_FILE = Path(f"{AUCTION_NAME}_offset.json")

def load_offset() -> int | str:
    """Завантажує останній оброблений ID. Повертає 0, якщо файлу немає."""
    if OFFSET_FILE.exists():
        try:
            data = json.loads(OFFSET_FILE.read_text(encoding="utf-8"))
            return data.get("last_id", 0)
        except json.JSONDecodeError:
            return 0
    return 0

def save_offset(last_id: int | str) -> None:
    """Зберігає останній оброблений ID на диск."""
    OFFSET_FILE.write_text(json.dumps({"last_id": last_id}), encoding="utf-8")


def md5_of(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()

def object_key(stock_id: str | int, url: str, is_360: bool = False, is_video: bool = False, frame_idx: int | None = None) -> str:
    if is_video:
        return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/video/{md5_of(url)}.mp4"
    if is_360:
        if frame_idx is None:
            return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/360_interior/{md5_of(url)}.jpg"
        else:
            return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/360/{md5_of(url)}.jpg"
    return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/{md5_of(url)}.jpg"


class DatabaseReader:
    def __init__(self):
        self.pool: aiomysql.Pool | None = None

    async def connect(self) -> None:
        self.pool = await aiomysql.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            autocommit=True, # Оскільки ми лише читаємо, транзакції не потрібні
            minsize=1,
            maxsize=5,
        )
        logger.info("Connected to database %s on %s:%s (READ ONLY)", DB_NAME, DB_HOST, DB_PORT)

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Database connection closed.")

    async def fetch_batch(self, last_id: int | str) -> list[dict]:
        """Вибирає батч записів суворо без блокувань."""
        if not self.pool:
            raise RuntimeError("Database pool is not initialized")

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:

                # Якщо offset ще нульовий, беремо просто самі верхні (найновіші) лоти
                if not last_id or last_id == 0:
                    query = f"""
                        SELECT ln AS id, photos_json AS raw_data
                        FROM {DB_TABLE}
                        ORDER BY ln DESC
                        LIMIT %s
                    """
                    await cur.execute(query, (BATCH_SIZE,))

                # Якщо offset вже є, беремо ті, що МЕНШІ за збережений ln
                else:
                    query = f"""
                        SELECT ln AS id, photos_json AS raw_data
                        FROM {DB_TABLE}
                        WHERE ln < %s
                        ORDER BY ln DESC
                        LIMIT %s
                    """
                    await cur.execute(query, (last_id, BATCH_SIZE))

                return await cur.fetchall()


class ImageDownloader:
    def __init__(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> None:
        self._session = session
        self._sem     = sem
        self._timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_S)

    async def fetch(self, url: str, stock_id: str) -> bytes | None:
        async with self._sem:
            try:
                async with self._session.get(url, timeout=self._timeout) as resp:
                    resp.raise_for_status()
                    return await resp.read()
            except aiohttp.ClientResponseError as exc:
                logger.warning("Lot %s -> HTTP %s — %s", stock_id, exc.status, url)
            except asyncio.TimeoutError:
                logger.warning("Lot %s -> Timeout downloading — %s", stock_id, url)
            except Exception as exc:
                logger.warning("Lot %s -> Download error %r — %s", stock_id, exc, url)
            return None


class MinioUploader:
    def __init__(self) -> None:
        self._boto = aioboto3.Session(
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
        )

    async def upload_many(self, items: list[tuple[bytes, str]]) -> None:
        if not items:
            return
        async with self._boto.client("s3", endpoint_url=MINIO_ENDPOINT) as s3:
            await asyncio.gather(
                *[self._put(s3, data, key) for data, key in items],
                return_exceptions=True,
            )

    @staticmethod
    async def _put(s3: Any, data: bytes, key: str) -> None:
        content_type = "video/mp4" if key.endswith(".mp4") else "image/jpeg"
        try:
            await s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=BytesIO(data),
                ContentType=content_type,
            )
            logger.debug("Uploaded %s (%d B)", key, len(data))
        except Exception as exc:
            logger.error("Upload failed %s — %r", key, exc)


async def _extract_images_iaai(lot: dict[str, Any], downloader: ImageDownloader) -> list[tuple[str, bool, bool, int | None]]:
    result = []
    images = lot.get("images") or lot.get("imageUrls") or lot.get("links") or []

    for url in images:
        if isinstance(url, str):
            result.append((url, False, False, None))

    video_url = lot.get("video_url")
    if video_url and isinstance(video_url, str):
        result.append((video_url, False, True, None))

    image360_url = lot.get("image360Url")
    source_file = str(lot.get("source_file", ""))
    stock_id = str(lot.get("stock_id", ""))

    partition_key = "".join(filter(str.isdigit, source_file))
    if not partition_key:
        partition_key = stock_id

    if image360_url and partition_key:
        logger.debug("Lot %s -> Downloading HTML for 360...", stock_id)
        html_bytes = await downloader.fetch(image360_url, stock_id)

        if html_bytes:
            html_text = html_bytes.decode("utf-8", errors="ignore")
            tenant_match = re.search(r'var\s+tenant\s*=\s*"([^"]+)"', html_text)
            tenant = tenant_match.group(1) if tenant_match else "iaai"
            amount_match = re.search(r'data-amount-x="(\d+)"', html_text)

            if amount_match:
                amount = int(amount_match.group(1))
                for i in range(1, amount + 1):
                    url_360 = f"https://mediaretriever.iaai.com/api/ThreeSixtyImageRetriever?tenant={tenant}&partitionKey={partition_key}&imageOrder={i}"
                    result.append((url_360, True, False, i))

            int_url = f"https://mediaretriever.iaai.com/api/InteriorImageRetriever?tenant={tenant}&partitionKey={partition_key}"
            result.append((int_url, True, False, None))
    return result

def _extract_images_manheim(lot: dict[str, Any]) -> list[tuple[str, bool, bool, int | None]]:
    result = []
    images = lot.get("images") or []
    for url in images:
        if isinstance(url, str):
            result.append((url, False, False, None))
    return result

def _extract_images_copart(lot: dict[str, Any]) -> list[tuple[str, bool, bool, int | None]]:
    result = []
    images_list = lot.get("imagesList") or lot.get("data", {}).get("imagesList", {})

    if not images_list:
        return result

    for img_obj in images_list.get("IMAGE", []):
        full_url = img_obj.get("fullUrl")
        if full_url:
            result.append((full_url, False, False, None))

    for img_obj in images_list.get("EXTERIOR_360", []):
        base_url = img_obj.get("image360Url")
        frames = img_obj.get("frameCount", 0)
        if base_url and frames > 0 and "_0." in base_url:
            prefix, ext = base_url.rsplit("_0.", 1)
            for i in range(frames):
                url_i = f"{prefix}_{i}.{ext}"
                result.append((url_i, True, False, i))

    for video_obj in images_list.get("ENGINE_VIDEO_SOUND", []):
        video_url = video_obj.get("highResUrl")
        if video_url:
            result.append((video_url, False, True, None))

    return result

async def process_lot(
    lot: dict[str, Any],
    downloader: ImageDownloader,
    uploader: MinioUploader,
) -> None:
    stock_id = (
        lot.get("stock_id") or
        lot.get("data", {}).get("lotDetails", {}).get("ln") or
        lot.get("lot_number") or
        lot.get("stockId") or
        lot.get("id") or
        (lot.get("data", {}).get("imagesList", {}).get("IMAGE", [{}])[0].get("lotNumberStr"))
    )

    if not stock_id:
        return

    if AUCTION_NAME.lower() == "copart":
        images_data = _extract_images_copart(lot)
    elif AUCTION_NAME.lower() == "iaai":
        images_data = await _extract_images_iaai(lot, downloader)
    elif AUCTION_NAME.lower() == "manheim":
        images_data = _extract_images_manheim(lot)
    else:
        images_data = await _extract_images_iaai(lot, downloader)

    if not images_data:
        logger.debug("Lot %s — no images/videos found.", stock_id)
        return

    results: list[bytes | None] = await asyncio.gather(
        *[downloader.fetch(item[0], stock_id) for item in images_data]
    )

    batch = [
        (data, object_key(stock_id, url, is_360, is_video, frame_idx))
        for (url, is_360, is_video, frame_idx), data in zip(images_data, results)
        if data is not None
    ]

    await uploader.upload_many(batch)
    logger.info("Lot %s — uploaded %d/%d", stock_id, len(batch), len(images_data))


def extract_lots(value: Any) -> list[dict]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, dict):
        for key in ("lots", "data", "items", "results"):
            nested_value = value.get(key)
            if isinstance(nested_value, list):
                return [v for v in nested_value if isinstance(v, dict)]
        return [value]
    return []


async def run() -> None:
    sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    img_connector = aiohttp.TCPConnector(
        limit=AIOHTTP_CONNECTOR_LIMIT,
        limit_per_host=AIOHTTP_LIMIT_PER_HOST or None,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    img_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,video/*,*/*;q=0.8",
    }

    db_reader = DatabaseReader()
    await db_reader.connect()

    # Читаємо останній offset з диску
    last_processed_id = load_offset()
    logger.info("Starting from offset ID: %s", last_processed_id)

    async with aiohttp.ClientSession(connector=img_connector, headers=img_headers) as img_session:
        downloader = ImageDownloader(img_session, sem)
        uploader   = MinioUploader()

        try:
            while True:
                try:
                    db_records = await db_reader.fetch_batch(last_processed_id)

                    if not db_records:
                        logger.debug("No new records — sleeping %.1fs", EMPTY_POLL_SLEEP)
                        await asyncio.sleep(EMPTY_POLL_SLEEP)
                        continue

                    logger.info("Fetched %d record(s). Processing...", len(db_records))
                    t0 = asyncio.get_event_loop().time()

                    for record in db_records:
                        current_id = record["id"]
                        raw_data = record["raw_data"]

                        lots = extract_lots(raw_data)
                        if lots:
                            for lot in lots:
                                await process_lot(lot, downloader, uploader)

                        # Оновлюємо offset локально після кожного запису (або можна після батчу)
                        last_processed_id = current_id

                    # Зберігаємо offset у файл після успішного батчу
                    save_offset(last_processed_id)

                    logger.info("Batch done in %.2fs. New offset: %s", asyncio.get_event_loop().time() - t0, last_processed_id)

                except aiomysql.Error as exc:
                    logger.error("Database connection lost: %r. Reconnecting in 5s...", exc)
                    await asyncio.sleep(5)
                    await db_reader.close()
                    await db_reader.connect()
                except Exception as exc:
                    logger.error("Unexpected error in main loop: %r", exc)
                    await asyncio.sleep(2)

        except asyncio.CancelledError:
            logger.info("Cancelled.")
        except KeyboardInterrupt:
            logger.info("Interrupted.")
        finally:
            save_offset(last_processed_id) # Зберігаємо стан перед виходом
            await db_reader.close()
            logger.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(run())