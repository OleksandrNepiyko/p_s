"""
launch:

docker rm -f usa-image-downloader
docker build -t oleksandrnepiyko/usa-image-downloader:latest .
docker run -d --name usa-image-downloader --restart always --env-file .env.name_of_web oleksandrnepiyko/usa-image-downloader:latest
docker logs -f usa-image-downloader
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os, re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
import aioboto3
import aiohttp
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from dotenv import find_dotenv, load_dotenv

env_path = find_dotenv(".env")
load_dotenv(env_path)


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip().strip('"').strip("'")


KAFKA_BOOTSTRAP: str  = _env("KAFKA_BOOTSTRAP", "10.30.0.113:19092")
TOPIC:           str  = _env("TOPIC", "iaai-raw-lots")
GROUP_ID:        str  = _env("GROUP_ID", "saver-worker-group")

KAFKA_USER:  str = _env("KAFKA_USER")
KAFKA_TOKEN: str = _env("KAFKA_TOKEN")

EMPTY_POLL_SLEEP: float = float(_env("EMPTY_POLL_SLEEP", "1.0"))

MINIO_ACCESS_KEY: str  = _env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY: str  = _env("MINIO_SECRET_KEY")
MINIO_REGION:     str  = _env("MINIO_REGION", "us-east-1")
MINIO_SECURE:     bool = _env("MINIO_SECURE", "false").lower() == "true"

_raw_ep = _env("MINIO_ENDPOINT")
MINIO_ENDPOINT: str = (
    _raw_ep if _raw_ep.startswith("http")
    else f"{'https' if MINIO_SECURE else 'http'}://{_raw_ep}"
)

BUCKET:       str = _env("MINIO_BUCKET_BASE")
IMG_PREFIX:   str = _env("IMG_PREFIX")
AUCTION_NAME: str = _env("AUCTION_NAME")

MAX_CONCURRENT_DOWNLOADS: int = int(_env("MAX_CONCURRENT_DOWNLOADS", "50"))
AIOHTTP_CONNECTOR_LIMIT:  int = int(_env("AIOHTTP_CONNECTOR_LIMIT",  "100"))
AIOHTTP_LIMIT_PER_HOST:   int = int(_env("AIOHTTP_LIMIT_PER_HOST",   "0"))
DOWNLOAD_TIMEOUT_S:       int = int(_env("DOWNLOAD_TIMEOUT_S",       "30"))
BATCH_SIZE:               int = int(_env("BATCH_SIZE",               "30"))

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
logger = logging.getLogger(f"{AUCTION_NAME}.consumer")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def md5_of(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def object_key(
    stock_id: str | int,
    url: str,
    is_360: bool = False,
    is_video: bool = False,
    frame_idx: int | None = None,
) -> str:
    if is_video:
        return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/video/{md5_of(url)}.mp4"
    if is_360:
        if frame_idx is None:
            return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/360_interior/{md5_of(url)}.jpg"
        return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/360/{md5_of(url)}.jpg"
    return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/{md5_of(url)}.jpg"


# ---------------------------------------------------------------------------
# ImageDownloader
# ---------------------------------------------------------------------------

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
                logger.warning("Lot %s -> Timeout — %s", stock_id, url)
            except Exception as exc:
                logger.warning("Lot %s -> Download error %r — %s", stock_id, exc, url)
            return None


# ---------------------------------------------------------------------------
# MinioUploader
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Image extractors
# ---------------------------------------------------------------------------

async def _extract_images_iaai(
    lot: dict[str, Any], downloader: ImageDownloader
) -> list[tuple[str, bool, bool, int | None]]:
    result = []
    images = lot.get("images") or lot.get("imageUrls") or lot.get("links") or []
    for url in images:
        if isinstance(url, str):
            result.append((url, False, False, None))

    video_url = lot.get("video_url")
    if video_url and isinstance(video_url, str):
        result.append((video_url, False, True, None))

    image360_url = lot.get("image360Url")
    source_file  = str(lot.get("source_file", ""))
    stock_id     = str(lot.get("stock_id", ""))

    logger.info("Lot %s -> RAW JSON image360Url: %s", stock_id, image360_url)

    partition_key = "".join(filter(str.isdigit, source_file)) or stock_id

    if image360_url and partition_key:
        logger.info("Lot %s -> Downloading HTML for 360...", stock_id)
        html_bytes = await downloader.fetch(image360_url, stock_id)
        if html_bytes:
            html_text = html_bytes.decode("utf-8", errors="ignore")
            tenant_match = re.search(r'var\s+tenant\s*=\s*"([^"]+)"', html_text)
            tenant       = tenant_match.group(1) if tenant_match else "iaai"
            amount_match = re.search(r'data-amount-x="(\d+)"', html_text)
            logger.info(
                "Lot %s -> tenant: '%s', amount: '%s'",
                stock_id, tenant,
                amount_match.group(1) if amount_match else "NOT_FOUND",
            )
            if amount_match:
                for i in range(1, int(amount_match.group(1)) + 1):
                    url_360 = (
                        f"https://mediaretriever.iaai.com/api/ThreeSixtyImageRetriever"
                        f"?tenant={tenant}&partitionKey={partition_key}&imageOrder={i}"
                    )
                    result.append((url_360, True, False, i))
            int_url = (
                f"https://mediaretriever.iaai.com/api/InteriorImageRetriever"
                f"?tenant={tenant}&partitionKey={partition_key}"
            )
            result.append((int_url, True, False, None))
        else:
            logger.warning("Lot %s -> Failed to download HTML for 360", stock_id)
    else:
        logger.info("Lot %s -> 360 Skipped", stock_id)

    return result


def _extract_images_manheim(lot: dict[str, Any]) -> list[tuple[str, bool, bool, int | None]]:
    return [
        (url, False, False, None)
        for url in (lot.get("images") or [])
        if isinstance(url, str)
    ]


def _extract_images_copart(lot: dict[str, Any]) -> list[tuple[str, bool, bool, int | None]]:
    result     = []
    images_list = lot.get("imagesList") or lot.get("data", {}).get("imagesList", {})
    if not images_list:
        return result

    for img_obj in images_list.get("IMAGE", []):
        full_url = img_obj.get("fullUrl")
        if full_url:
            result.append((full_url, False, False, None))

    for img_obj in images_list.get("EXTERIOR_360", []):
        base_url = img_obj.get("image360Url")
        frames   = img_obj.get("frameCount", 0)
        if base_url and frames > 0 and "_0." in base_url:
            prefix, ext = base_url.rsplit("_0.", 1)
            for i in range(frames):
                result.append((f"{prefix}_{i}.{ext}", True, False, i))

    for video_obj in images_list.get("ENGINE_VIDEO_SOUND", []):
        video_url = video_obj.get("highResUrl")
        if video_url:
            result.append((video_url, False, True, None))

    return result


# ---------------------------------------------------------------------------
# process_lot
# ---------------------------------------------------------------------------

async def process_lot(
    lot: dict[str, Any],
    downloader: ImageDownloader,
    uploader: MinioUploader,
) -> None:
    stock_id = (
        lot.get("stock_id")
        or lot.get("data", {}).get("lotDetails", {}).get("ln")
        or lot.get("lot_number")
        or lot.get("stockId")
        or lot.get("id")
        or (lot.get("data", {}).get("imagesList", {}).get("IMAGE", [{}])[0].get("lotNumberStr"))
    )

    if not stock_id:
        logger.warning("No stock_id. Preview: %s", str(lot)[:120])
        return

    auction = AUCTION_NAME.lower()
    if auction == "copart":
        images_data = _extract_images_copart(lot)
    elif auction == "iaai":
        images_data = await _extract_images_iaai(lot, downloader)
    elif auction == "manheim":
        images_data = _extract_images_manheim(lot)
    else:
        images_data = await _extract_images_iaai(lot, downloader)

    if not images_data:
        logger.info("Lot %s — no media found.", stock_id)
        return

    logger.info("Lot %s — %d media item(s)", stock_id, len(images_data))

    for url, is_360, is_video, frame_idx in images_data:
        if is_video:
            mt = "VIDEO"
        elif is_360:
            mt = f"360_EXTERIOR (frame {frame_idx})" if frame_idx is not None else "360_INTERIOR"
        else:
            mt = "NORMAL_PHOTO"
        logger.info("Lot %s -> %s: %s", stock_id, mt, url)

    results: list[bytes | None] = await asyncio.gather(
        *[downloader.fetch(item[0], str(stock_id)) for item in images_data]
    )

    batch = [
        (data, object_key(stock_id, url, is_360, is_video, frame_idx))
        for (url, is_360, is_video, frame_idx), data in zip(images_data, results)
        if data is not None
    ]

    if len(batch) < len(images_data):
        logger.warning("Lot %s — %d download(s) failed", stock_id, len(images_data) - len(batch))

    await uploader.upload_many(batch)
    logger.info("Lot %s — uploaded %d/%d", stock_id, len(batch), len(images_data))


# ---------------------------------------------------------------------------
# extract_lots
# ---------------------------------------------------------------------------

def extract_lots(value: Any) -> list[dict]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON, skipping. Error: %s | Preview: %s", exc, value[:150])
            return []

    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]

    if isinstance(value, dict):
        for key in ("lots", "data", "items", "results"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [v for v in nested if isinstance(v, dict)]
        return [value]

    logger.warning("Skipping record: unexpected type %s", type(value).__name__)
    return []


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

async def run() -> None:
    sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    img_connector = aiohttp.TCPConnector(
        limit=AIOHTTP_CONNECTOR_LIMIT,
        limit_per_host=AIOHTTP_LIMIT_PER_HOST or None,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    img_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,video/*,*/*;q=0.8",
    }

    # Опціональна SASL авторизація
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=BATCH_SIZE,
        security_protocol="PLAINTEXT",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # Старт з retry
    for attempt in range(1, 11):
        try:
            await consumer.start()
            logger.info(
                "Connected to Kafka %s | topic=%s | group=%s",
                KAFKA_BOOTSTRAP, TOPIC, GROUP_ID,
            )
            break
        except KafkaError as exc:
            logger.warning(
                "Kafka unavailable on startup (attempt %d/10): %r. Retrying in 10s...",
                attempt, exc,
            )
            await asyncio.sleep(10)
    else:
        logger.error("Could not connect to Kafka after 10 attempts. Exiting.")
        return

    async with aiohttp.ClientSession(connector=img_connector, headers=img_headers) as img_session:
        downloader = ImageDownloader(img_session, sem)
        uploader   = MinioUploader()

        try:
            while True:
                try:
                    # getmany повертає dict[TopicPartition, list[ConsumerRecord]]
                    records_map = await consumer.getmany(
                        timeout_ms=3000,
                        max_records=BATCH_SIZE,
                    )
                except KafkaError as exc:
                    logger.error("Kafka poll error: %r. Reconnecting in 5s...", exc)
                    await asyncio.sleep(5)
                    continue

                if not records_map:
                    logger.debug("No records — sleeping %.1fs", EMPTY_POLL_SLEEP)
                    await asyncio.sleep(EMPTY_POLL_SLEEP)
                    continue

                all_records = [r for records in records_map.values() for r in records]
                logger.info("Polled %d record(s)", len(all_records))

                all_lots = [
                    lot
                    for record in all_records
                    for lot in extract_lots(record.value)
                ]

                if all_lots:
                    t0 = asyncio.get_event_loop().time()
                    results = await asyncio.gather(
                        *[process_lot(lot, downloader, uploader) for lot in all_lots],
                        return_exceptions=True,
                    )
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            logger.error("Unhandled error in lot[%d]: %r", i, r)
                    logger.info(
                        "Batch done — %d lots in %.2fs",
                        len(all_lots),
                        asyncio.get_event_loop().time() - t0,
                    )

                # Комітимо тільки після успішної обробки батча
                try:
                    await consumer.commit()
                except KafkaError as exc:
                    logger.error("Commit failed: %r. Will re-process on next restart.", exc)

        except asyncio.CancelledError:
            logger.info("Cancelled.")
        except KeyboardInterrupt:
            logger.info("Interrupted.")
        finally:
            await consumer.stop()
            logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(run())