r"""
lanch:

PS D:\Automoto\Programs\IAAI\iaai_photos_downloader>
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
import aiohttp, uuid, socket
from dotenv import find_dotenv, load_dotenv
#
env_path = find_dotenv(".env")
load_dotenv(env_path)


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip().strip('"').strip("'")


PANDAPROXY_URL: str   = _env("PANDAPROXY_URL")
TOPIC:          str   = _env("TOPIC",           "iaai-raw-lots")
_hostname = socket.gethostname()
_pod_suffix = _hostname.split("-")[-1]

# GROUP_ID тепер статичний для всіх подів (прибрали динамічний суфікс)
GROUP_ID = "saver-worker-group"
base_name = _env("CONSUMER_NAME", "image-saver-worker")
CONSUMER_NAME = f"{base_name}-{uuid.uuid4().hex[:8]}"
KAFKA_USER:     str   = _env("KAFKA_USER")
KAFKA_TOKEN:    str   = _env("KAFKA_TOKEN")

POLL_TIMEOUT_MS:  int   = int(_env("POLL_TIMEOUT_MS",   "3000"))
POLL_MAX_BYTES:   int   = int(_env("POLL_MAX_BYTES",     str(500 * 1024)))
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


def md5_of(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()

def object_key(stock_id: str | int, url: str, is_360: bool = False, is_video: bool = False, frame_idx: int | None = None) -> str:
    # 1. Для відео
    if is_video:
        return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/video/{md5_of(url)}.mp4"

    # 2. Для 360-фото
    if is_360:
        if frame_idx is None:
            # Якщо номера кадру немає — це салон
            return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/360_interior/{md5_of(url)}.jpg"
        else:
            # Якщо є номер кадру — це зовнішня панорама
            return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/360/{md5_of(url)}.jpg"

    # 3. Для звичайних фото
    return f"{IMG_PREFIX}/{AUCTION_NAME}/{stock_id}/{md5_of(url)}.jpg"

def _basic_auth_header() -> dict[str, str]:
    if KAFKA_USER and KAFKA_TOKEN:
        import base64
        token = base64.b64encode(f"{KAFKA_USER}:{KAFKA_TOKEN}".encode()).decode()
        return {"Authorization": f"Basic {token}"}
    return {}

class PandaProxyConsumer:
    _PP_CT     = "application/vnd.kafka.v2+json"
    _PP_ACCEPT = "application/vnd.kafka.json.v2+json"

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session  = session
        self._base_uri = None
        self._auth     = _basic_auth_header()

    @property
    def _h_ct(self) -> dict:
        return {**self._auth, "Content-Type": self._PP_CT}

    async def _post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        return self._session.post(url, ssl=False, headers=self._h_ct, **kwargs)

    async def create(self) -> None:
        url = f"{PANDAPROXY_URL}/consumers/{GROUP_ID}"

        for attempt in range(1, 6):
            # Генеруємо нове унікальне ім'я: база + суфікс пода + uuid
            new_uuid = uuid.uuid4().hex[:8]
            current_name = f"{base_name}-{new_uuid}"
            self._base_uri = f"{PANDAPROXY_URL}/consumers/{GROUP_ID}/instances/{current_name}"

            payload = {
                "name":              current_name,
                "format":            "json",
                "auto.offset.reset": "earliest"
            }
            try:
                async with self._session.post(
                    url, json=payload, headers=self._h_ct, ssl=False,
                ) as resp:

                    # --- ДОДАНО ЛОГУВАННЯ 422 ПОМИЛКИ ---
                    if resp.status >= 400 and resp.status != 409:
                        error_body = await resp.text()
                        logger.error("HTTP %s on create — payload: %s | response: %s", resp.status, payload, error_body)
                        resp.raise_for_status()
                    # ------------------------------------

                    if resp.status == 409:
                        await resp.text()
                        logger.warning("Consumer already exists — deleting and recreating …")
                        await self._force_delete()
                        await asyncio.sleep(1)
                        continue  # На наступній ітерації буде нове ім'я ✓

                    resp.raise_for_status()
                    data = await resp.json()
                    if "base_uri" in data:
                        self._base_uri = data["base_uri"]

                logger.info("Consumer created — base_uri=%s", self._base_uri)
                return

            except aiohttp.ClientResponseError as exc:
                if exc.status in (500, 502, 503, 504) and attempt < 5:
                    logger.warning("Proxy unavailable (HTTP %s). Retrying in 3s...", exc.status)
                    await asyncio.sleep(3)
                else:
                    raise # Кидаємо помилку далі, якщо це не 50x або закінчились спроби

    async def subscribe(self) -> None:
        async with self._session.post(
            f"{self._base_uri}/subscription",
            json={"topics": [TOPIC]},
            headers=self._h_ct,
            ssl=False,
        ) as resp:
            resp.raise_for_status()
        logger.info("Subscribed to topic '%s'", TOPIC)

    async def _recover_offset_out_of_range(self) -> bool:
        """Повертає True якщо seek вдався, False якщо треба перестворити консумер."""
        for attempt in range(1, 4):
            try:
                async with self._session.get(
                    f"{self._base_uri}/assignments",
                    headers={**self._auth, "Accept": "application/vnd.kafka.v2+json"},
                    ssl=False,
                ) as resp:
                    if resp.status == 404:
                        return False  # Сесія мертва — перестворювати
                    resp.raise_for_status()
                    data = await resp.json()
                    partitions = data.get("partitions", [])

                if not partitions:
                    # Assignments ще не прийшли після rebalance — чекаємо
                    logger.warning("No partitions assigned yet (attempt %d/3), waiting...", attempt)
                    await asyncio.sleep(2)
                    continue

                async with self._session.post(
                    f"{self._base_uri}/positions/end",  # <--- Змінюємо на end
                    json={"partitions": partitions},
                    headers={**self._auth, "Content-Type": "application/vnd.kafka.v2+json"},
                    ssl=False,
                ) as resp:
                    if resp.status == 404:
                        return False
                    resp.raise_for_status()

                logger.info("Seek to earliest succeeded for partitions: %s", partitions)
                return True

            except Exception as exc:
                logger.warning("Seek attempt %d/3 failed: %r", attempt, exc)
                await asyncio.sleep(2)

        return False  # Всі спроби вичерпані

    async def poll(self) -> list[dict]:
        url = (
            f"{self._base_uri}/records"
            f"?timeout={POLL_TIMEOUT_MS}&max_bytes={POLL_MAX_BYTES}"
        )
        for attempt in range(1, 6):
            try:
                async with self._session.get(
                    url,
                    headers={**self._auth, "Accept": self._PP_ACCEPT},
                    ssl=False,
                ) as resp:

                    if resp.status == 400:
                        error_text = await resp.text()
                        if "40002" in error_text or "offset_out_of_range" in error_text:
                            logger.warning("Caught offset_out_of_range (40002). Attempting seek...")
                            seek_ok = await self._recover_offset_out_of_range()
                            if seek_ok:
                                continue  # Seek вдався — повторюємо poll на живій сесії
                            else:
                                # Сесія мертва — піднімаємо 404 щоб run() перестворив
                                raise aiohttp.ClientResponseError(
                                    resp.request_info, resp.history, status=404
                                )
                        else:
                            logger.error("Poll failed HTTP 400: %s", error_text)
                            resp.raise_for_status()

                    if resp.status == 404:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=404
                        )

                    resp.raise_for_status()
                    return await resp.json()

            except aiohttp.ClientResponseError as exc:
                if exc.status == 404:
                    raise  # Піднімаємо вгору в run() — там є обробник
                raise

            except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as exc:
                logger.warning("Poll connection error (attempt %d/5): %r", attempt, exc)
                await asyncio.sleep(min(2 ** attempt, 30))

        logger.error("Poll failed after 5 attempts — returning empty")
        return []

    async def commit(self, records: list[dict]) -> None:
        if not records:
            return

        high: dict[tuple[str, int], int] = {}
        for r in records:
            k = (r["topic"], r["partition"])
            high[k] = max(high.get(k, -1), r["offset"])

        offsets_list = [
            {"topic": t, "partition": p, "offset": o + 1}
            for (t, p), o in high.items()
        ]

        payload = {"partitions": offsets_list}
        payload_str = json.dumps(payload, separators=(',', ':'))
        payload_bytes = payload_str.encode("utf-8")

        headers = {
            **self._auth,
            "Content-Type": "application/vnd.kafka.v2+json",
            "Accept": "application/vnd.kafka.v2+json"
        }

        async with self._session.post(
            f"{self._base_uri}/offsets",
            data=payload_bytes,
            headers=headers,
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                error_body = await resp.text()
                logger.error("Commit failed. Status: %d, Response: %s, Payload: %s",
                             resp.status, error_body, payload_str)
            resp.raise_for_status()

        logger.debug("Successfully committed %d partition(s)", len(offsets_list))

    async def delete(self) -> None:
        if not self._base_uri:
            return
        try:
            async with self._session.delete(
                self._base_uri, headers=self._h_ct, ssl=False,
            ) as resp:
                resp.raise_for_status()
            logger.info("Consumer instance deleted.")
        except Exception as exc:
            logger.warning("Could not delete consumer: %r", exc)

    async def _force_delete(self) -> None:
        try:
            async with self._session.delete(
                self._base_uri, headers=self._h_ct, ssl=False,
            ) as resp:
                logger.info("Force-deleted stale consumer (status=%s)", resp.status)
        except Exception:
            pass

class ImageDownloader:
    def __init__(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> None:
        self._session = session
        self._sem     = sem
        self._timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_S)

    async def fetch(self, url: str, stock_id: str) -> bytes | None: # <--- ДОДАЛИ stock_id
        async with self._sem:
            try:
                async with self._session.get(url, timeout=self._timeout) as resp:
                    resp.raise_for_status()
                    return await resp.read()
            except aiohttp.ClientResponseError as exc:
                # Тепер ми чітко бачимо, для якого лота яка помилка (наприклад, 404)
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
        # Динамічно визначаємо ContentType залежно від розширення
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

# Уніфіковані парсери повертають список кортежів: (url, is_360, is_video, frame_idx)

async def _extract_images_iaai(lot: dict[str, Any], downloader: ImageDownloader) -> list[tuple[str, bool, bool, int | None]]:
    """Витягує лінки для IAAI (фото, відео та 360 з HTML)."""
    result = []
    images = lot.get("images") or lot.get("imageUrls") or lot.get("links") or []

    for url in images:
        if isinstance(url, str):
            result.append((url, False, False, None))

    video_url = lot.get("video_url")
    if video_url and isinstance(video_url, str):
        result.append((video_url, False, True, None))

    # --- ЛОГУВАННЯ ТА ПОШУК 360 ---
    image360_url = lot.get("image360Url")
    source_file = str(lot.get("source_file", ""))
    stock_id = str(lot.get("stock_id", ""))

    # ЛОГ 1: Перевіряємо, чи взагалі прийшов лінк на 360 з API
    logger.info("Lot %s -> RAW JSON image360Url: %s", stock_id, image360_url)

    partition_key = "".join(filter(str.isdigit, source_file))
    if not partition_key:
        partition_key = stock_id

    if image360_url and partition_key:
        logger.info("Lot %s -> Downloading HTML for 360...", stock_id)
        html_bytes = await downloader.fetch(image360_url, stock_id)

        if html_bytes:
            html_text = html_bytes.decode("utf-8", errors="ignore")

            tenant_match = re.search(r'var\s+tenant\s*=\s*"([^"]+)"', html_text)
            tenant = tenant_match.group(1) if tenant_match else "iaai"

            amount_match = re.search(r'data-amount-x="(\d+)"', html_text)

            # ЛОГ 2: Дивимось, що знайшли регулярки в HTML
            logger.info("Lot %s -> Regex found tenant: '%s', amount: '%s'",
                        stock_id, tenant, amount_match.group(1) if amount_match else "NOT_FOUND")

            if amount_match:
                amount = int(amount_match.group(1))
                for i in range(1, amount + 1):
                    url_360 = f"https://mediaretriever.iaai.com/api/ThreeSixtyImageRetriever?tenant={tenant}&partitionKey={partition_key}&imageOrder={i}"
                    result.append((url_360, True, False, i))

            # Інтер'єр (is_360=True, щоб збереглося в папці 360 як MD5)
            int_url = f"https://mediaretriever.iaai.com/api/InteriorImageRetriever?tenant={tenant}&partitionKey={partition_key}"
            result.append((int_url, True, False, None))
        else:
            logger.warning("Lot %s -> Failed to download HTML for 360", stock_id)
    else:
        logger.info("Lot %s -> 360 Skipped (No image360Url or partition_key)", stock_id)

    return result

def _extract_images_manheim(lot: dict[str, Any]) -> list[tuple[str, bool, bool, int | None]]:
    """Витягує лінки для manheim."""
    result = []
    images = lot.get("images") or []
    for url in images:
        if isinstance(url, str):
            result.append((url, False, False, None))
    return result

def _extract_images_copart(lot: dict[str, Any]) -> list[tuple[str, bool, bool, int | None]]:
    """Витягує звичайні фото, 360 та відео для Copart."""
    result = []

    # Шукаємо imagesList або в корені, або всередині "data" (якщо структура зміниться)
    images_list = lot.get("imagesList") or lot.get("data", {}).get("imagesList", {})

    if not images_list:
        return result

    # 1. Забираємо звичайні фото
    for img_obj in images_list.get("IMAGE", []):
        full_url = img_obj.get("fullUrl")
        if full_url:
            result.append((full_url, False, False, None))

    # 2. Генеруємо лінки для 360
    for img_obj in images_list.get("EXTERIOR_360", []):
        base_url = img_obj.get("image360Url")
        frames = img_obj.get("frameCount", 0)

        if base_url and frames > 0 and "_0." in base_url:
            prefix, ext = base_url.rsplit("_0.", 1)
            for i in range(frames):
                url_i = f"{prefix}_{i}.{ext}"
                result.append((url_i, True, False, i))

    # 3. Витягуємо відео
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
        logger.warning("No stock_id found in lot. First 120 chars: %s", str(lot)[:120])
        return

    # Динамічно обираємо екстрактор
    if AUCTION_NAME.lower() == "copart":
        images_data = _extract_images_copart(lot)
    elif AUCTION_NAME.lower() == "iaai":
        images_data = await _extract_images_iaai(lot, downloader)
    elif AUCTION_NAME.lower() == "manheim":
        images_data = _extract_images_manheim(lot)
    else:
        images_data = await _extract_images_iaai(lot, downloader)

    if not images_data:
        logger.info("Lot %s — no images/videos found by extractor.", stock_id)
        return

    logger.info("Lot %s — %d media item(s) to process", stock_id, len(images_data))

    # ==========================================
    # ДОДАНО ЛОГУВАННЯ ЗНАЙДЕНИХ ПОСИЛАНЬ
    # ==========================================
    for item in images_data:
        url, is_360, is_video, frame_idx = item
        if is_video:
            media_type = "VIDEO"
        elif is_360:
            # Розділяємо логування: якщо є кадр - це екстер'єр, якщо ні - інтер'єр
            media_type = f"360_EXTERIOR (frame {frame_idx})" if frame_idx else "360_INTERIOR"
        else:
            media_type = "NORMAL_PHOTO"

        logger.info("Lot %s -> Found %s: %s", stock_id, media_type, url)
    # ==========================================

    results: list[bytes | None] = await asyncio.gather(
        *[downloader.fetch(item[0], stock_id) for item in images_data]
    )

    # Оновлене розпакування (url, is_360, is_video, frame_idx)
    batch = [
        (data, object_key(stock_id, url, is_360, is_video, frame_idx))
        for (url, is_360, is_video, frame_idx), data in zip(images_data, results)
        if data is not None
    ]

    if len(batch) < len(images_data):
        logger.warning("Lot %s — %d download(s) failed", stock_id, len(images_data) - len(batch))

    await uploader.upload_many(batch)
    logger.info("Lot %s — uploaded %d/%d", stock_id, len(batch), len(images_data))


def extract_lots(value: Any) -> list[dict]:
    # 1. Якщо брокер віддав value як сирий рядок (String), пробуємо його розпарсити
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            # Логуємо помилку і шматок "битого" тексту для дебагу, пропускаємо файл
            logger.error("Invalid JSON detected, skipping record. Error: %s | Preview: %s...", exc, value[:150])
            return []

    # 2. Якщо це вже готовий список словників
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)] # Додатковий захист від сміття в списках

    # 3. Якщо це словник (JSON об'єкт)
    if isinstance(value, dict):
        # Шукаємо вкладені списки лотів за типовими ключами
        for key in ("lots", "data", "items", "results"):
            nested_value = value.get(key)
            if isinstance(nested_value, list):
                return [v for v in nested_value if isinstance(v, dict)]

        # Якщо вкладених масивів немає, вважаємо, що сам словник і є лотом
        return [value]

    # Якщо прийшло щось зовсім незрозуміле (None, int, float)
    logger.warning("Skipping record: unexpected data type %s", type(value).__name__)
    return []


async def run() -> None:
    sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    pp_connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)

    img_connector = aiohttp.TCPConnector(
        limit=AIOHTTP_CONNECTOR_LIMIT,
        limit_per_host=AIOHTTP_LIMIT_PER_HOST or None,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    # Трохи розширено Accept, щоб пускало відео-потоки без проблем
    img_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,video/*,*/*;q=0.8",
    }

    async with (
        aiohttp.ClientSession(connector=pp_connector) as pp_session,
        aiohttp.ClientSession(connector=img_connector, headers=img_headers) as img_session,
    ):
        consumer   = PandaProxyConsumer(pp_session)
        downloader = ImageDownloader(img_session, sem)
        uploader   = MinioUploader()

        await consumer.create()
        await consumer.subscribe()

        try:
            while True:
                try:
                    records = await consumer.poll()

                except aiohttp.ClientResponseError as exc:
                    # 404: Сесія протухла. Створюємо НОВИЙ консюмер.
                    if exc.status == 404:
                        logger.warning("Redpanda session expired (404). Reconnecting with fresh instance...")
                        await asyncio.sleep(2)
                        await consumer.create()
                        await consumer.subscribe()
                        continue

                    # 409: Rebalance in progress. Чекаємо.
                    elif exc.status == 409:
                        logger.warning("Group rebalance in progress (409). Waiting before next poll...")
                        await asyncio.sleep(3)
                        continue

                    # 400: Bad request. ПЕРЕСТВОРЮЄМО консюмер
                    elif exc.status == 400:
                        logger.error("Bad request on poll (400). Session state is corrupted. RECREATING consumer...")
                        await asyncio.sleep(2)
                        await consumer.create()
                        await consumer.subscribe()
                        continue
                    else:
                        raise

                except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError) as exc:
                    logger.error("Network drop in poll loop: %r. Redpanda proxy might be down. Reconnecting in 5s...", exc)
                    await asyncio.sleep(5)
                    # Відвал мережі означає, що проксі скоріш за все забув про нашу сесію.
                    # Тому не просто повторюємо poll, а повністю перестворюємо консюмер.
                    try:
                        await consumer.create()
                        await consumer.subscribe()
                    except Exception as recreate_exc:
                        logger.warning("Failed to recreate consumer after network drop: %r", recreate_exc)
                    continue

                if not records:
                    logger.debug("No records — sleeping %.1fs", EMPTY_POLL_SLEEP)
                    await asyncio.sleep(EMPTY_POLL_SLEEP)
                    continue

                logger.info("Polled %d record(s)", len(records))

                all_lots = [
                    lot
                    for record in records
                    for lot in extract_lots(record.get("value"))
                ]

                if all_lots:
                    t0 = asyncio.get_event_loop().time()
                    results = await asyncio.gather(
                        *[process_lot(lot, downloader, uploader) for lot in all_lots],
                        return_exceptions=True,
                    )
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            logger.error("Unhandled in lot[%d]: %r", i, r)
                    logger.info(
                        "Batch done — %d lots in %.2fs",
                        len(all_lots),
                        asyncio.get_event_loop().time() - t0,
                    )

                    try:
                        await consumer.commit(records)
                    except (aiohttp.ClientResponseError, asyncio.TimeoutError) as exc:
                        logger.error("Commit failed due to network/timeout: %r. Will re-process batch on next restart.", exc)
                        # Ми просто логуємо помилку. Скрипт піде на наступне коло,
                        # де у блоці try-except для `poll()` спрацює перепідключення.

        except asyncio.CancelledError:
            logger.info("Cancelled.")
        except KeyboardInterrupt:
            logger.info("Interrupted.")
        finally:
            logger.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(run())