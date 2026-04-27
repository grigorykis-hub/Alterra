import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]

PRIMARY_DATA_FILE = ROOT / "web" / "data" / "alterra_metrics.json"
FALLBACK_DATA_FILE = ROOT / "scripts" / "web" / "data" / "alterra_metrics.json"
DATA_FILE = PRIMARY_DATA_FILE if PRIMARY_DATA_FILE.exists() else FALLBACK_DATA_FILE

PRIMARY_INPUT_DIR = ROOT / "data"
FALLBACK_INPUT_DIR = ROOT / "scripts" / "data"
INPUT_DIR = PRIMARY_INPUT_DIR if PRIMARY_INPUT_DIR.exists() else FALLBACK_INPUT_DIR

DEFAULT_CHANNELS = ("Сайт", "Telegram", "VK")


def safe_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def read_csv_rows(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def calc_channel_metrics(name, traffic, leads, shows, bookings, deals, ad_spend, margin):
    cpl = round(ad_spend / leads, 2) if leads > 0 else 0
    cac = round(ad_spend / deals, 2) if deals > 0 else 0
    romi = round((margin - ad_spend) / ad_spend, 2) if ad_spend > 0 else 0
    return {
        "name": name,
        "traffic": int(traffic),
        "leads": int(leads),
        "cpl": cpl,
        "shows": int(shows),
        "bookings": int(bookings),
        "deals": int(deals),
        "cac": cac,
        "romi": romi,
    }


def load_channels_from_csv(path: Path):
    rows = read_csv_rows(path)
    if not rows:
        return []

    grouped = {}
    for row in rows:
        channel = (row.get("channel") or "").strip()
        if not channel:
            continue
        grouped.setdefault(
            channel,
            {
                "traffic": 0,
                "leads": 0,
                "shows": 0,
                "bookings": 0,
                "deals": 0,
                "ad_spend": 0.0,
                "margin": 0.0,
            },
        )
        grouped[channel]["traffic"] += safe_int(row.get("traffic"))
        grouped[channel]["leads"] += safe_int(row.get("leads"))
        grouped[channel]["shows"] += safe_int(row.get("shows"))
        grouped[channel]["bookings"] += safe_int(row.get("bookings"))
        grouped[channel]["deals"] += safe_int(row.get("deals"))
        grouped[channel]["ad_spend"] += safe_float(row.get("ad_spend"))
        grouped[channel]["margin"] += safe_float(row.get("margin"))

    return [
        calc_channel_metrics(
            name=channel,
            traffic=vals["traffic"],
            leads=vals["leads"],
            shows=vals["shows"],
            bookings=vals["bookings"],
            deals=vals["deals"],
            ad_spend=vals["ad_spend"],
            margin=vals["margin"],
        )
        for channel, vals in grouped.items()
    ]


def vk_api_request(method, params):
    token = os.getenv("VK_API_TOKEN", "").strip()
    if not token:
        return None

    query = {
        **params,
        "access_token": token,
        "v": os.getenv("VK_API_VERSION", "5.199"),
    }
    url = f"https://api.vk.com/method/{method}?{urlencode(query)}"
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if payload.get("error"):
        raise RuntimeError(f"VK API error: {payload['error'].get('error_msg')}")
    return payload.get("response")


def load_vk_posts_from_api():
    group_id = os.getenv("VK_GROUP_ID", "").strip()
    if not group_id:
        return []

    raw = vk_api_request(
        "wall.get",
        {
            "owner_id": f"-{group_id}",
            "count": int(os.getenv("VK_POSTS_LIMIT", "20")),
            "filter": "owner",
            "extended": 0,
        },
    )
    if not raw:
        return []

    items = raw.get("items", [])
    now = datetime.now(timezone.utc).date()
    lookback_days = int(os.getenv("VK_LOOKBACK_DAYS", "45"))
    min_date = now - timedelta(days=lookback_days)

    posts = []
    for item in items:
        post_date = datetime.fromtimestamp(item.get("date", 0), tz=timezone.utc).date()
        if post_date < min_date:
            continue

        views = safe_int((item.get("views") or {}).get("count"))
        likes = safe_int((item.get("likes") or {}).get("count"))
        comments = safe_int((item.get("comments") or {}).get("count"))
        reposts = safe_int((item.get("reposts") or {}).get("count"))
        interactions = likes + comments + reposts
        er = round((interactions / views) * 100, 2) if views > 0 else 0

        text = (item.get("text") or "").strip().replace("\n", " ")
        topic = text[:40] + "..." if len(text) > 40 else (text or f"Пост #{item.get('id')}")
        has_video = bool(item.get("attachments")) and any(a.get("type") == "video" for a in item["attachments"])
        has_photo = bool(item.get("attachments")) and any(a.get("type") == "photo" for a in item["attachments"])
        content_format = "Видео" if has_video else ("Фото" if has_photo else "Пост")

        posts.append(
            {
                "date": str(post_date),
                "topic": topic,
                "format": content_format,
                "reach": views,
                "er": er,
                "ctr": 0.0,
                "clicks": 0,
                "leads": 0,
                "cpl": 0,
                "shows": 0,
                "bookings": 0,
                "deals": 0,
                "romi": 0.0,
            }
        )

    return posts


def merge_vk_posts_with_csv(vk_posts, csv_path: Path):
    rows = read_csv_rows(csv_path)
    if not rows:
        return vk_posts

    by_date = {}
    for post in vk_posts:
        by_date.setdefault(post["date"], []).append(post)

    for row in rows:
        date = (row.get("date") or "").strip()
        if not date:
            continue

        target = by_date.get(date, [])
        if target:
            post = target[0]
            post["clicks"] += safe_int(row.get("clicks"))
            post["leads"] += safe_int(row.get("leads"))
            post["shows"] += safe_int(row.get("shows"))
            post["bookings"] += safe_int(row.get("bookings"))
            post["deals"] += safe_int(row.get("deals"))

            ad_spend = safe_float(row.get("ad_spend"))
            margin = safe_float(row.get("margin"))
            post["cpl"] = round(ad_spend / post["leads"], 2) if post["leads"] > 0 else 0
            post["ctr"] = round((post["clicks"] / post["reach"]) * 100, 2) if post["reach"] > 0 else 0
            post["romi"] = round((margin - ad_spend) / ad_spend, 2) if ad_spend > 0 else 0
        else:
            reach = safe_int(row.get("reach"))
            clicks = safe_int(row.get("clicks"))
            leads = safe_int(row.get("leads"))
            ad_spend = safe_float(row.get("ad_spend"))
            margin = safe_float(row.get("margin"))
            er = safe_float(row.get("er"))
            ctr = safe_float(row.get("ctr"))
            if ctr == 0 and reach > 0:
                ctr = round((clicks / reach) * 100, 2)

            vk_posts.append(
                {
                    "date": date,
                    "topic": (row.get("topic") or "VK пост").strip(),
                    "format": (row.get("format") or "Пост").strip(),
                    "reach": reach,
                    "er": er,
                    "ctr": ctr,
                    "clicks": clicks,
                    "leads": leads,
                    "cpl": round(ad_spend / leads, 2) if leads > 0 else 0,
                    "shows": safe_int(row.get("shows")),
                    "bookings": safe_int(row.get("bookings")),
                    "deals": safe_int(row.get("deals")),
                    "romi": round((margin - ad_spend) / ad_spend, 2) if ad_spend > 0 else 0,
                }
            )

    vk_posts.sort(key=lambda x: x["date"], reverse=True)
    return vk_posts


def rebuild_funnel(channels):
    leads = sum(x["leads"] for x in channels)
    return {
        "traffic": sum(x["traffic"] for x in channels),
        "leads": leads,
        "qualified": int(round(leads * 0.54)),
        "shows": sum(x["shows"] for x in channels),
        "bookings": sum(x["bookings"] for x in channels),
        "deals": sum(x["deals"] for x in channels),
    }


def ensure_channels(channels, fallback):
    if channels:
        return channels
    existing_by_name = {row["name"]: row for row in fallback}
    return [existing_by_name[name] for name in DEFAULT_CHANNELS if name in existing_by_name]


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Не найден файл метрик: {DATA_FILE}")

    with DATA_FILE.open("r", encoding="utf-8") as f:
        current_data = json.load(f)

    channels_csv = Path(os.getenv("CHANNELS_CSV_PATH", INPUT_DIR / "channels_metrics.csv"))
    vk_posts_csv = Path(os.getenv("VK_POSTS_CSV_PATH", INPUT_DIR / "vk_posts_metrics.csv"))

    channels = load_channels_from_csv(channels_csv)
    channels = ensure_channels(channels, current_data.get("channels", []))

    vk_posts = load_vk_posts_from_api()
    if not vk_posts:
        vk_posts = current_data.get("vk_posts", [])
    vk_posts = merge_vk_posts_with_csv(vk_posts, vk_posts_csv)

    updated_data = {
        **current_data,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "channels": channels,
        "funnel": rebuild_funnel(channels),
        "vk_posts": vk_posts,
    }

    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(updated_data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Updated: {DATA_FILE}")
    print(f"Channels source: {'CSV' if channels_csv.exists() else 'fallback JSON'}")
    print(f"VK source: {'VK API' if os.getenv('VK_API_TOKEN') and os.getenv('VK_GROUP_ID') else 'fallback JSON'}")


if __name__ == "__main__":
    main()
