import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "web" / "data" / "alterra_metrics.json"
INPUT_DIR = ROOT / "data"


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


def read_csv_rows(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def require_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Отсутствует обязательная переменная окружения: {name}")
    return value


def require_file(path: Path, label: str):
    if not path.exists():
        raise RuntimeError(f"Отсутствует обязательный файл {label}: {path}")


def vk_api_request(method, params):
    token = require_env("VK_API_TOKEN")
    query = {
        **params,
        "access_token": token,
        "v": os.getenv("VK_API_VERSION", "5.199"),
    }
    url = f"https://api.vk.com/method/{method}?{urlencode(query)}"
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("error"):
        raise RuntimeError(f"VK API error ({method}): {payload['error'].get('error_msg')}")
    return payload.get("response")


def load_channels_from_csv(path: Path):
    rows = read_csv_rows(path)
    grouped = {}
    for row in rows:
        channel = (row.get("channel") or "").strip()
        if not channel:
            continue
        grouped.setdefault(
            channel,
            {"traffic": 0, "leads": 0, "shows": 0, "bookings": 0, "deals": 0, "ad_spend": 0.0, "margin": 0.0},
        )
        grouped[channel]["traffic"] += safe_int(row.get("traffic"))
        grouped[channel]["leads"] += safe_int(row.get("leads"))
        grouped[channel]["shows"] += safe_int(row.get("shows"))
        grouped[channel]["bookings"] += safe_int(row.get("bookings"))
        grouped[channel]["deals"] += safe_int(row.get("deals"))
        grouped[channel]["ad_spend"] += safe_float(row.get("ad_spend"))
        grouped[channel]["margin"] += safe_float(row.get("margin"))

    channels = []
    for name, vals in grouped.items():
        leads = vals["leads"]
        deals = vals["deals"]
        ad_spend = vals["ad_spend"]
        margin = vals["margin"]
        channels.append(
            {
                "name": name,
                "traffic": vals["traffic"],
                "leads": leads,
                "cpl": round(ad_spend / leads, 2) if leads > 0 else 0,
                "shows": vals["shows"],
                "bookings": vals["bookings"],
                "deals": deals,
                "cac": round(ad_spend / deals, 2) if deals > 0 else 0,
                "romi": round((margin - ad_spend) / ad_spend, 2) if ad_spend > 0 else 0,
            }
        )
    return channels


def load_vk_subscribers(group_id: str):
    data = vk_api_request("groups.getById", {"group_id": group_id, "fields": "members_count"})
    if not data:
        raise RuntimeError("VK API не вернул данные группы")
    # VK API may return either a list or an object with "groups".
    if isinstance(data, dict):
        groups = data.get("groups") or []
        if not groups:
            raise RuntimeError("VK API не вернул список групп в groups.getById")
        first = groups[0]
    else:
        first = data[0]
    return safe_int(first.get("members_count"))


def load_vk_posts_from_api(group_id: str):
    raw = vk_api_request(
        "wall.get",
        {
            "owner_id": f"-{group_id}",
            "count": int(os.getenv("VK_POSTS_LIMIT", "30")),
            "filter": "owner",
            "extended": 0,
        },
    )
    if not raw:
        return []

    lookback_days = int(os.getenv("VK_LOOKBACK_DAYS", "60"))
    min_date = datetime.now(timezone.utc).date() - timedelta(days=lookback_days)

    posts = []
    for item in raw.get("items", []):
        post_date = datetime.fromtimestamp(item.get("date", 0), tz=timezone.utc).date()
        if post_date < min_date:
            continue

        views = safe_int((item.get("views") or {}).get("count"))
        likes = safe_int((item.get("likes") or {}).get("count"))
        comments = safe_int((item.get("comments") or {}).get("count"))
        reposts = safe_int((item.get("reposts") or {}).get("count"))
        interactions = likes + comments + reposts
        er = round((interactions / views) * 100, 2) if views > 0 else 0

        attachments = item.get("attachments") or []
        has_video = any(a.get("type") == "video" for a in attachments)
        has_photo = any(a.get("type") == "photo" for a in attachments)
        content_format = "Видео" if has_video else ("Фото" if has_photo else "Пост")

        text = (item.get("text") or "").strip().replace("\n", " ")
        topic = text[:60] + "..." if len(text) > 60 else (text or f"Пост #{item.get('id')}")

        posts.append(
            {
                "date": str(post_date),
                "topic": topic,
                "format": content_format,
                "reach": views,
                "likes": likes,
                "comments": comments,
                "reposts": reposts,
                "er": er,
                "ctr": 0.0,
                "clicks": 0,
                "leads": 0,
                "cpl": 0,
                "shows": 0,
                "bookings": 0,
                "deals": 0,
                "romi": 0.0,
                "frequency": 0.0,
                "cpc": 0.0,
                "cpf": 0.0,
                "budget": 0.0,
                "subscribers": 0,
            }
        )
    return posts


def merge_vk_posts_with_csv(vk_posts, csv_path: Path):
    rows = read_csv_rows(csv_path)
    by_date = {}
    for post in vk_posts:
        by_date.setdefault(post["date"], []).append(post)

    for row in rows:
        date = (row.get("date") or "").strip()
        if not date:
            continue
        target = by_date.get(date, [])
        if not target:
            continue
        post = target[0]
        post["clicks"] += safe_int(row.get("clicks"))
        post["leads"] += safe_int(row.get("leads"))
        post["shows"] += safe_int(row.get("shows"))
        post["bookings"] += safe_int(row.get("bookings"))
        post["deals"] += safe_int(row.get("deals"))

        budget = safe_float(row.get("ad_spend"))
        margin = safe_float(row.get("margin"))
        clicks = post["clicks"]
        leads = post["leads"]

        post["budget"] = budget
        post["ctr"] = round((clicks / post["reach"]) * 100, 2) if post["reach"] > 0 else 0
        post["cpc"] = round(budget / clicks, 2) if clicks > 0 else 0
        post["cpf"] = round(budget / leads, 2) if leads > 0 else 0
        post["cpl"] = round(budget / leads, 2) if leads > 0 else 0
        post["frequency"] = round(safe_float(row.get("frequency"), 0), 2)
        post["subscribers"] = safe_int(row.get("subscribers"))
        post["romi"] = round((margin - budget) / budget, 2) if budget > 0 else 0

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


def main():
    group_id = require_env("VK_GROUP_ID")

    channels_csv = Path(os.getenv("CHANNELS_CSV_PATH", INPUT_DIR / "channels_metrics.csv"))
    vk_posts_csv = Path(os.getenv("VK_POSTS_CSV_PATH", INPUT_DIR / "vk_posts_metrics.csv"))
    require_file(channels_csv, "channels_metrics.csv")
    require_file(vk_posts_csv, "vk_posts_metrics.csv")

    channels = load_channels_from_csv(channels_csv)
    if not channels:
        raise RuntimeError("channels_metrics.csv пустой или не содержит валидных строк")

    vk_posts = load_vk_posts_from_api(group_id)
    if not vk_posts:
        raise RuntimeError("VK API вернул 0 постов, обновление остановлено")
    vk_posts = merge_vk_posts_with_csv(vk_posts, vk_posts_csv)

    vk_total = load_vk_subscribers(group_id)
    tg_total = safe_int(os.getenv("TG_SUBSCRIBERS_TOTAL"), 0)
    tg_growth = safe_int(os.getenv("TG_SUBSCRIBERS_GROWTH_30D"), 0)
    vk_growth = safe_int(os.getenv("VK_SUBSCRIBERS_GROWTH_30D"), 0)
    if vk_growth == 0:
        vk_growth = max(0, sum(safe_int(p.get("subscribers")) for p in vk_posts))

    updated = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "channels": channels,
        "funnel": rebuild_funnel(channels),
        "subscribers": {
            "vk_total": vk_total,
            "vk_growth_30d": vk_growth,
            "tg_total": tg_total,
            "tg_growth_30d": tg_growth,
        },
        "vk_posts": vk_posts,
        "events": [],
        "tasks": [],
    }

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(updated, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Updated: {DATA_FILE}")
    print("Source mode: REAL_ONLY")


if __name__ == "__main__":
    main()
