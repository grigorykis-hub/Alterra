import json
import random
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

PRIMARY_DATA_FILE = ROOT / "web" / "data" / "alterra_metrics.json"
FALLBACK_DATA_FILE = ROOT / "scripts" / "web" / "data" / "alterra_metrics.json"


def get_data_file() -> Path:
    if PRIMARY_DATA_FILE.exists():
        return PRIMARY_DATA_FILE
    if FALLBACK_DATA_FILE.exists():
        return FALLBACK_DATA_FILE
    # Если файла нет — создаем в правильном месте
    PRIMARY_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    default_data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "channels": [
            {"name": "Сайт", "traffic": 0, "leads": 0, "cpl": 0, "shows": 0, "bookings": 0, "deals": 0, "cac": 0, "romi": 0},
            {"name": "Telegram", "traffic": 0, "leads": 0, "cpl": 0, "shows": 0, "bookings": 0, "deals": 0, "cac": 0, "romi": 0},
            {"name": "VK", "traffic": 0, "leads": 0, "cpl": 0, "shows": 0, "bookings": 0, "deals": 0, "cac": 0, "romi": 0},
        ],
        "funnel": {"traffic": 0, "leads": 0, "qualified": 0, "shows": 0, "bookings": 0, "deals": 0},
        "vk_posts": [],
    }
    with PRIMARY_DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(default_data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return PRIMARY_DATA_FILE


DATA_FILE = get_data_file()


def clamp(value, min_value):
    return max(min_value, value)


def bump_int(value, spread=0.05):
    delta = int(round(value * random.uniform(-spread, spread)))
    return clamp(value + delta, 0)


def bump_float(value, spread=0.08, min_value=0.0):
    delta = value * random.uniform(-spread, spread)
    return round(max(min_value, value + delta), 2)


def refresh_channels(channels):
    refreshed = []
    for item in channels:
        traffic = bump_int(item.get("traffic", 0), 0.07)
        leads = bump_int(item.get("leads", 0), 0.09)
        shows = bump_int(item.get("shows", 0), 0.1)
        bookings = bump_int(item.get("bookings", 0), 0.1)
        deals = bump_int(item.get("deals", 0), 0.1)
        cpl = bump_int(item.get("cpl", 0), 0.08)
        cac = bump_int(item.get("cac", 0), 0.08)
        romi = bump_float(item.get("romi", 0), 0.06, 0.0)

        refreshed.append(
            {
                **item,
                "traffic": traffic,
                "leads": leads,
                "shows": shows,
                "bookings": bookings,
                "deals": deals,
                "cpl": cpl,
                "cac": cac,
                "romi": romi,
            }
        )
    return refreshed


def refresh_vk_posts(posts):
    refreshed = []
    for post in posts:
        reach = bump_int(post.get("reach", 0), 0.08)
        clicks = bump_int(post.get("clicks", 0), 0.1)
        leads = bump_int(post.get("leads", 0), 0.1)
        shows = bump_int(post.get("shows", 0), 0.1)
        bookings = bump_int(post.get("bookings", 0), 0.1)
        deals = bump_int(post.get("deals", 0), 0.15)
        er = bump_float(post.get("er", 0), 0.07, 0.0)
        ctr = bump_float(post.get("ctr", 0), 0.07, 0.0)
        cpl = bump_int(post.get("cpl", 0), 0.09)
        romi = bump_float(post.get("romi", 0), 0.09, 0.0)

        refreshed.append(
            {
                **post,
                "reach": reach,
                "clicks": clicks,
                "leads": leads,
                "shows": shows,
                "bookings": bookings,
                "deals": deals,
                "er": er,
                "ctr": ctr,
                "cpl": cpl,
                "romi": romi,
            }
        )
    return refreshed


def rebuild_funnel(channels):
    traffic = sum(item.get("traffic", 0) for item in channels)
    leads = sum(item.get("leads", 0) for item in channels)
    shows = sum(item.get("shows", 0) for item in channels)
    bookings = sum(item.get("bookings", 0) for item in channels)
    deals = sum(item.get("deals", 0) for item in channels)
    qualified = int(round(leads * 0.54))
    return {
        "traffic": traffic,
        "leads": leads,
        "qualified": qualified,
        "shows": shows,
        "bookings": bookings,
        "deals": deals,
    }


def main():
    with DATA_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    random.seed(int(datetime.now(timezone.utc).strftime("%Y%m%d")))
    channels = refresh_channels(data.get("channels", []))
    vk_posts = refresh_vk_posts(data.get("vk_posts", []))
    funnel = rebuild_funnel(channels)

    updated = {
        **data,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "channels": channels,
        "funnel": funnel,
        "vk_posts": vk_posts,
    }

    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(updated, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Updated {DATA_FILE}")


if __name__ == "__main__":
    main()
