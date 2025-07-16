import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any

import praw
from common.data import DataEntity  # Pydantic модель

def load_config() -> Dict[str, Any]:
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def scrape_reddit() -> List[Dict[str, Any]]:
    cfg = load_config()
    reddit_cfg = cfg["reddit"]
    reddit = praw.Reddit(
        client_id=reddit_cfg["client_id"],
        client_secret=reddit_cfg["client_secret"],
        user_agent=reddit_cfg["user_agent"]
    )

    entities: List[Dict[str, Any]] = []
    per_sub = cfg.get("posts_per_subreddit", 20)

    for sub_name in cfg["search_subreddits"]:
        print(f"--- Получаем новые посты r/{sub_name} (limit={per_sub})")
        try:
            subreddit = reddit.subreddit(sub_name)
            for post in subreddit.new(limit=per_sub):
                text = post.selftext.strip() or post.title.strip()
                if not text:
                    continue

                entities.append({
                   "datetime": datetime.utcfromtimestamp(post.created_utc).isoformat(),
                   "content": text,               # legacy field для HF остаётся content
                   "uri": f"https://reddit.com{post.permalink}",
                   "label": {"subreddit": sub_name},             # или пустая строка — по вашему усмотрению
                   "content_size_bytes": len(text.encode("utf-8")),
                   "source": 1

                })
            time.sleep(2)
        except Exception as e:
            print(f"ERROR: при скрапинге r/{sub_name}: {e}")

    print(f"Всего Reddit-сущностей: {len(entities)}")

    os.makedirs("normalized", exist_ok=True)
    out = f"normalized/reddit_{datetime.utcnow():%Y%m%d_%H%M}.jsonl"
    with open(out, "w", encoding="utf-8") as f:
        for e in entities:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"Сохранено: {out}")

    return entities

if __name__ == "__main__":
    scrape_reddit()
