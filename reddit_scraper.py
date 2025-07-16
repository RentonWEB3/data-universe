import os
import json
import time
from datetime import datetime
import praw

CONFIG_PATH = "config.json"

def load_config():
    return json.load(open(CONFIG_PATH, "r", encoding="utf-8"))

def scrape_reddit():
    cfg = load_config()
    reddit_cfg = cfg["reddit"]
    reddit = praw.Reddit(
        client_id=reddit_cfg["client_id"],
        client_secret=reddit_cfg["client_secret"],
        user_agent=reddit_cfg["user_agent"]
    )

    entities = []
    per_sub = cfg.get("posts_per_subreddit", 20)
    for sub_name in cfg["search_subreddits"]:
        print(f"--- Получаем новые посты r/{sub_name} (limit={per_sub})")
        try:
            subreddit = reddit.subreddit(sub_name)
            for post in subreddit.new(limit=per_sub):
                text = post.selftext or post.title or ""
                if not text.strip():
                    continue
                uri = f"https://reddit.com{post.permalink}"
                created_at = datetime.utcfromtimestamp(post.created_utc).isoformat()
                content_bytes = text.encode("utf-8")
                entities.append({
                    "uri": uri,
                    "datetime": created_at,
                    "source": "REDDIT",
                    "label": sub_name,
                    "content": text,
                    "content_size_bytes": len(content_bytes)
                })
            time.sleep(2)
        except Exception as e:
            print(f"ERROR: при скрапинге r/{sub_name}: {e}")

    # дедупликация по uri
    seen = set()
    unique = []
    for e in entities:
        if e["uri"] in seen:
            continue
        seen.add(e["uri"])
        unique.append(e)
    print(f"Всего Reddit-сущностей: {len(unique)}")

    os.makedirs("normalized", exist_ok=True)
    out_file = f"normalized/reddit_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for e in unique:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"Сохранено: {out_file}")

    return unique

if __name__ == "__main__":
    scrape_reddit()
