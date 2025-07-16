import os
import json
import asyncio
from datetime import datetime
from twikit import Client, errors as twikit_errors

CONFIG_PATH = "config.json"

def load_config():
    return json.load(open(CONFIG_PATH, "r", encoding="utf-8"))

def is_valid(tweet):
    text = getattr(tweet, "text", "").strip()
    return bool(text)

async def scrape_twitter():
    cfg = load_config()
    client = Client()
    client.load_cookies(cfg["cookies_file"])
    print(f"Куки загружены из файла: {cfg['cookies_file']}")

    entities = []
    for term in cfg["search_keywords"]:
        print(f"--- Поиск по ключевому слову: '{term}'")
        try:
            result = await client.search_tweet(term, "latest", cfg["tweets_per_keyword"])
        except twikit_errors.NotFound:
            print(f"WARNING: '{term}' вернул 404, пропускаем")
            continue
        except Exception as e:
            print(f"ERROR: при поиске '{term}': {e}, пропускаем")
            continue

        tweets = getattr(result, "data", result)
        for tweet in tweets:
            if not is_valid(tweet):
                continue
            text = tweet.text
            entities.append({
                "uri": f"https://twitter.com/i/web/status/{tweet.id}",
                "datetime": tweet.created_at,
                "source": "X",
                "label": {"name": term},
                "content": text,
                "content_size_bytes": len(text.encode("utf-8"))
            })

        # троттлинг
        await asyncio.sleep(20)

    # дедупликация по uri
    seen = set()
    unique = []
    for e in entities:
        if e["uri"] in seen:
            continue
        seen.add(e["uri"])
        unique.append(e)
    print(f"После дедупликации: {len(unique)} твитов")

    os.makedirs("normalized", exist_ok=True)
    out_file = f"normalized/twitter_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for e in unique:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"Сохранено: {out_file}")

    return unique

if __name__ == "__main__":
    asyncio.run(scrape_twitter())
