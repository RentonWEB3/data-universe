

import os
import json
import asyncio
from datetime import datetime as _dt
from twikit import Client, errors as twikit_errors

def load_config():
    """Читает config.json из корня проекта."""
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def is_valid(tweet) -> bool:
    """Проверяет, что у твита есть id, текст и время."""
    return getattr(tweet, "id", None) and getattr(tweet, "text", None) and getattr(tweet, "created_at", None)

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
            print("Пауза 20 сек для троттлинга…")
            await asyncio.sleep(20)
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
                "datetime": str(tweet.created_at),  # чтобы совпадало с DataEntity
                "content": tweet.text,                     # legacy
                "label": {"keyword": term},
                "content_size_bytes": len(text.encode("utf-8")),
                "source": 2
             })

        # небольшая пауза между ключевыми словами
        await asyncio.sleep(1)

    # дедупликация
        seen = set()
        unique = []
        for e in entities:
            uri = e.get("uri")
            if not uri or uri in seen:
                continue
            seen.add(uri)
            unique.append(e)
        print(f"После дедупликации: {len(unique)} твитов")

    # сохраняем в normalized/twitter_YYYYMMDD_HHMM.jsonl
    os.makedirs("normalized", exist_ok=True)
    out_file = f"normalized/twitter_{_dt.utcnow().strftime('%Y%m%d_%H%M')}.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for e in unique:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"Сохранено: {out_file}")

    return unique

if __name__ == "__main__":
    asyncio.run(scrape_twitter())
