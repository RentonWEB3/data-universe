import asyncio

import argparse

import json

import os

import re

from datetime import datetime

from dateutil import parser as date_parser

from twikit import Client

from common.data import DataEntity



# === CLI ===

parser = argparse.ArgumentParser()

parser.add_argument("--screen_name", type=str, required=True, help="Twitter username (без @)")

parser.add_argument("--count", type=int, default=100, help="Количество твитов (по умолчанию 100)")

parser.add_argument("--keywords", type=str, help="Ключевые слова через запятую (например: ai,ethics,startup)")

args = parser.parse_args()



# === Настройки ===

COOKIES_PATH = "twitter_cookies.json"

OUTPUT_DIR = "normalized"

os.makedirs(OUTPUT_DIR, exist_ok=True)



FILENAME = f"twitter_{args.screen_name}_{datetime.now().strftime('%Y-%m-%d')}.jsonl"

OUTPUT_PATH = os.path.join(OUTPUT_DIR, FILENAME)



# === Фильтрация ===

keywords = [k.strip().lower() for k in args.keywords.split(",")] if args.keywords else []



def is_english(text):

    words = text.split()

    if not words:

        return False

    latin_words = [w for w in words if re.fullmatch(r"[A-Za-z0-9\-@#]+", w)]

    return len(latin_words) / len(words) > 0.8



def is_valid(tweet):

    text = tweet.full_text.strip()

    lower_text = text.lower()



    if lower_text.startswith("rt @") or lower_text.startswith("@"):

        return False  # ретвиты и реплаи



    if len(text) < 30 or "http" in lower_text:

        return False



    if keywords and not any(k in lower_text for k in keywords):

        return False



    if not is_english(text):

        return False



    return True



# === Асинхронная логика ===

async def main():

    client = Client()

    client.load_cookies(COOKIES_PATH)



    user = await client.get_user_by_screen_name(args.screen_name)

    tweets = await user.get_tweets(tweet_type="Tweets", count=args.count)



    entities = []



    for tweet in tweets:

        if not is_valid(tweet):

            continue



        entity = DataEntity(

            id=str(tweet.id),

            source=1,  # Twitter

            uri=f"https://twitter.com/{tweet.user.screen_name}/status/{tweet.id}",

            datetime=date_parser.parse(tweet.created_at).isoformat(),

            content=tweet.full_text.strip(),

            content_size_bytes=len(tweet.full_text.encode("utf-8")),

            labels=[],

            metadata={

                "user": tweet.user.screen_name

            }

        )

        entities.append(entity)



    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:

        for entity in entities:

            f.write(json.dumps(entity.model_dump(), ensure_ascii=False, default=str) + "\n")



    print(f"✅ Сохранено {len(entities)} твитов в {OUTPUT_PATH}")



# === Запуск ===

asyncio.run(main())


