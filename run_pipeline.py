#!/usr/bin/env python3
import os
import json
import asyncio
from datetime import datetime

# 1) Твиттер
from twikit_scraper import scrape_twitter

# 2) Реддит
from reddit_scraper import scrape_reddit

# 3) Загрузчик в БД
from loader import load_entities
from common.data import DataSource

# 4) Hugging Face
import bittensor as bt
from huggingface_utils.huggingface_uploader import DualUploader
from huggingface_utils.encoding_system import EncodingKeyManager

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

async def main():
    cfg = load_config()

    # ——— TWITTER ———
    twitter_data = await scrape_twitter()
    print(f"\nВсего твитов собрано: {len(twitter_data)}\n")

    # ——— REDDIT ———
    reddit_data = scrape_reddit()
    count = load_entities("normalized")
    print(f"\nВсего Reddit-записей добавлено в БД: {count}\n")

  # ——— HUGGING FACE UPLOAD ———
    db_path    = cfg["db_path"]
    s3_url     = cfg.get("s3_auth_url", "")
    state_file = cfg.get("state_file", "upload_state.json")

    subtensor = bt.subtensor()
    wallet    = bt.wallet()
    enc_mgr   = EncodingKeyManager()

    uploader = DualUploader(
        db_path=db_path,
        subtensor=subtensor,
        wallet=wallet,
        encoding_key_manager=enc_mgr,
        private_encoding_key_manager=enc_mgr,
        s3_auth_url=s3_url,
        state_file=state_file
    )

    # если у вас не настроен S3 — ставим заглушку
    if uploader.s3_auth is None:
        class DummyS3Auth:
            def get_credentials(self, *args, **kwargs):
                return None
        uploader.s3_auth = DummyS3Auth()

    print("=== Начинаем загрузку на Hugging Face ===")
    metadata = uploader.upload_sql_to_huggingface()
    print("\n=== Загрузка завершена. Метаданные: ===")
    for m in metadata:
        print(m)

if __name__ == "__main__":
    asyncio.run(main())
