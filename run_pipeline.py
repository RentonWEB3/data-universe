

import argparse

import os

import datetime

import logging

import sys



from huggingface_hub import HfApi



# Добавляем текущую директорию в sys.path

sys.path.append(os.path.dirname(__file__))



# Импорты (при необходимости адаптируй пути)

from twikit_scraper import scrape_tweets

from normalize_twitter import normalize_twitter

from normalize_reddit import normalize_reddit

from add_weights import apply_desirability_weight

from common.data import DataEntityBucket

from scraping.reddit_scraper import scrape_reddit_posts



logging.basicConfig(level=logging.INFO)



def main():

    parser = argparse.ArgumentParser(description="Unified Twitter + Reddit pipeline with HF upload")

    parser.add_argument('--twitter_screen_name', type=str, required=True)

    parser.add_argument('--twitter_keywords', type=str, required=True)

    parser.add_argument('--reddit_subreddits', type=str, required=True)

    parser.add_argument('--limit', type=int, default=100)

    parser.add_argument('--hf_repo', type=str, default="RentonWEB3/crypto-data")

    parser.add_argument('--hf_token', type=str, required=True)

    args = parser.parse_args()



    logging.info("📡 Scraping Twitter...")

    twitter_raw = scrape_tweets(

        screen_name=args.twitter_screen_name,

        keywords=args.twitter_keywords.split(','),

        limit=args.limit

    )



    logging.info("📡 Scraping Reddit...")

    reddit_raw = scrape_reddit_posts(

        subreddits=args.reddit_subreddits.split(','),

        limit=args.limit

    )



    logging.info("🧼 Normalizing...")

    twitter_entities = normalize_twitter(twitter_raw)

    reddit_entities = normalize_reddit(reddit_raw)

    all_entities = twitter_entities + reddit_entities



    logging.info("⚖️ Applying desirability weights...")

    weighted_entities = apply_desirability_weight(all_entities)



    bucket = DataEntityBucket()

    for entity in weighted_entities:

        bucket.add(entity)



    os.makedirs("output", exist_ok=True)

    filename = f"output/data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json.lz4"

    bucket.save_to_file(filename)

    logging.info(f"✅ Saved: {filename}")



    logging.info("🚀 Uploading to Hugging Face...")

    hf = HfApi()

    hf.upload_file(

        path_or_fileobj=filename,

        path_in_repo=os.path.basename(filename),

        repo_id=args.hf_repo,

        repo_type="dataset",

        token=args.hf_token

    )



    logging.info("🎯 Done! File sent for validation by subnet.")



if __name__ == "__main__":

    main()

