# scraping/custom_scrapers.py

import asyncio
import datetime as _dt

from common.data import DataEntity, DataSource, DataLabel
from scraping.scraper import ScrapeConfig

from twikit_scraper import scrape_twitter
import reddit_scraper

class CustomTwitterScraper:
    """
    Обёртка над twikit_scraper.scrape_twitter()
    """

    async def scrape(self, config: ScrapeConfig):
        tweets = await scrape_twitter()

        entities = []
        for t in tweets:
            dt_val = t.get("datetime")
            if isinstance(dt_val, str):
                try:
                    # Формат: 'Thu Jul 17 12:10:04 +0000 2025'
                    dt_obj = _dt.datetime.strptime(dt_val, '%a %b %d %H:%M:%S %z %Y')
                except Exception:
                    dt_obj = _dt.datetime.fromisoformat(dt_val)
            else:
                dt_obj = dt_val

            entities.append(DataEntity(
                uri=t["uri"],
                datetime=dt_obj,
                source=DataSource.X,
                label=DataLabel(value=t["label"]["name"]),
                content=t["content"].encode("utf-8"),
                content_size_bytes=t["content_size_bytes"]
            ))

        limit = config.entity_limit or len(entities)
        return entities[:limit]


class CustomRedditScraper:
    """
    Обёртка над reddit_scraper.scrape_reddit()
    """

    async def scrape(self, config: ScrapeConfig):
        posts = await asyncio.to_thread(reddit_scraper.scrape_reddit)

        entities = []
        for p in posts:
            entities.append(DataEntity(
                uri=p["uri"],
                datetime=p["datetime"],
                source=DataSource.REDDIT,
                label=DataLabel(value=p["label"]),
                content=p["content"].encode("utf-8"),
                content_size_bytes=p["content_size_bytes"]
            ))

        limit = config.entity_limit or len(entities)
        return entities[:limit]

