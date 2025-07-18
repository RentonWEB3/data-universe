import threading
from typing import Callable, Dict
from common.data import DataSource
from scraping.reddit.reddit_lite_scraper import RedditLiteScraper
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.scraper import Scraper, ScraperId
from scraping.x.microworlds_scraper import MicroworldsTwitterScraper
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.quacker_url_scraper import QuackerUrlScraper
from scraping.youtube.youtube_custom_scraper import YouTubeTranscriptScraper
from scraping.custom_scrapers import CustomTwitterScraper, CustomRedditScraper


DEFAULT_FACTORIES = {
    # Заменяем RedditLite на ваш кастомный
    ScraperId.REDDIT_LITE: CustomRedditScraper,
    ScraperId.REDDIT_CUSTOM: CustomRedditScraper,
    # Для Twitter добавляем X_CUSTOM
    ScraperId.X_CUSTOM: CustomTwitterScraper,
    # Сохраняем старые пути для других скраперов
    ScraperId.X_APIDOJO: ApiDojoTwitterScraper,
    ScraperId.X_FLASH: MicroworldsTwitterScraper,
    ScraperId.X_MICROWORLDS: MicroworldsTwitterScraper,
    ScraperId.X_QUACKER: QuackerUrlScraper,
    ScraperId.YOUTUBE_TRANSCRIPT: YouTubeTranscriptScraper
}


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def __init__(
        self, factories: Dict[DataSource, Callable[[], Scraper]] = DEFAULT_FACTORIES
    ):
        self.factories = factories

    def get(self, scraper_id: ScraperId) -> Scraper:
        """Returns a scraper for the given scraper id."""
        if scraper_id == ScraperId.X_CUSTOM:
            from scraping.custom_scrapers import CustomTwitterScraper
            return CustomTwitterScraper()
        if scraper_id == ScraperId.REDDIT_CUSTOM:
            from scraping.custom_scrapers import CustomRedditScraper
            return CustomRedditScraper()

        assert scraper_id in self.factories, f"Scraper id {scraper_id} not supported."

        return self.factories[scraper_id]()
