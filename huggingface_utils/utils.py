"""Module for preprocessing Twitter and Reddit data with optimized performance and dual-key encoding."""

import json
import hashlib
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
import psutil
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import bittensor as bt
from huggingface_utils.encoding_system import EncodingKeyManager, encode_url
import re

# Constants
TWEET_DATASET_COLUMNS = ['text', 'label', 'tweet_hashtags', 'datetime', 'username_encoded', 'url_encoded']
REDDIT_DATASET_COLUMNS = ['text', 'label', 'dataType', 'communityName', 'datetime', 'username_encoded', 'url_encoded']

# Stats Related Constants
STATS_VERSION = "2.0.0"
DEFAULT_STATS_STRUCTURE = {
    "version": STATS_VERSION,
    "data_source": None,
    "summary": {
        "total_rows": 0,
        "last_update_dt": None,
        "start_dt": None,
        "end_dt": None,
        "update_history": [],
        "metadata": {}
    },
    "topics": []
}


def get_default_stats_structure() -> Dict[str, Any]:
    """
    Return a default stats structure with current version

    Returns:
        Dict[str, Any]: Default stats structure
    """
    return DEFAULT_STATS_STRUCTURE.copy()


def migrate_stats_to_v2(stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate stats from v1.0.0 to v2.0.0 format by removing update_history from topics
    and maintaining the simplified structure.

    Args:
        stats (Dict[str, Any]): Original stats dictionary

    Returns:
        Dict[str, Any]: Migrated stats in v2.0.0 format
    """
    if stats.get("version") == STATS_VERSION:
        return stats

    # Create new v2.0.0 structure
    new_stats = get_default_stats_structure()

    # Migrate basic fields
    new_stats["data_source"] = stats.get("data_source")

    # Migrate summary
    summary = stats.get("summary", {})
    new_stats["summary"].update({
        "total_rows": summary.get("total_rows", 0),
        "last_update_dt": summary.get("last_update_dt"),
        "start_dt": summary.get("start_dt"),
        "end_dt": summary.get("end_dt"),
        "update_history": summary.get("update_history", []),
        "metadata": summary.get("metadata", {})
    })

    # Migrate topics (removing update_history from topics)
    old_topics = stats.get("topics", [])
    new_topics = []

    for topic in old_topics:
        if not isinstance(topic, dict):
            continue

        new_topic = {
            "topic": topic.get("topic"),
            "topic_type": topic.get("topic_type"),
            "total_count": topic.get("total_count", 0),
            "total_percentage": topic.get("total_percentage", 0)
        }
        # Only add topic if it has valid data
        if all(new_topic.values()):
            new_topics.append(new_topic)

    new_stats["topics"] = new_topics
    return new_stats


def get_optimal_threads() -> int:
    """
    Determine optimal number of threads based on system resources.
    
    Returns:
        int: Optimal number of threads (between 2 and 8)
    """
    try:
        # Get CPU cores
        cpu_count = psutil.cpu_count(logical=False)  # Physical cores only
        if cpu_count is None:
            cpu_count = os.cpu_count() or 2  # Fallback to logical cores or 2
            
        # Get available memory in GB
        available_memory_gb = psutil.virtual_memory().available / (1024 ** 3)
        
        # Calculate threads based on resources
        if available_memory_gb < 4:  # Less than 4GB available
            optimal_threads = 2
        elif available_memory_gb < 8:  # Less than 8GB available
            optimal_threads = min(3, cpu_count)
        else:
            optimal_threads = min(cpu_count, 8)  # Cap at 8 threads
            
        # Always ensure at least 2 threads
        return max(2, optimal_threads)
    
    except Exception as e:
        bt.logging.warning(f"Error detecting system resources: {e}. Defaulting to 2 threads.")
        return 2


def generate_static_integer(hotkey: str, max_value: int = 256) -> int:
    """Generate a static integer from a string key."""
    hash_value = hashlib.sha1(hotkey.encode()).digest()[:8]
    return int.from_bytes(hash_value, byteorder='big') % max_value


def decode_content(content: bytes) -> Dict[str, Any]:
    """Decode JSON content with error handling."""
    try:
        if isinstance(content, bytes):
            return json.loads(content.decode('utf-8'))
        return json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return {}


def batch_encode(data: pd.Series, fernet, batch_size: int = 10000) -> pd.Series:
    """Efficiently encode data in batches using specified Fernet key."""
    result = np.empty(len(data), dtype=object)
    
    # Adjust batch size based on available memory
    available_memory_gb = psutil.virtual_memory().available / (1024 ** 3)
    if available_memory_gb < 4:
        batch_size = 5000
    elif available_memory_gb < 8:
        batch_size = 7500
    
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        # Vectorized null check
        mask = batch.notna()
        if mask.any():
            result[i:i+batch_size][mask] = [encode_url(url, fernet) for url in batch[mask]]
        result[i:i+batch_size][~mask] = None
        
    return pd.Series(result, index=data.index)


def parallel_encode_batch(items: pd.Series, fernet) -> pd.Series:
    """Encode items in parallel using thread pool."""
    if items.empty:
        return items
        
    # Get optimal thread count based on system resources
    n_threads = get_optimal_threads()
    bt.logging.info(f"Using {n_threads} threads for parallel encoding")
    
    # Calculate optimal chunk size based on data size and thread count
    total_items = len(items)
    chunk_size = max(1000, total_items // (n_threads * 2))
    
    # Split into chunks for parallel processing
    chunks = [items[i:i + chunk_size] for i in range(0, total_items, chunk_size)]
    
    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        encode_func = partial(batch_encode, fernet=fernet)
        results = list(executor.map(encode_func, chunks))
    
    return pd.concat(results) if results else pd.Series(dtype=object)

# файл: huggingface_utils/utils.py

import pandas as pd
import re
import bittensor as bt
from huggingface_utils.encoding_system import EncodingKeyManager

# файл: huggingface_utils/utils.py

import pandas as pd
import re
import bittensor as bt
from huggingface_utils.encoding_system import EncodingKeyManager

def preprocess_twitter_df(
    df: pd.DataFrame,
    encoding_key_manager: EncodingKeyManager,
    private_encoding_key_manager: EncodingKeyManager
) -> pd.DataFrame:
    """
    Приводим Twitter-данные к единому формату:
    - datetime — к pandas.Timestamp
    - text      — из поля content (строка)
    - tweet_hashtags — список хэштегов
    - label     — хэштеги, склеенные строкой (может быть пустая)
    - кодируем username и url (если есть колонки)
    """

    try:
        bt.logging.info(f"Starting Twitter preprocessing with {len(df)} rows")

        # Шаг 1: datetime
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

        # Шаг 2: переименуем content в text (если нет text)
        if "content" in df.columns and "text" not in df.columns:
            df = df.rename(columns={"content": "text"})
        elif "text" not in df.columns:
            df["text"] = ""

        # Шаг 3: извлекаем хэштеги
        def extract_tags(text: str) -> list:
            return re.findall(r"#\w+", text or "")

        df["tweet_hashtags"] = df["text"].apply(extract_tags)

        # Шаг 4: формируем label — склеиваем теги пробелом
        df["label"] = df["tweet_hashtags"].apply(lambda tags: " ".join(tags))

        # Шаг 5: НЕ удаляем ни одной строки — оставляем всё
        bt.logging.info(f"Twitter preprocessing done: {len(df)} rows remain")

        # Шаг 6: кодируем username и url, если они есть
        # (Если у вас нет колонок username/url, этот блок можно опустить.)
        if "username" in df.columns:
            public_fernet = encoding_key_manager.get_fernet()
            df["username_encoded"] = df["username"].apply(
                lambda u: public_fernet.encrypt(u.encode()).decode()
            )
        else:
            df["username_encoded"] = ""

        if "url" in df.columns:
            private_fernet = private_encoding_key_manager.get_fernet()
            df["url_encoded"] = df["url"].apply(
                lambda u: private_fernet.encrypt(u.encode()).decode()
            )
        else:
            df["url_encoded"] = ""

        # Оставляем только нужные колонки в нужном порядке
        return df[["text", "tweet_hashtags", "label", "datetime", "username_encoded", "url_encoded"]]

    except Exception as e:
        bt.logging.error(f"Error in preprocess_twitter_df: {e}")
        # Если что-то пошло не так, возвращаем пустой DF с правильными колонками
        return pd.DataFrame(columns=[
            "text", "tweet_hashtags", "label", "datetime", "username_encoded", "url_encoded"
        ])


# файл: huggingface_utils/utils.py

import pandas as pd
import json
import bittensor as bt
from huggingface_utils.encoding_system import EncodingKeyManager

def preprocess_reddit_df(
    df: pd.DataFrame,
    encoding_key_manager: EncodingKeyManager,
    private_encoding_key_manager: EncodingKeyManager
) -> pd.DataFrame:
    """
    Приводит Reddit-данные к единому формату:
    - datetime  — к pandas.Timestamp
    - text      — заголовок + тело поста/комментария
    - label     — имя сабреддита
    - dataType  — 'post' или 'comment'
    - communityName — название сабреддита
    - кодировка url, username (если нужно)
    """
    try:
        bt.logging.info(f"Starting Reddit preprocessing with {len(df)} rows")

        # 1) Преобразуем дату
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

        # 2) 'content' — строка JSON, раскодируем её
        def parse_content(x):
            try:
                return json.loads(x) if isinstance(x, str) else {}
            except:
                return {}

        parsed = df["content"].apply(parse_content)

        # 3) Вытащим поля: заголовок, тело, тип
        texts = parsed.apply(lambda c: (c.get("title") or "") + 
                                       (" " + c.get("selftext") if c.get("selftext") else ""))
        types = parsed.apply(lambda c: c.get("dataType", ""))  # 'post' или 'comment'
        communities = parsed.apply(lambda c: c.get("communityName", ""))

        result = pd.DataFrame({
            "text": texts,
            "dataType": types,
            "communityName": communities,
            "datetime": df["datetime"]
        })

        # 4) label = имя сабреддита
        result["label"] = result["communityName"]

        bt.logging.info(f"Reddit preprocessing done: {len(result)} rows remain")

        # 5) кодируем username/url, если они есть
        if "username" in df.columns:
            pub = encoding_key_manager.get_fernet()
            result["username_encoded"] = df["username"].apply(
                lambda u: pub.encrypt(u.encode()).decode() if u else ""
            )
        else:
            result["username_encoded"] = ""

        if "url" in df.columns:
            priv = private_encoding_key_manager.get_fernet()
            result["url_encoded"] = df["url"].apply(
                lambda u: priv.encrypt(u.encode()).decode() if u else ""
            )
        else:
            result["url_encoded"] = ""

        # Оставляем в таком порядке
        return result[[
            "text", "dataType", "communityName", "label", "datetime",
            "username_encoded", "url_encoded"
        ]]

    except Exception as e:
        bt.logging.error(f"Error in preprocess_reddit_df: {e}")
        # возвращаем пустую таблицу с нужными колонками
        return pd.DataFrame(columns=[
            "text", "dataType", "communityName", "label", "datetime",
            "username_encoded", "url_encoded"
        ])
