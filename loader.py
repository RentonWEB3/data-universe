import os
import json
from typing import List
from datetime import datetime
from dateutil import parser as date_parser

from common.data import DataEntity, DataLabel, DataSource


def to_data_entity(data: dict) -> DataEntity:
    """
    Преобразует словарь JSON в объект DataEntity.
    Ожидает поля:
      - uri: str
      - datetime: ISO-строка или datetime
      - source: int (1=Reddit, 2=Twitter)
      - label: Optional[str]
      - content: str или bytes
      - content_size_bytes: int
    """

    # 1) datetime может быть строкой — парсим
    dt_raw = data.get("datetime")
    if isinstance(dt_raw, str):
        dt_obj = date_parser.parse(dt_raw)
    elif isinstance(dt_raw, dt.datetime):
        dt_obj = dt_raw
    else:
        raise ValueError(f"Поле 'datetime' должно быть str или datetime, получили {type(dt_raw)}")

    # 2) content может быть строкой или байтами — приводим к bytes
    content_raw = data.get("content")
    if isinstance(content_raw, str):
        content_bytes = content_raw.encode("utf-8")
    elif isinstance(content_raw, (bytes, bytearray)):
        content_bytes = bytes(content_raw)
    else:
        raise ValueError(f"Поле 'content' должно быть str или bytes, получили {type(content_raw)}")

    # 3) остальные поля
    uri = data["uri"]
    source = DataSource(data["source"])
    label = DataLabel(value=data["label"]) if data.get("label") else None
    size = data["content_size_bytes"]

    return DataEntity(
        uri=uri,
        datetime=dt_obj,
        source=source,
        label=label,
        content=content_bytes,
        content_size_bytes=size
    )

def load_entities(path: str = "scraped_data/") -> List[DataEntity]:
    """
    Загружает все .jsonl-файлы из папки и конвертирует в список DataEntity.
    """
    entities = []
    for file_name in os.listdir(path):
        if file_name.endswith(".jsonl"):
            full_path = os.path.join(path, file_name)
            with open(full_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        entity = to_data_entity(data)
                        entities.append(entity)
                    except Exception as e:
                        print(f"Ошибка в {file_name}: {e}")
    return entities


if __name__ == "__main__":
    result = load_entities()
    print(f"Загружено сущностей: {len(result)}")
