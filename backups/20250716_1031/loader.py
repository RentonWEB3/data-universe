import os
import json
from typing import List
from datetime import datetime
from dateutil import parser as date_parser

from common.data import DataEntity, DataLabel, DataSource


def to_data_entity(data: dict) -> DataEntity:
    """
    Преобразует словарь json в объект DataEntity.
    """
    if "text" in data and "created_at" in data:
        data = {
            "id": data["id"],
            "timestamp": data["created_at"],
            "content": data["text"],
            "uri": f"https://twitter.com/i/web/status/{data['id']}",
            "label": data.get("keyword", ""),
            "content_size_bytes": len(data["text"].encode("utf-8")),
            "source": 2
        }

    # 2) если это запись из реддита (скрапер отдаёт datetime, content и т.д.)
    elif "datetime" in data:
        # просто переименуем datetime → timestamp
        data["timestamp"] = data.pop("datetime")

    # 3) в остальных случаях оставляем поля как есть,
    #    но DataEntity по-прежнему ждёт timestamp, а не datetime
    if "timestamp" not in data:
        raise ValueError(f"У записи нет поля 'timestamp': {data}")

    return DataEntity(
        uri=data["id"],
        datetime=date_parser.parse(data["timestamp"]),
        source=DataSource.X,  # Twitter / X
        label=DataLabel(value=f"#{data['author']}"),
        content=data["text"].encode("utf-8"),
        content_size_bytes=len(data["text"].encode("utf-8"))
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
