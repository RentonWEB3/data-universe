import os

import json

from tqdm import tqdm

from datetime import datetime, timezone



from dynamic_desirability.desirability_retrieval import sync_run_retrieval

from rewards.data import DataDesirabilityLookup

from common.data import DataEntity, DataLabel, DataSource



SRC_DIR = "scraped_data"

DST_DIR = "weighted"

THRESHOLD = 0.1  # порог desirability



def load_entities(src_dir: str) -> list[DataEntity]:

    """Загружает сущности из jsonl-файлов"""

    entities = []

    for filename in os.listdir(src_dir):

        if not filename.endswith(".jsonl"):

            continue

        path = os.path.join(src_dir, filename)

        with open(path, "r", encoding="utf-8") as f:

            for line in f:

                try:

                    data = json.loads(line)

                    entities.append(DataEntity(

                        uri=data["id"],

                        datetime=datetime.strptime(data["timestamp"], "%a %b %d %H:%M:%S %z %Y"),

                        source=DataSource.X if data["source"].lower() == "twitter" else DataSource.REDDIT,

                        label=DataLabel(value=data["author"]) if "author" in data else None,

                        content=data["text"].encode("utf-8"),

                        content_size_bytes=len(data["text"].encode("utf-8"))

                    ))

                except Exception as e:

                    print(f"❌ Ошибка при загрузке {filename}: {e}")

    return entities



def main():

    print("🚀 Загружаем сущности...")

    entities = load_entities(SRC_DIR)

    print(f"📦 Найдено сущностей: {len(entities)}")



    print("📊 Получаем веса desirability...")

    lookup: DataDesirabilityLookup = sync_run_retrieval(config=None)



    print("💾 Сохраняем отфильтрованные сущности по весу...")

    os.makedirs(DST_DIR, exist_ok=True)



    kept = 0

    for entity in tqdm(entities):

        desirability = lookup.get_weight(entity)

        if desirability >= THRESHOLD:

            output = {

                "id": entity.uri,

                "timestamp": entity.datetime.isoformat(),

                "source": DataSource(entity.source).name,

                "label": entity.label.value if entity.label else None,

                "desirability_weight": desirability,

            }

            path = os.path.join(DST_DIR, f"{entity.uri}.json")

            with open(path, "w", encoding="utf-8") as f:

                json.dump(output, f, ensure_ascii=False, indent=2)

            kept += 1



    print(f"\n✅ Завершено. Сохранено {kept} сущностей в {DST_DIR}/")



if __name__ == "__main__":

    main()




