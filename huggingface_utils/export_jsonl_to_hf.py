import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from datasets import Dataset, DatasetDict
from huggingface_hub import HfApi

HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    raise RuntimeError("❌ Переменная среды HF_TOKEN не установлена")

DATASET_NAME = "RentonWEB3/crypto-tweets"
NORMALIZED_DIR = "normalized"
EXPORTS_DIR = "exports"
README_PATH = "README.md"

SOURCE_MAPPING = {
    2: "X",
    1: "REDDIT"
}

def load_jsonl(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def main():
    files = sorted(Path(NORMALIZED_DIR).glob("*.jsonl"))
    all_data = []

    for file in files:
        print(f"📄 Обрабатывается файл: {file.name}")
        data = list(load_jsonl(file))
        for item in data:
            source_value = item.get("source")
            item["source"] = SOURCE_MAPPING.get(source_value, str(source_value))
        print(f"✅ Загружено записей: {len(data)}")
        all_data.extend(data)

    if not all_data:
        print("⚠️ Нет данных для экспорта")
        return

    df = pd.DataFrame(all_data)
    df["label"] = df["label"].apply(lambda x: x["name"] if isinstance(x, dict) and "name" in x else str(x))
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    parquet_path = f"{EXPORTS_DIR}/data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(parquet_path)
    print(f"📦 Паркет сохранён: {parquet_path}")

    # 🪣 Генерация MinerIndex
    try:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime", "label", "source"])
        df["date"] = df["datetime"].dt.date

        grouped = df.groupby(["source", "label", "date"])

        buckets = []
        for (source, label, date), group in grouped:
            start = datetime.combine(date, datetime.min.time())
            end = start + pd.Timedelta(days=1)
            bucket = {
                "source": str(source).lower(),
                "label": str(label),
                "start_time": start.isoformat() + "Z",
                "end_time": end.isoformat() + "Z"
            }
            buckets.append(bucket)

        index = {"buckets": buckets}
        with open("miner_index.json", "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
        print(f"🧠 MinerIndex сохранён → miner_index.json")
    except Exception as e:
        print(f"❌ Ошибка генерации MinerIndex: {e}")

    ds = Dataset.from_pandas(df)
    dsdict = DatasetDict({"train": ds})

    print(f"⬆️ Загружаем датасет на Hugging Face: {DATASET_NAME}")
    dsdict.push_to_hub(DATASET_NAME)
    print("✅ Успешно загружено на Hugging Face!")

if __name__ == "__main__":
    main()
