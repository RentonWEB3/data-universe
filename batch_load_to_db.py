#!/usr/bin/env python3
import os
import json
import sqlite3
from common.data import DataEntity, DataSource, DataLabel
from loader import to_data_entity

def main():
    # config.json должен лежать рядом с этим скриптом
    cfg = json.load(open("config.json", "r", encoding="utf-8"))
    db_path = cfg["db_path"]
    files = sorted(f for f in os.listdir("normalized") if f.endswith(".jsonl"))

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    total_inserted = 0

    for file_name in files:
        # определяем источник по имени файла
        source_hint = 1 if file_name.startswith("reddit_") else 2
        print(f"--- Обрабатываем {file_name}, предполагаемый source={source_hint}")
        inserted = 0

        with open(os.path.join("normalized", file_name), "r", encoding="utf-8") as f:
            for line in f:
                raw = json.loads(line)
                try:
                    ent: DataEntity = to_data_entity(raw)

                    # 1) URI – простой str
                    uri = ent.uri

                    # 2) datetime → ISO-строка
                    dt_iso = ent.datetime.isoformat()

                    # 3) content: bytes → BLOB
                    blob = sqlite3.Binary(ent.content)

                    # 4) label может быть DataLabel или str или None
                    if isinstance(ent.label, DataLabel):
                        label_val = ent.label.value
                    else:
                        label_val = ent.label  # None или уже str

                    # 5) размер – int
                    size = ent.content_size_bytes

                    # 6) source может быть DataSource или int
                    if isinstance(ent.source, DataSource):
                        source_val = ent.source.value
                    else:
                        source_val = int(ent.source)

                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO DataEntity
                          (uri, datetime, content, label, content_size_bytes, source)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (uri, dt_iso, blob, label_val, size, source_val)
                    )
                    if cursor.rowcount:
                        inserted += 1

                except Exception as e:
                    # при ошибке выводим URI, чтобы можно было отлаживать
                    print(f"⚠️ Ошибка при вставке {raw.get('uri', '')}: {e}")

        conn.commit()
        print(f"Вставлено из {file_name}: {inserted}")
        total_inserted += inserted

    print(f"\nВсего вставлено: {total_inserted}")

if __name__ == "__main__":
    main()
