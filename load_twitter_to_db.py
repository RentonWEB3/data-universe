#!/usr/bin/env python3
import sqlite3
import json
import sys
from datetime import datetime

# Параметры
DB_PATH = 'data_universe.db'
SOURCE = 2  # для X/Twitter
JSONL_FILE = 'normalized/twitter_20250714_1317.jsonl'  # подставьте ваш путь

def ensure_table(conn):
    conn.execute('''
    CREATE TABLE IF NOT EXISTS DataEntity (
        id TEXT PRIMARY KEY,
        datetime TEXT,
        label TEXT,
        content TEXT,
        source INTEGER
    );
    ''')
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    cur = conn.cursor()

    added = 0
    with open(JSONL_FILE, encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line)
            # Предполагаем, что поле created_at хранит ISO-строку
            dt = obj.get('created_at')
            # Если нужно, можно парсить:
            # dt = datetime.fromisoformat(obj['created_at']).strftime('%Y-%m-%d %H:%M:%S')
            content = obj.get('text') or ''
            tweet_id = str(obj.get('id'))
            try:
                cur.execute('''
                    INSERT OR IGNORE INTO DataEntity (id, datetime, label, content, source)
                    VALUES (?, ?, ?, ?, ?)
                ''', (tweet_id, dt, None, content, SOURCE))
                if cur.rowcount:
                    added += 1
            except Exception as e:
                print(f"ERROR inserting {tweet_id}: {e}", file=sys.stderr)

    conn.commit()
    conn.close()
    print(f"Added to DB: {added} rows.")

if __name__ == '__main__':
    main()
