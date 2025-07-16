import os
import time

def log(msg: str):
    print(f"\n🕒 {time.strftime('%H:%M:%S')} | {msg}\n")

log("🚀 Старт пайплайна — запуск скрапера Twitter...")
os.system("python twikit_scraper.py")

log("✅ Скрапинг Twitter завершён.")

log("📥 Запуск скрапера Reddit...")
os.system("python reddit_scraper.py")

log("✅ Скрапинг Reddit завершён.")

log("🧹 Удаляем старые parquet-файлы (если остались)...")
os.system("rm -f exports/*.parquet")

log("📦 Запуск экспорта в Hugging Face через export_jsonl_to_hf...")
os.system("python huggingface_utils/export_jsonl_to_hf.py")

log("✅ Pipeline завершён.")
