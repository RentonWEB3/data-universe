import os
import time
import subprocess
from datetime import datetime

# Вставь свой HF_TOKEN сюда
HF_TOKEN ="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"

def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

while True:
    log("🚀 Запуск пайплайна...")
    try:
        result = subprocess.run(
            ["venv/bin/python", "run_pipeline.py"],
            cwd="/root/data-universe",
            env={**os.environ, "HF_TOKEN": HF_TOKEN},
            capture_output=True,
            text=True
        )
        log("✅ Пайплайн завершён.")
        print(result.stdout)
        if result.stderr:
            print("⚠️ STDERR:", result.stderr)
    except Exception as e:
        log(f"❌ Ошибка при запуске пайплайна: {e}")
    
    log("⏳ Ожидание 20 минут до следующего запуска...")
    time.sleep(5 * 60)
