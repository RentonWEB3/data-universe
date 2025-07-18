import os
import json
import bittensor
from bittensor import Synapse, Wallet, Axon, Subtensor

INDEX_PATH = "miner_index.json"
CURRENT_INDEX = {}

def load_index():
    global CURRENT_INDEX
    if os.path.exists(INDEX_PATH):
        try:
            with open(INDEX_PATH, "r", encoding="utf-8") as f:
                CURRENT_INDEX = json.load(f)
            print(f"✅ Загружен индекс: {len(CURRENT_INDEX.get('buckets', []))} bucket’ов")
        except Exception as e:
            print("❌ Ошибка чтения miner_index.json:", e)
    else:
        print("⚠️ Внимание! miner_index.json не найден")

def handler(syn: Synapse) -> Synapse:
    load_index()
    hotkey = getattr(syn.dendrite, "hotkey", "unknown")
    ip = getattr(syn.dendrite, "ip", "unknown")
    port = getattr(syn.dendrite, "port", "unknown")
    syn_name = getattr(syn, "name", "unknown")
    print(f"📡 Запрос от {hotkey} ({ip}:{port}), тип запроса: {syn_name}")

    syn.compressed_index_serialized = json.dumps(CURRENT_INDEX)
    print("📤 Отправлен miner_index.json")
    return syn

if __name__ == '__main__':
    wallet = Wallet(name="default", hotkey="default")
    ax = Axon(wallet=wallet, port=8091)
    ax.attach(forward_fn=handler)
    ax.start()

    subt = Subtensor()
    subt.serve_axon(axon=ax, netuid=13)  # ❗ без аргумента wallet

    print("✅ Axon-сервер запущен и слушает на порту 8091")
    while True:
        pass
