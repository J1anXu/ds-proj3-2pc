import requests
import json
import random

# 协调者地址：对应 app-core -> ports: "9002:5000"
COORDINATOR_URL = "http://localhost:9002/start_tx"


def run_2pc_test():
    print("\n🔍 Starting 2PC Test...\n")

    # 生成一个事务 ID
    tx_id = f"tx_{random.randint(1000, 9999)}"
    payload = {"operation": "test_operation"}

    data = {
        "tx_id": tx_id,
        "payload": payload
    }

    print(f"➡️ Sending transaction to coordinator:")
    print(f"   {COORDINATOR_URL}")
    print(f"   tx_id={tx_id}\n")

    try:
        response = requests.post(
            COORDINATOR_URL,
            json=data,
            timeout=5
        )
    except Exception as e:
        print(f"❌ ERROR: Cannot reach coordinator: {e}")
        return

    if response.status_code != 200:
        print(f"❌ Coordinator returned HTTP {response.status_code}")
        print(response.text)
        return

    try:
        result = response.json()
    except Exception as e:
        print("❌ Failed to parse JSON response:", e)
        print("Raw response:", response.text)
        return

    print("📥 Response from coordinator:\n")
    print(json.dumps(result, indent=4, ensure_ascii=False))
    print("\n🟢 Test Completed.\n")


if __name__ == "__main__":
    run_2pc_test()
