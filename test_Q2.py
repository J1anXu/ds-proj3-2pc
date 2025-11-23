import requests
import json
import time
import subprocess

COORDINATOR_URL = "http://localhost:9002/start_tx"

print("\n🔍 Starting 2PC Decision Phase Test...\n")

tx_id = "tx_test_decision"

print(f"➡️ Sending transaction to coordinator:\n    {COORDINATOR_URL}")
print(f"   tx_id={tx_id}\n")

try:
    resp = requests.post(
        COORDINATOR_URL,
        json={"tx_id": tx_id, "payload": {"operation": "op_test"}},
        timeout=3
    )

    if resp.status_code != 200:
        print(f"❌ Coordinator returned HTTP {resp.status_code}")
        print(resp.text)
        exit(1)

    data = resp.json()
    print("📥 Response from coordinator:\n")
    print(json.dumps(data, indent=4))

except Exception as e:
    print(f"❌ ERROR contacting coordinator: {e}")
    exit(1)

# Wait for gRPC commit/abort messages to be delivered
print("\n⏳ Waiting 1 second for Decision Phase RPCs to propagate...\n")
time.sleep(1)

print("📌 Fetching relevant logs (Decision Phase RPCs)...\n")

cmd = [
    "docker", "compose", "logs", "--tail=200"
]

try:
    logs = subprocess.check_output(cmd).decode("utf-8")
except Exception as e:
    print(f"❌ Error reading logs: {e}")
    exit(1)

# Only show Decision Phase logs
filtered = []

KEYWORDS = [
    "Phase Decision",
    "GlobalCommit",
    "GlobalAbort",
    "locally COMMIT",
    "locally ABORT"
]

for line in logs.splitlines():
    if any(key in line for key in KEYWORDS):
        filtered.append(line)

if not filtered:
    print("❌ No Decision Phase logs detected!")
else:
    print("\n".join(filtered))

print("\n🟢 Decision Phase Test Completed.\n")
