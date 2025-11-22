import subprocess
import time
import re

NODES = ["node1", "node2", "node3", "node4", "node5"]

def get_commit_index(node):
    """读取 node 的日志，解析最后出现的 commit_index"""
    try:
        logs = subprocess.check_output(
            ["docker", "compose", "logs", node, "--tail", "200"],
            stderr=subprocess.STDOUT
        ).decode()

        matches = re.findall(r"commit_index\s*=\s*(\d+)", logs)
        if matches:
            return int(matches[-1])
        return None
    except:
        return None

def wait_for_leader():
    """等待 leader 输出 commit_index"""
    print("⏳ Waiting for leader to generate commit_index...")
    for _ in range(20):
        for node in NODES:
            idx = get_commit_index(node)
            if idx is not None and idx >= 0:
                print(f"🔥 Leader found: {node} commit_index={idx}")
                return
        time.sleep(1)
    print("❌ No leader found.")
    exit(1)

def test_commit_sync():
    print("🟢 Running Test Case 5: commit_index synchronization test\n")

    wait_for_leader()

    time.sleep(5)

    print("\n📌 Checking commit_index of all nodes:\n")

    results = {}
    for node in NODES:
        idx = get_commit_index(node)
        results[node] = idx
        print(f"   {node}: commit_index = {idx}")

    values = list(results.values())
    if None in values:
        print("\n❌ Some nodes have no commit_index output!")
        return
    
    if len(set(values)) == 1:
        print("\n🎉 PASS: All nodes synchronized commit_index =", values[0])
    else:
        print("\n❌ FAIL: Nodes have inconsistent commit_index!")
        print("Values:", results)


if __name__ == "__main__":
    test_commit_sync()
