import os
import subprocess
import re
import time

NODES = ["node1", "node2", "node3", "node4", "node5"]

def run_cmd(cmd):
    """Run shell command and return output."""
    return subprocess.getoutput(cmd)


def find_current_leader():
    """Parse docker logs to find current leader."""
    print("🔍 Searching for current leader...")

    # find most recent "becomes LEADER"
    output = run_cmd("docker compose logs --tail=500")

    # pattern: Node X becomes LEADER
    matches = re.findall(r"Node (\d+) becomes LEADER", output)

    if not matches:
        print("❌ No leader found in logs!")
        return None

    leader_id = matches[-1]  # last one is current
    leader_node = f"node{leader_id}"

    print(f"✅ Current leader detected: {leader_node}")
    return leader_node


def kill_leader(node):
    print(f"\n💥 Stopping leader {node} to trigger election...")
    run_cmd(f"docker stop {node}")

    print("⏳ Waiting 5 seconds so followers start election...")
    time.sleep(5)

    print(f"🔄 Restarting leader {node} ...")
    run_cmd(f"docker start {node}")

    # give it time to rejoin cluster
    time.sleep(2)


def check_new_election():
    """Check logs for new election and new leader."""
    print("\n🔍 Checking for new election...")

    output = run_cmd("docker compose logs --tail=300")

    # Find nodes sending RequestVote
    req_vote = re.findall(r"Node (\d+) sends RPC RequestVote", output)
    leaders = re.findall(r"Node (\d+) becomes LEADER", output)

    print("\n📌 RequestVote activity:")
    for node in set(req_vote):
        print(f"  - Node {node} was requesting votes")

    if leaders:
        new_leader = leaders[-1]
        print(f"\n🎉 New leader elected: Node {new_leader}")
    else:
        print("❌ No new leader detected yet")

    print("\n📌 Printing recent RequestVote and Leader messages:\n")
    print(run_cmd(
        "docker compose logs --tail=200 | grep -E 'RequestVote|becomes LEADER'"
    ))


if __name__ == "__main__":
    leader = find_current_leader()
    if leader:
        kill_leader(leader)
        check_new_election()
    else:
        print("❌ Cannot run test — no leader found.")
