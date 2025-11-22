import subprocess
import re
import random
import time

def run_cmd(cmd):
    return subprocess.getoutput(cmd)

def get_all_raft_nodes():
    """Find all containers whose name matches node[0-9]+"""
    output = run_cmd("docker ps --format '{{.Names}}'")
    nodes = [name for name in output.split("\n") if re.fullmatch(r"node[0-9]+", name)]
    return nodes

def choose_survivor(nodes):
    survivor = random.choice(nodes)
    print(f"🟩 Survivor chosen: {survivor}")
    return survivor

def kill_all(nodes, survivor):
    print("\n💥 Killing all nodes except:", survivor)
    for node in nodes:
        if node != survivor:
            print(f"💀 Killing {node} ...")
            run_cmd(f"docker kill {node}")
            time.sleep(1)
    print("\n🟢 Survivor still running:", survivor)

def check_logs():
    print("\n🔍 Checking logs...")
    out = run_cmd("docker compose logs --tail=200 | grep -E 'becomes LEADER|RequestVote'")
    print(out)

if __name__ == "__main__":
    nodes = get_all_raft_nodes()
    print("Detected Raft nodes:", nodes)

    if len(nodes) <= 1:
        print("Not enough Raft nodes running.")
    else:
        survivor = choose_survivor(nodes)
        kill_all(nodes, survivor)
        print("\n⏳ Waiting 3 seconds...")
        time.sleep(3)
        check_logs()
