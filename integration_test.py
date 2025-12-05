import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import requests

LEADER_URL = "http://localhost:8000"
FOLLOWER_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
    "http://localhost:8005",
]


def set_write_quorum(q: int) -> None:
    resp = requests.post(
        f"{LEADER_URL}/config/write_quorum",
        json={"write_quorum": q},
        timeout=5.0,
    )
    resp.raise_for_status()


def get_record(base_url: str, key: str) -> Dict:
    resp = requests.get(f"{base_url}/get/{key}", timeout=5.0)
    if resp.status_code != 200:
        raise KeyError(f"{base_url} missing key {key}")
    data = resp.json()
    return {"value": data["value"], "version": int(data["version"])}


def check_key_consistency(key: str) -> None:
    leader_rec = get_record(LEADER_URL, key)
    for url in FOLLOWER_URLS:
        follower_rec = get_record(url, key)
        assert follower_rec == leader_rec, (
            f"[{key}] follower {url} out of sync: {follower_rec} vs {leader_rec}"
        )


def test_1_basic_write():
    print("\n=== Test 1: Basic Write ===")
    set_write_quorum(3)
    
    resp = requests.post(
        f"{LEADER_URL}/set",
        json={"key": "key1", "value": "value1"},
        timeout=10.0,
    )
    data = resp.json()
    print(f"Response: {data}")
    assert data["status"] == "committed", f"Expected 'committed', got '{data['status']}'"
    assert data["acks"] >= 3, f"Expected at least 3 acks, got {data['acks']}"
    print(f"Write succeeded with {data['acks']} acknowledgments")


def test_2_quorum_satisfaction():
    print("\n=== Test 2: Quorum Satisfaction ===")
    set_write_quorum(2)
    
    resp = requests.post(
        f"{LEADER_URL}/set",
        json={"key": "key2", "value": "value2"},
        timeout=10.0,
    )
    data = resp.json()
    assert data["status"] == "committed"
    assert data["acks"] >= 2
    print(f"Quorum of 2 satisfied with {data['acks']} acknowledgments")


def test_3_eventual_consistency():
    print("\n=== Test 3: Eventual Consistency ===")
    set_write_quorum(1)
    
    resp = requests.post(
        f"{LEADER_URL}/set",
        json={"key": "key3", "value": "value3"},
        timeout=10.0,
    )
    data = resp.json()
    version = data["version"]
    
    time.sleep(2)
    
    # Check all followers have the data
    consistent = True
    for url in FOLLOWER_URLS:
        try:
            rec = get_record(url, "key3")
            if rec["version"] != version or rec["value"] != "value3":
                consistent = False
                break
        except:
            consistent = False
            break
    
    assert consistent, "Not all followers eventually consistent"
    print("All followers eventually consistent")


def test_4_multiple_writes():
    print("\n=== Test 4: Multiple Writes ===")
    set_write_quorum(3)
    
    keys = ["multi_1", "multi_2", "multi_3"]
    for i, key in enumerate(keys):
        resp = requests.post(
            f"{LEADER_URL}/set",
            json={"key": key, "value": f"value_{i}"},
            timeout=10.0,
        )
        assert resp.json()["status"] == "committed"
    
    print("3 sequential writes succeeded")


def test_5_read_after_write():
    print("\n=== Test 5: Read After Write ===")
    set_write_quorum(2)
    
    resp = requests.post(
        f"{LEADER_URL}/set",
        json={"key": "read_test", "value": "read_value"},
        timeout=10.0,
    )
    version = resp.json()["version"]
    
    time.sleep(1)
    
    # Read from leader
    rec = get_record(LEADER_URL, "read_test")
    assert rec["value"] == "read_value"
    assert rec["version"] == version
    print(f"Read successful: value=read_value, version={version}")


def test_6_version_consistency():
    print("\n=== Test 6: Version Consistency ===")
    set_write_quorum(3)
    
    versions = []
    for i in range(17, 22):
        resp = requests.post(
            f"{LEADER_URL}/set",
            json={"key": "version_test", "value": f"value_{i}"},
            timeout=10.0,
        )
        versions.append(resp.json()["version"])
    
    # Verify versions are monotonically increasing
    is_monotonic = all(versions[i] < versions[i+1] for i in range(len(versions)-1))
    assert is_monotonic, "Versions are not monotonically increasing"
    print(f"Versions are monotonically increasing: {versions}")


def test_7_concurrent_writes():
    print("\n=== Test 7: Concurrent Writes ===")
    set_write_quorum(2)
    
    def write_concurrent(i: int):
        return requests.post(
            f"{LEADER_URL}/set",
            json={"key": f"concurrent_{i}", "value": f"value_{i}"},
            timeout=10.0,
        ).json()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(write_concurrent, i) for i in range(5)]
        results = [f.result() for f in as_completed(futures)]
    
    success_count = sum(1 for r in results if r["status"] == "committed")
    assert success_count == 5, f"Expected 5 successes, got {success_count}"
    
    # Get final value and version
    time.sleep(1)
    final_rec = get_record(LEADER_URL, "concurrent_1")
    print(f"{success_count}/5 concurrent writes succeeded")
    print(f"Final value: {final_rec['value']}, version: {final_rec['version']}")


def main() -> None:
    print("Checking cluster health...")
    lh = requests.get(f"{LEADER_URL}/health", timeout=5.0).json()
    print("Leader health check passed\n")
    print("="*60)
    
    test_1_basic_write()
    test_2_quorum_satisfaction()
    test_3_eventual_consistency()
    test_4_multiple_writes()
    test_5_read_after_write()
    test_6_version_consistency()
    test_7_concurrent_writes()
    
    print("\n" + "="*60)
    print("!! ALL TESTS PASSED !!")
    print("="*60)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"\n Integration test FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        sys.exit(1)
