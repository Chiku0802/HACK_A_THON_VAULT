"""
Test: Python SDK integration with Vault cluster
"""

import sys
import os
import subprocess
import time
import urllib.request
import shutil

# Ensure utf-8 stdout on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Add root directory to python path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
from vault_client import VaultClient


def main():
    print("--- Testing Vault Python Client SDK ---")

    test_data_dir = os.path.join(ROOT_DIR, "data-test-py")
    if os.path.exists(test_data_dir):
        shutil.rmtree(test_data_dir, ignore_errors=True)

    runner_script = os.path.join(ROOT_DIR, "tests", "run-test-cluster.js")
    cmd = ["node", runner_script, "9880", test_data_dir]

    proc = subprocess.Popen(cmd)

    # Wait for gateway to be ready
    gateway_url = "http://127.0.0.1:9880"
    ready = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(f"{gateway_url}/", timeout=1):
                ready = True
                break
        except Exception:
            time.sleep(0.25)

    if not ready:
        proc.kill()
        raise RuntimeError("Cluster failed to start")

    client = VaultClient(gateway_url)

    try:
        # 1. Cluster Status
        status = client.get_cluster_status()
        print(f"[OK] Connected to cluster: {len(status['nodes'])} nodes online")

        # 2. Create Bucket
        b_res = client.create_bucket("py-bucket", policy="REPLICATION")
        print(f"[OK] Created bucket: {b_res['name']}")

        # 3. Put & Get Object
        data = "Testing Vault storage directly via Python Client SDK!"
        put_res = client.put_object("py-bucket", "notes/hello.txt", data)
        print(f"[OK] Put object 'hello.txt' (ETag: {put_res['etag']})")

        get_data = client.get_object("py-bucket", "notes/hello.txt").decode("utf-8")
        assert get_data == data, "Retrieved data matches written data"
        print("[OK] Get object validated with 100% integrity")

        # 4. Multipart upload in Python
        upload_id = client.init_multipart("py-bucket", "large/payload.dat")
        client.upload_part("py-bucket", "large/payload.dat", upload_id, 1, b"Chunk 1 ")
        client.upload_part("py-bucket", "large/payload.dat", upload_id, 2, b"Chunk 2 ")
        client.upload_part("py-bucket", "large/payload.dat", upload_id, 3, b"Chunk 3")
        comp = client.complete_multipart("py-bucket", "large/payload.dat", upload_id)
        assert comp["size"] == 23
        print(f"[OK] Multipart upload completed (size: {comp['size']} bytes)")

        # 5. List objects
        objs = client.list_objects("py-bucket")
        assert len(objs) == 2
        print(f"[OK] List objects returned {len(objs)} objects")

        # 6. Delete object
        client.delete_object("py-bucket", "notes/hello.txt")
        objs_after = client.list_objects("py-bucket")
        assert len(objs_after) == 1
        print("[OK] Object deletion verified (tombstoned)")

        print("All Python Client SDK tests passed successfully!\n")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        if os.path.exists(test_data_dir):
            shutil.rmtree(test_data_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
