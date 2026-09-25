"""
Vault Object Storage - Python Client SDK
High-level client library for storing, replicating, retrieving, and inspecting objects in Vault.
Zero external dependencies (uses standard library urllib).
"""

import json
import urllib.request
import urllib.error
from typing import Optional, Dict, Any, List, Union


class VaultClient:
    def __init__(self, endpoint: str = "http://127.0.0.1:8080"):
        self.endpoint = endpoint.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[bytes] = None,
    ) -> urllib.request.urlopen:
        url = f"{self.endpoint}/{path.lstrip('/')}"
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("User-Agent", "Vault-Python-SDK/1.0")

        if headers:
            for k, v in headers.items():
                req.add_header(k, str(v))

        try:
            return urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(err_body)
                msg = parsed.get("error", err_body)
            except Exception:
                msg = err_body
            raise RuntimeError(f"Vault API error [HTTP {e.code}]: {msg}") from e

    # --- Bucket Operations ---

    def create_bucket(self, bucket: str, policy: str = "REPLICATION") -> Dict[str, Any]:
        """Creates a bucket with default storage policy (REPLICATION or ERASURE)."""
        res = self._request("PUT", f"/{bucket}", headers={"X-Vault-Storage-Policy": policy})
        return json.loads(res.read().decode("utf-8"))

    def list_buckets(self) -> List[Dict[str, Any]]:
        """Lists all existing buckets."""
        status = self.get_cluster_status()
        return status.get("buckets", [])

    # --- Object Operations ---

    def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[str, bytes],
        policy: Optional[str] = None,
        replicas: Optional[int] = None,
        ec_k: Optional[int] = None,
        ec_m: Optional[int] = None,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        """Stores an object into Vault."""
        payload = data.encode("utf-8") if isinstance(data, str) else data
        headers = {"Content-Type": content_type}

        if policy:
            headers["X-Vault-Storage-Policy"] = policy
        if replicas is not None:
            headers["X-Vault-Replicas"] = str(replicas)
        if ec_k is not None:
            headers["X-Vault-EC-K"] = str(ec_k)
        if ec_m is not None:
            headers["X-Vault-EC-M"] = str(ec_m)

        res = self._request("PUT", f"/{bucket}/{key}", headers=headers, data=payload)
        return json.loads(res.read().decode("utf-8"))

    def get_object(
        self,
        bucket: str,
        key: str,
        byte_range: Optional[str] = None,
    ) -> bytes:
        """Retrieves and verifies an object from Vault with read quorum."""
        headers = {}
        if byte_range:
            headers["Range"] = f"bytes={byte_range}"

        res = self._request("GET", f"/{bucket}/{key}", headers=headers)
        return res.read()

    def head_object(self, bucket: str, key: str) -> Dict[str, str]:
        """Retrieves object metadata headers without transferring payload."""
        res = self._request("HEAD", f"/{bucket}/{key}")
        headers = dict(res.headers)
        return {
            "content_length": headers.get("content-length"),
            "content_type": headers.get("content-type"),
            "etag": headers.get("etag"),
            "version_id": headers.get("x-vault-version-id"),
            "policy": headers.get("x-vault-storage-policy"),
            "last_modified": headers.get("last-modified"),
        }

    def delete_object(self, bucket: str, key: str) -> Dict[str, Any]:
        """Deletes an object (writes durable tombstone)."""
        res = self._request("DELETE", f"/{bucket}/{key}")
        return json.loads(res.read().decode("utf-8"))

    def list_objects(self, bucket: str, prefix: str = "", limit: int = 1000) -> List[Dict[str, Any]]:
        """Lists objects within a bucket."""
        path = f"/{bucket}?prefix={urllib.request.quote(prefix)}&limit={limit}"
        res = self._request("GET", path)
        return json.loads(res.read().decode("utf-8")).get("objects", [])

    # --- Multipart Operations ---

    def init_multipart(self, bucket: str, key: str, policy: Optional[str] = None) -> str:
        """Initiates an S3-compatible multi-part upload session."""
        headers = {"X-Vault-Storage-Policy": policy} if policy else {}
        res = self._request("POST", f"/{bucket}/{key}?uploads", headers=headers)
        data = json.loads(res.read().decode("utf-8"))
        return data["uploadId"]

    def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        data: Union[str, bytes],
    ) -> Dict[str, Any]:
        """Uploads a single part of a multi-part upload."""
        payload = data.encode("utf-8") if isinstance(data, str) else data
        path = f"/{bucket}/{key}?uploadId={upload_id}&partNumber={part_number}"
        res = self._request("PUT", path, data=payload)
        return json.loads(res.read().decode("utf-8"))

    def complete_multipart(self, bucket: str, key: str, upload_id: str) -> Dict[str, Any]:
        """Completes multi-part upload and assembles chunks into atomic object manifest."""
        path = f"/{bucket}/{key}?uploadId={upload_id}"
        res = self._request("POST", path)
        return json.loads(res.read().decode("utf-8"))

    def abort_multipart(self, bucket: str, key: str, upload_id: str) -> Dict[str, Any]:
        """Aborts a multi-part upload session and cleans up staged parts."""
        path = f"/{bucket}/{key}?uploadId={upload_id}"
        res = self._request("DELETE", path)
        return json.loads(res.read().decode("utf-8"))

    # --- Cluster & Chaos Admin Operations ---

    def get_cluster_status(self) -> Dict[str, Any]:
        """Retrieves comprehensive cluster health, node topology, and metrics."""
        res = self._request("GET", "/api/cluster/status")
        return json.loads(res.read().decode("utf-8"))

    def chaos_kill_node(self, node_id: str) -> Dict[str, Any]:
        """Simulates sudden hardware or network crash of a node."""
        res = self._request(
            "POST",
            "/api/chaos/kill-node",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"nodeId": node_id}).encode("utf-8"),
        )
        return json.loads(res.read().decode("utf-8"))

    def chaos_revive_node(self, node_id: str) -> Dict[str, Any]:
        """Revives an offline node and replays pending hinted handoffs."""
        res = self._request(
            "POST",
            "/api/chaos/revive-node",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"nodeId": node_id}).encode("utf-8"),
        )
        return json.loads(res.read().decode("utf-8"))

    def chaos_corrupt_chunk(self, node_id: str, chunk_id: str) -> Dict[str, Any]:
        """Inverts bits in chunk file on disk to simulate bitrot."""
        res = self._request(
            "POST",
            "/api/chaos/corrupt-chunk",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"nodeId": node_id, "chunkId": chunk_id}).encode("utf-8"),
        )
        return json.loads(res.read().decode("utf-8"))

    def chaos_heal(self) -> Dict[str, Any]:
        """Heals all network partitions and revives all nodes."""
        res = self._request("POST", "/api/chaos/heal")
        return json.loads(res.read().decode("utf-8"))

    def trigger_scrub(self) -> Dict[str, Any]:
        """Triggers a cluster-wide cryptographic disk scrub for silent bitrot."""
        res = self._request("POST", "/api/actions/scrub")
        return json.loads(res.read().decode("utf-8"))

    def trigger_heal(self) -> Dict[str, Any]:
        """Triggers an immediate autonomous healing and reconstruction cycle."""
        res = self._request("POST", "/api/actions/heal")
        return json.loads(res.read().decode("utf-8"))
