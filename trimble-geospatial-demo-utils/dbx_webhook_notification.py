import hashlib
import hmac
import json
from typing import Iterable, Optional, Tuple

import requests


def try_find_long(root: dict, keys: Iterable[str]) -> Optional[int]:
    for key in keys:
        value = root.get(key)
        if value is not None:
            try:
                return int(value)
            except (ValueError, TypeError):
                pass
    return None


def try_find_string(root: dict, keys: Iterable[str]) -> Optional[str]:
    for key in keys:
        value = root.get(key)
        if value is not None:
            return str(value)
    return None


def _normalize_status(status: Optional[str]) -> Optional[str]:
    if status is None:
        return None

    normalized = str(status).strip().upper()

    # Match the Azure Function's DetermineSuccess() heuristic
    if "SUCCESS" in normalized or normalized in {"SUCCEEDED", "OK", "COMPLETED"}:
        return "SUCCESS"
    if "FAIL" in normalized or "ERROR" in normalized or "CANCEL" in normalized:
        return "FAILED"

    return status


def _build_signature(body_bytes: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
    return f"sha256={digest}"  # server accepts with/without prefix


def send_notification(
    root_json_str: str,
    notification_url: str,
    *,
    webhook_secret: str,
    timeout_s: int = 10,
) -> Tuple[bool, Optional[int], str]:
    """Send a signed webhook notification to the Azure Function.

    Returns: (ok, status_code, response_text)
    """

    try:
        root = json.loads(root_json_str)
    except json.JSONDecodeError as e:
        msg = f"Invalid JSON input: {e}"
        print(msg)
        return False, None, msg

    # Customize the names we look for (top-level)
    run_id_names = ["runId", "RunId", "run_id"]
    job_id_names = ["jobId", "JobId", "job_id"]
    status_names = ["status", "Status", "state", "result_state", "resultState"]
    error_names = ["error", "Error", "errorMessage", "error_message", "message"]

    run_id = try_find_long(root, run_id_names)
    job_id = try_find_long(root, job_id_names)
    status = _normalize_status(try_find_string(root, status_names))
    error = try_find_string(root, error_names)

    # Keep payload compatible with the Function's TryFind* logic
    payload = {
        "runId": run_id,
        "jobId": job_id,
        "status": status,
        "error": error,
        # Extra aliases for maximum compatibility
        "run_id": run_id,
        "job_id": job_id,
        "result_state": status,
        "error_message": error,
    }

    # Serialize ourselves so signature matches the exact bytes we send.
    body_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    body_bytes = body_str.encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "X-Dbx-Signature": _build_signature(body_bytes, webhook_secret),
    }

    print(f"Sending notification to {notification_url} with payload: {payload}")

    try:
        response = requests.post(notification_url, data=body_bytes, headers=headers, timeout=timeout_s)
        ok = 200 <= response.status_code < 300
        if ok:
            print(f"Notification sent successfully, response status: {response.status_code}")
        else:
            print(f"Notification failed, response status: {response.status_code} body: {response.text}")
        return ok, response.status_code, response.text
    except requests.RequestException as e:
        msg = f"Failed to send notification: {e}"
        print(msg)
        return False, None, msg
