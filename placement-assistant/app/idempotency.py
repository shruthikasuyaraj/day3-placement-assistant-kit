"""Stable fingerprints for side effects. Hashing, used for exactly-once."""

import hashlib
import json
import re
from datetime import date


def _normalize(value):
    if isinstance(value,dict):
        return {k:_normalize(v) for k,v in value.items()}
    if isinstance(value,list):
        return [_normalize(v) for v in value]
    if isinstance(value,float) and value.is_integer():
        return int(value)
    return value


def canonical_json(value) -> str:
    value = _normalize(value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",",":"),
        ensure_ascii=False
    )


def idempotency_key(run_id: str, step_seq: int, tool_name: str, args: dict) -> str:
    value = [run_id,step_seq,tool_name,args]
    return hashlib.sha256(
        canonical_json(value).encode("utf-8")
    ).hexdigest()


def notification_dedupe_key(roll_no: str, message: str, day: date) -> str:
    message = re.sub(r"\s+"," ",message).strip()
    value = [roll_no,message,day.isoformat()]
    return hashlib.sha256(
        canonical_json(value).encode("utf-8")
    ).hexdigest()