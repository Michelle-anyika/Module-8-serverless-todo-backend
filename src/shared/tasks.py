"""Single-table key layout and helpers shared by every task function.

Table: PK = USER#<sub>, SK = TASK#<taskId>
GSI1:  GSI1PK = USER#<sub>, GSI1SK = STATUS#<status>#<deadline>  (list tasks by status)
"""
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

STATUS_PENDING = "Pending"
STATUS_COMPLETED = "Completed"
STATUS_EXPIRED = "Expired"
STATUSES = (STATUS_PENDING, STATUS_COMPLETED, STATUS_EXPIRED)

ENTITY_TYPE = "Task"

_table = None


def table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


def user_pk(user_id):
    return f"USER#{user_id}"


def task_sk(task_id):
    return f"TASK#{task_id}"


def status_sk(status, deadline):
    return f"STATUS#{status}#{deadline}"


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def default_deadline():
    minutes = int(os.environ.get("DEFAULT_EXPIRY_MINUTES", "5"))
    return now_utc() + timedelta(minutes=minutes)


def to_public(item):
    """Strip storage keys and convert Decimals for the API response."""
    out = {}
    for key, value in item.items():
        if key in ("PK", "SK", "GSI1PK", "GSI1SK", "EntityType"):
            continue
        if isinstance(value, Decimal):
            value = int(value) if value == value.to_integral_value() else float(value)
        out[key] = value
    return out
