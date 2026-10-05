import json
import os

os.environ.setdefault("AWS_DEFAULT_REGION", "eu-north-1")
os.environ.setdefault("QUEUE_URL", "https://sqs.example/q.fifo")
os.environ.setdefault("TABLE_NAME", "module-8-tasks")

from events.stream_router import classify  # noqa: E402
from shared.http import HttpError, json_body  # noqa: E402
from shared.tasks import parse_iso, status_sk, to_public  # noqa: E402

PENDING = {"Status": "Pending", "Deadline": "2026-01-01T00:05:00Z"}


def test_insert_pending_schedules():
    assert classify("INSERT", {}, PENDING) == "SCHEDULE"


def test_complete_cancels():
    assert classify("MODIFY", PENDING, {**PENDING, "Status": "Completed"}) == "CANCEL"


def test_delete_pending_cancels():
    assert classify("REMOVE", PENDING, {}) == "CANCEL"


def test_delete_completed_is_noop():
    assert classify("REMOVE", {**PENDING, "Status": "Completed"}, {}) is None


def test_expiry_update_is_noop():
    assert classify("MODIFY", PENDING, {**PENDING, "Status": "Expired"}) is None


def test_deadline_change_reschedules():
    assert classify("MODIFY", PENDING, {**PENDING, "Deadline": "2026-01-01T01:00:00Z"}) == "SCHEDULE"


def test_reopen_reschedules():
    assert classify("MODIFY", {**PENDING, "Status": "Completed"}, PENDING) == "SCHEDULE"


def test_description_edit_is_noop():
    assert classify("MODIFY", PENDING, {**PENDING, "Description": "x"}) is None


def test_keys_hidden_from_api():
    out = to_public({"PK": "USER#1", "SK": "TASK#1", "GSI1PK": "a", "GSI1SK": "b", "EntityType": "Task", "TaskId": "1"})
    assert out == {"TaskId": "1"}


def test_status_sort_key():
    assert status_sk("Pending", "2026-01-01T00:00:00Z") == "STATUS#Pending#2026-01-01T00:00:00Z"


def test_parse_iso_handles_z():
    assert parse_iso("2026-01-01T00:00:00Z").utcoffset().total_seconds() == 0


def test_invalid_json_rejected():
    try:
        json_body({"body": "{bad"})
    except HttpError as err:
        assert err.status == 400
    else:
        raise AssertionError("expected HttpError")


def test_valid_json_body():
    assert json_body({"body": json.dumps({"description": "x"})}) == {"description": "x"}
