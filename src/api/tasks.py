"""CRUD handlers for /tasks. Each handler is deployed as its own Lambda so it
gets a least-privilege IAM role (Query / GetItem / PutItem / UpdateItem / DeleteItem)."""
import re
import uuid

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from shared.http import HttpError, api_handler, current_user, json_body, path_param, response
from shared.tasks import (ENTITY_TYPE, STATUS_COMPLETED, STATUS_EXPIRED, STATUS_PENDING, STATUSES,
                          default_deadline, iso, now_utc, parse_iso, status_sk, table, task_sk,
                          to_public, user_pk)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
UUID_RE = re.compile(r"^[0-9a-f-]{36}$")
MAX_DESCRIPTION = 500


def _description(value):
    if not isinstance(value, str) or not value.strip():
        raise HttpError(400, "description is required")
    value = value.strip()
    if len(value) > MAX_DESCRIPTION:
        raise HttpError(400, f"description must be at most {MAX_DESCRIPTION} characters")
    return value


def _date(value):
    if not isinstance(value, str) or not DATE_RE.match(value):
        raise HttpError(400, "date must be in YYYY-MM-DD format")
    return value


def _future_deadline(value):
    try:
        deadline = parse_iso(value)
    except (TypeError, ValueError, AttributeError):
        raise HttpError(400, "deadline must be an ISO-8601 timestamp")
    if deadline <= now_utc():
        raise HttpError(400, "deadline must be in the future")
    return deadline


def _task_id(event):
    task_id = path_param(event, "taskId")
    if not UUID_RE.match(task_id):
        raise HttpError(404, "Task not found")
    return task_id


@api_handler
def list_tasks(event, _context):
    user_id, _ = current_user(event)
    status = (event.get("queryStringParameters") or {}).get("status")
    if status:
        if status not in STATUSES:
            raise HttpError(400, f"status must be one of {', '.join(STATUSES)}")
        kwargs = {"IndexName": "GSI1",
                  "KeyConditionExpression": Key("GSI1PK").eq(user_pk(user_id))
                  & Key("GSI1SK").begins_with(f"STATUS#{status}#")}
    else:
        kwargs = {"KeyConditionExpression": Key("PK").eq(user_pk(user_id)) & Key("SK").begins_with("TASK#")}

    items = []
    while True:
        page = table().query(**kwargs)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    tasks = sorted((to_public(i) for i in items), key=lambda t: t.get("CreatedAt", ""), reverse=True)
    return response(200, {"tasks": tasks, "count": len(tasks)})


@api_handler
def get_task(event, _context):
    user_id, _ = current_user(event)
    item = table().get_item(Key={"PK": user_pk(user_id), "SK": task_sk(_task_id(event))}).get("Item")
    if not item:
        raise HttpError(404, "Task not found")
    return response(200, to_public(item))


@api_handler
def create_task(event, _context):
    user_id, email = current_user(event)
    body = json_body(event)
    now = now_utc()
    deadline = _future_deadline(body["deadline"]) if body.get("deadline") else default_deadline()
    task_id = str(uuid.uuid4())
    item = {
        "PK": user_pk(user_id),
        "SK": task_sk(task_id),
        "EntityType": ENTITY_TYPE,
        "TaskId": task_id,
        "UserId": user_id,
        "UserEmail": email,
        "Description": _description(body.get("description")),
        "Date": _date(body["date"]) if body.get("date") else now.strftime("%Y-%m-%d"),
        "Status": STATUS_PENDING,
        "Deadline": iso(deadline),
        "DeadlineEpoch": int(deadline.timestamp()),
        "CreatedAt": iso(now),
        "UpdatedAt": iso(now),
        "Version": 1,
        "GSI1PK": user_pk(user_id),
        "GSI1SK": status_sk(STATUS_PENDING, iso(deadline)),
    }
    table().put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    return response(201, to_public(item))


@api_handler
def update_task(event, _context):
    user_id, _ = current_user(event)
    task_id = _task_id(event)
    body = json_body(event)
    key = {"PK": user_pk(user_id), "SK": task_sk(task_id)}
    current = table().get_item(Key=key, ConsistentRead=True).get("Item")
    if not current:
        raise HttpError(404, "Task not found")

    changes = {}
    if "description" in body:
        changes["Description"] = _description(body["description"])
    if "date" in body:
        changes["Date"] = _date(body["date"])

    status = current["Status"]
    deadline = parse_iso(current["Deadline"])
    if "status" in body and body["status"] != status:
        new_status = body["status"]
        if new_status not in (STATUS_PENDING, STATUS_COMPLETED):
            raise HttpError(400, "status can only be set to Pending or Completed")
        if status == STATUS_EXPIRED:
            raise HttpError(409, "Expired tasks cannot change status")
        if new_status == STATUS_PENDING and deadline <= now_utc() and not body.get("deadline"):
            deadline = default_deadline()  # a re-opened task gets a fresh deadline
            changes["Deadline"] = iso(deadline)
        status = new_status
        changes["Status"] = status
    if body.get("deadline"):
        if current["Status"] == STATUS_EXPIRED:
            raise HttpError(409, "Expired tasks cannot change deadline")
        deadline = _future_deadline(body["deadline"])
        changes["Deadline"] = iso(deadline)

    if not changes:
        raise HttpError(400, "Nothing to update: provide description, date, status or deadline")

    changes["DeadlineEpoch"] = int(deadline.timestamp())
    changes["GSI1SK"] = status_sk(status, iso(deadline))
    changes["UpdatedAt"] = iso(now_utc())
    changes["Version"] = int(current.get("Version", 1)) + 1

    names = {f"#f{i}": k for i, k in enumerate(changes)}
    values = {f":v{i}": v for i, v in enumerate(changes.values())}
    values[":expected"] = current.get("Version", 1)
    values[":status"] = current["Status"]
    try:
        updated = table().update_item(
            Key=key,
            UpdateExpression="SET " + ", ".join(f"#f{i} = :v{i}" for i in range(len(changes))),
            # Optimistic locking: fails if the expiry processor (or another client) changed the task meanwhile.
            ConditionExpression="attribute_exists(PK) AND Version = :expected AND #st = :status",
            ExpressionAttributeNames={**names, "#st": "Status"},
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise HttpError(409, "Task was modified concurrently (it may have just expired); reload and retry")
        raise
    return response(200, to_public(updated))


@api_handler
def delete_task(event, _context):
    user_id, _ = current_user(event)
    try:
        table().delete_item(Key={"PK": user_pk(user_id), "SK": task_sk(_task_id(event))},
                            ConditionExpression="attribute_exists(PK)")
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise HttpError(404, "Task not found")
        raise
    return response(204)
