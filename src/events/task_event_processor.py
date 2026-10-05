"""SQS FIFO consumer for task lifecycle events. Every action is idempotent:

  SCHEDULE - upsert a one-time EventBridge Scheduler schedule `at(deadline)`
             whose target is this same FIFO queue with an EXPIRE payload.
  CANCEL   - delete the schedule (already gone = success).
  EXPIRE   - conditional update Pending -> Expired, then one SNS email to the
             owner (NotifiedAt guards against duplicate emails on retry).
"""
import json
import logging
import os
from datetime import timedelta

import boto3
from botocore.exceptions import ClientError

from shared.tasks import (STATUS_EXPIRED, STATUS_PENDING, iso, now_utc, parse_iso, status_sk, table,
                          task_sk, user_pk)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

scheduler = boto3.client("scheduler")
sns = boto3.client("sns")

TOPIC_ARN = os.environ["TOPIC_ARN"]
QUEUE_ARN = os.environ["QUEUE_ARN"]
SCHEDULE_GROUP = os.environ["SCHEDULE_GROUP"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
SCHEDULER_DLQ_ARN = os.environ["SCHEDULER_DLQ_ARN"]
SCHEDULE_PREFIX = os.environ.get("SCHEDULE_PREFIX", "module-8-task-")

# Scheduler fires at minute granularity; accept EXPIRE events slightly early.
EXPIRY_TOLERANCE_SECONDS = 60


def schedule_name(task_id):
    return f"{SCHEDULE_PREFIX}{task_id}"


def _get_task(msg):
    return table().get_item(Key={"PK": user_pk(msg["userId"]), "SK": task_sk(msg["taskId"])},
                            ConsistentRead=True).get("Item")


def handle_schedule(msg):
    task = _get_task(msg)
    if not task or task["Status"] != STATUS_PENDING:
        logger.info("schedule_skipped_stale", extra={"taskId": msg["taskId"], "type": "SCHEDULE"})
        return
    deadline = parse_iso(task["Deadline"])
    if deadline <= now_utc() + timedelta(seconds=EXPIRY_TOLERANCE_SECONDS):
        handle_expire(msg)  # deadline already (nearly) reached - expire directly
        return

    params = {
        "Name": schedule_name(msg["taskId"]),
        "GroupName": SCHEDULE_GROUP,
        "ScheduleExpression": f"at({deadline.strftime('%Y-%m-%dT%H:%M:%S')})",
        "ScheduleExpressionTimezone": "UTC",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "ActionAfterCompletion": "DELETE",
        "Description": f"Expire task {msg['taskId']}",
        "Target": {
            "Arn": QUEUE_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"type": "EXPIRE", "taskId": msg["taskId"], "userId": msg["userId"],
                                 "deadline": task["Deadline"], "source": "eventbridge-scheduler"}),
            "SqsParameters": {"MessageGroupId": msg["taskId"]},
            "RetryPolicy": {"MaximumRetryAttempts": 10, "MaximumEventAgeInSeconds": 3600},
            "DeadLetterConfig": {"Arn": SCHEDULER_DLQ_ARN},
        },
    }
    try:
        scheduler.create_schedule(**params)
    except ClientError as err:
        if err.response["Error"]["Code"] != "ConflictException":
            raise
        scheduler.update_schedule(**params)  # deadline changed / re-opened task
    logger.info("expiry_scheduled", extra={"taskId": msg["taskId"], "type": "SCHEDULE", "deadline": task["Deadline"],
                                           "schedule": params["Name"]})


def handle_cancel(msg):
    try:
        scheduler.delete_schedule(Name=schedule_name(msg["taskId"]), GroupName=SCHEDULE_GROUP)
        logger.info("expiry_cancelled", extra={"taskId": msg["taskId"], "type": "CANCEL", "source": msg.get("source")})
    except ClientError as err:
        if err.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        logger.info("expiry_cancel_noop", extra={"taskId": msg["taskId"], "type": "CANCEL"})


def handle_expire(msg):
    key = {"PK": user_pk(msg["userId"]), "SK": task_sk(msg["taskId"])}
    now = now_utc()
    try:
        task = table().update_item(
            Key=key,
            UpdateExpression="SET #st = :expired, GSI1SK = :gsi, ExpiredAt = :now, UpdatedAt = :now, Version = Version + :one",
            ConditionExpression="attribute_exists(PK) AND #st = :pending AND DeadlineEpoch <= :cutoff",
            ExpressionAttributeNames={"#st": "Status"},
            ExpressionAttributeValues={
                ":expired": STATUS_EXPIRED,
                ":pending": STATUS_PENDING,
                ":gsi": status_sk(STATUS_EXPIRED, msg.get("deadline") or iso(now)),
                ":now": iso(now),
                ":one": 1,
                ":cutoff": int(now.timestamp()) + EXPIRY_TOLERANCE_SECONDS,
            },
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as err:
        if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # Already expired (retry), completed, deleted or rescheduled - decide whether email is still owed.
        task = table().get_item(Key=key, ConsistentRead=True).get("Item")
        if not task or task["Status"] != STATUS_EXPIRED or task.get("NotifiedAt"):
            logger.info("expire_skipped", extra={"taskId": msg["taskId"], "type": "EXPIRE",
                                                 "status": task["Status"] if task else "Deleted"})
            return

    logger.info("task_expired", extra={"taskId": msg["taskId"], "type": "EXPIRE", "deadline": task["Deadline"]})
    notify(task)


def notify(task):
    email = (task.get("UserEmail") or "").lower()
    if email:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"Task expired: {task['Description'][:60]}",
            Message=(f"Hello,\n\nYour task has expired because it was not completed before its deadline.\n\n"
                     f"  Task:     {task['Description']}\n"
                     f"  Date:     {task['Date']}\n"
                     f"  Deadline: {task['Deadline']}\n"
                     f"  Task ID:  {task['TaskId']}\n\n- Module 8 Serverless To-Do"),
            MessageAttributes={"email": {"DataType": "String", "StringValue": email}},
        )
    try:
        table().update_item(
            Key={"PK": task["PK"], "SK": task["SK"]},
            UpdateExpression="SET NotifiedAt = :now",
            ConditionExpression="attribute_not_exists(NotifiedAt)",
            ExpressionAttributeValues={":now": iso(now_utc())},
        )
    except ClientError as err:
        if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    logger.info("expiry_notification_sent", extra={"taskId": task["TaskId"], "type": "EXPIRE", "notified": bool(email)})


HANDLERS = {"SCHEDULE": handle_schedule, "CANCEL": handle_cancel, "EXPIRE": handle_expire}


def handler(event, _context):
    failures = []
    for index, record in enumerate(event["Records"]):
        try:
            msg = json.loads(record["body"])
            HANDLERS[msg["type"]](msg)
        except Exception:
            logger.exception("task_event_failed", extra={"messageId": record["messageId"], "body": record["body"]})
            # FIFO: stop here so later messages of the same group are not processed out of order.
            failures.extend({"itemIdentifier": r["messageId"]} for r in event["Records"][index:])
            break
    return {"batchItemFailures": failures}
