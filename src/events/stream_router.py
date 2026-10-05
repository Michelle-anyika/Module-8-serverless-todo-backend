"""DynamoDB Streams consumer. Turns task changes into lifecycle events on the
SQS FIFO queue (MessageGroupId = TaskId keeps each task's events ordered):

  INSERT, Pending                         -> SCHEDULE  (create expiry schedule)
  MODIFY, -> Pending or deadline changed  -> SCHEDULE  (create/update schedule)
  MODIFY, Pending -> Completed            -> CANCEL    (delete expiry schedule)
  REMOVE, was Pending                     -> CANCEL
  anything else (e.g. Pending -> Expired) -> ignored

The stream eventID is used as the FIFO deduplication id, so a stream retry
never produces a duplicate lifecycle event."""
import json
import logging
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["QUEUE_URL"]
_deserializer = TypeDeserializer()


def _image(record, name):
    raw = record["dynamodb"].get(name)
    return {k: _deserializer.deserialize(v) for k, v in raw.items()} if raw else None


def classify(event_name, old, new):
    if event_name == "INSERT":
        return "SCHEDULE" if new and new.get("Status") == "Pending" else None
    if event_name == "MODIFY":
        if new.get("Status") == "Pending" and (old.get("Status") != "Pending" or old.get("Deadline") != new.get("Deadline")):
            return "SCHEDULE"
        if old.get("Status") == "Pending" and new.get("Status") == "Completed":
            return "CANCEL"
        return None
    if event_name == "REMOVE":
        return "CANCEL" if old and old.get("Status") == "Pending" else None
    return None


def handler(event, _context):
    failures = []
    for record in event["Records"]:
        seq = record["dynamodb"]["SequenceNumber"]
        try:
            old, new = _image(record, "OldImage"), _image(record, "NewImage")
            task = new or old
            action = classify(record["eventName"], old or {}, new or {})
            if not action:
                continue
            message = {
                "type": action,
                "taskId": task["TaskId"],
                "userId": task["UserId"],
                "deadline": task.get("Deadline"),
                "source": f"dynamodb:{record['eventName']}",
                "sourceEventId": record["eventID"],
            }
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message),
                MessageGroupId=task["TaskId"],
                MessageDeduplicationId=record["eventID"],
            )
            logger.info("lifecycle_event_enqueued", extra=message)
        except Exception:
            logger.exception("stream_record_failed", extra={"sequenceNumber": seq})
            failures.append({"itemIdentifier": seq})
            break  # preserve ordering: retry from the first failed record
    return {"batchItemFailures": failures}
