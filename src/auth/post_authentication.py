"""Cognito PostAuthentication trigger: subscribe the user's email to the task
notification topic. A per-subscription filter policy on the `email` message
attribute guarantees each user only receives notifications for their own tasks.

Never raises - a notification problem must not block sign-in."""
import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")
TOPIC_ARN = os.environ["TOPIC_ARN"]


def _existing_subscription(email):
    paginator = sns.get_paginator("list_subscriptions_by_topic")
    for page in paginator.paginate(TopicArn=TOPIC_ARN):
        for sub in page["Subscriptions"]:
            if sub["Protocol"] == "email" and sub["Endpoint"].lower() == email:
                return sub["SubscriptionArn"]
    return None


def handler(event, _context):
    email = event["request"]["userAttributes"].get("email", "").lower()
    if not email:
        return event
    try:
        existing = _existing_subscription(email)
        if existing:
            logger.info("sns_subscription_exists",
                        extra={"email": email, "pendingConfirmation": existing == "PendingConfirmation"})
            return event
        arn = sns.subscribe(
            TopicArn=TOPIC_ARN,
            Protocol="email",
            Endpoint=email,
            Attributes={"FilterPolicy": json.dumps({"email": [email]}), "FilterPolicyScope": "MessageAttributes"},
            ReturnSubscriptionArn=True,
        )["SubscriptionArn"]
        logger.info("sns_subscription_created", extra={"email": email, "subscriptionArn": arn})
    except Exception:
        logger.exception("sns_subscription_failed", extra={"email": email})
    return event
