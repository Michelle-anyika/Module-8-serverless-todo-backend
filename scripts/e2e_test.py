"""End-to-end test against the deployed stack.

Signs up (auto-confirmed) and signs in a throwaway user, exercises CRUD, then
verifies schedule creation, cancellation on complete/delete, and automatic
expiry of a task with a short deadline.

  python scripts/e2e_test.py [--email someone@example.com] [--keep]
"""
import argparse
import json
import time
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

import boto3

REGION = "eu-north-1"
STACK = "module-8-serverless-todo"


def outputs():
    stack = boto3.client("cloudformation", region_name=REGION).describe_stacks(StackName=STACK)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}


def call(api, token, method, path, body=None):
    req = urllib.request.Request(f"{api}{path}", method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as res:
            raw = res.read()
            return res.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def schedule_exists(scheduler, group, task_id):
    try:
        scheduler.get_schedule(Name=f"module-8-task-{task_id}", GroupName=group)
        return True
    except scheduler.exceptions.ResourceNotFoundException:
        return False


def wait_for(predicate, timeout, label):
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            print(f"  OK  {label} ({time.time() - start:.0f}s)")
            return True
        time.sleep(5)
    print(f"  FAIL {label} (timeout {timeout}s)")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", default=f"module8-e2e-{uuid.uuid4().hex[:6]}@example.com")
    parser.add_argument("--keep", action="store_true", help="keep the test user")
    args = parser.parse_args()

    out = outputs()
    api, pool, client_id, group = out["ApiUrl"], out["UserPoolId"], out["UserPoolClientId"], out["ScheduleGroup"]
    idp = boto3.client("cognito-idp", region_name=REGION)
    scheduler = boto3.client("scheduler", region_name=REGION)
    password = "Module8-" + uuid.uuid4().hex[:8] + "A1"
    results = []

    print(f"Sign up {args.email}")
    idp.sign_up(ClientId=client_id, Username=args.email, Password=password,
                UserAttributes=[{"Name": "email", "Value": args.email}])
    status = idp.admin_get_user(UserPoolId=pool, Username=args.email)["UserStatus"]
    results.append(("sign-up auto-confirmed", status == "CONFIRMED"))
    token = idp.initiate_auth(ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
                              AuthParameters={"USERNAME": args.email, "PASSWORD": password})["AuthenticationResult"]["IdToken"]
    results.append(("sign-in returns token", bool(token)))

    sns = boto3.client("sns", region_name=REGION)
    subs = sns.list_subscriptions_by_topic(TopicArn=out["TaskNotificationsTopicArn"])["Subscriptions"]
    results.append(("PostAuthentication subscribed email to SNS", any(s["Endpoint"] == args.email for s in subs)))

    results.append(("unauthenticated request rejected", call(api, "", "GET", "/tasks")[0] == 401))

    code, a = call(api, token, "POST", "/tasks", {"description": "e2e expire me",
                   "deadline": (datetime.now(timezone.utc) + timedelta(seconds=75)).isoformat()})
    results.append(("create task A (short deadline)", code == 201 and a["Status"] == "Pending"))
    code, b = call(api, token, "POST", "/tasks", {"description": "e2e complete me"})
    results.append(("create task B (default +5m deadline)", code == 201))
    code, c = call(api, token, "POST", "/tasks", {"description": "e2e delete me"})
    results.append(("create task C", code == 201))

    code, got = call(api, token, "GET", f"/tasks/{a['TaskId']}")
    results.append(("get task", code == 200 and got["TaskId"] == a["TaskId"]))
    code, listed = call(api, token, "GET", "/tasks")
    results.append(("list tasks", code == 200 and listed["count"] == 3))
    code, upd = call(api, token, "PUT", f"/tasks/{a['TaskId']}", {"description": "e2e expire me (edited)"})
    results.append(("update description", code == 200 and upd["Description"].endswith("(edited)")))

    print("Waiting for schedules to be created via Stream -> SQS FIFO -> Lambda")
    results.append(("schedule created for A", wait_for(lambda: schedule_exists(scheduler, group, a["TaskId"]), 60, "schedule A")))
    results.append(("schedule created for B", wait_for(lambda: schedule_exists(scheduler, group, b["TaskId"]), 60, "schedule B")))
    results.append(("schedule created for C", wait_for(lambda: schedule_exists(scheduler, group, c["TaskId"]), 60, "schedule C")))

    code, done = call(api, token, "PUT", f"/tasks/{b['TaskId']}", {"status": "Completed"})
    results.append(("complete task B", code == 200 and done["Status"] == "Completed"))
    results.append(("delete task C", call(api, token, "DELETE", f"/tasks/{c['TaskId']}")[0] == 204))
    results.append(("expiry cancelled for completed B", wait_for(lambda: not schedule_exists(scheduler, group, b["TaskId"]), 60, "cancel B")))
    results.append(("expiry cancelled for deleted C", wait_for(lambda: not schedule_exists(scheduler, group, c["TaskId"]), 60, "cancel C")))

    print("Waiting for task A to expire at its deadline")
    results.append(("task A auto-expired", wait_for(
        lambda: call(api, token, "GET", f"/tasks/{a['TaskId']}")[1].get("Status") == "Expired", 240, "expire A")))
    final = call(api, token, "GET", f"/tasks/{a['TaskId']}")[1]
    results.append(("expiry notification published (NotifiedAt set)", bool(final.get("NotifiedAt"))))
    code, expired = call(api, token, "GET", "/tasks?status=Expired")
    results.append(("list by status=Expired (GSI1)", code == 200 and expired["count"] == 1))

    if not args.keep:
        for t in (a, b):
            call(api, token, "DELETE", f"/tasks/{t['TaskId']}")
        idp.admin_delete_user(UserPoolId=pool, Username=args.email)
        for s in sns.list_subscriptions_by_topic(TopicArn=out["TaskNotificationsTopicArn"])["Subscriptions"]:
            if s["Endpoint"] == args.email and s["SubscriptionArn"].startswith("arn:"):
                sns.unsubscribe(SubscriptionArn=s["SubscriptionArn"])

    print("\nRESULTS")
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
