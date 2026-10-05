import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, _context):
    """Cognito PreSignUp trigger: auto-confirm the user and mark the email verified."""
    event["response"]["autoConfirmUser"] = True
    if "email" in event["request"].get("userAttributes", {}):
        event["response"]["autoVerifyEmail"] = True
    logger.info("user_auto_confirmed", extra={"userName": event.get("userName"), "trigger": event.get("triggerSource")})
    return event
