import json
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
}


class HttpError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def _default(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Unserialisable type {type(value)}")


def response(status, body=None):
    return {
        "statusCode": status,
        "headers": {**CORS_HEADERS, "Content-Type": "application/json"},
        "body": "" if body is None else json.dumps(body, default=_default),
    }


def current_user(event):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        raise HttpError(401, "Unauthorized")
    return user_id, claims.get("email", "")


def json_body(event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        raise HttpError(400, "Request body must be valid JSON")
    if not isinstance(body, dict):
        raise HttpError(400, "Request body must be a JSON object")
    return body


def path_param(event, name):
    value = (event.get("pathParameters") or {}).get(name)
    if not value:
        raise HttpError(400, f"Missing path parameter {name}")
    return value


def api_handler(fn):
    """Uniform error handling + structured request logging for API handlers."""

    def wrapper(event, context):
        ctx = event.get("requestContext", {})
        try:
            result = fn(event, context)
            logger.info("request_ok", extra={"route": ctx.get("resourcePath"), "method": ctx.get("httpMethod"),
                                             "status": result["statusCode"]})
            return result
        except HttpError as err:
            logger.warning("request_rejected", extra={"route": ctx.get("resourcePath"), "status": err.status,
                                                      "reason": err.message})
            return response(err.status, {"message": err.message})
        except Exception:
            logger.exception("request_failed", extra={"route": ctx.get("resourcePath")})
            return response(500, {"message": "Internal server error"})

    wrapper.__name__ = fn.__name__
    return wrapper
