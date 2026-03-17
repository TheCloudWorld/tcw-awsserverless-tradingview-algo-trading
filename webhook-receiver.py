import json
import os
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
sqs = boto3.client("sqs")

SECRET_NAME = os.environ["SECRET_NAME"]
QUEUE_URL = os.environ["SQS_QUEUE_URL"]

cached_secret = None


def lambda_handler(event, context):

    try:

        body = json.loads(event["body"])

        secret = get_secret()

        # Webhook Secret Validation
        if body.get("secret") != secret["webhook_secret"]:

            logger.warning("Unauthorized webhook request")

            return {
                "statusCode": 403,
                "body": "Unauthorized"
            }

        logger.info("Valid webhook received")

        # Push signal to SQS
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(body)
        )

        logger.info("Signal pushed to SQS")

        # Respond immediately to TradingView
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Signal received"})
        }

    except Exception as e:

        logger.exception("Webhook processing failed")

        return {
            "statusCode": 500,
            "body": str(e)
        }


def get_secret():

    global cached_secret

    if cached_secret:
        return cached_secret

    res = secrets_client.get_secret_value(
        SecretId=SECRET_NAME
    )

    cached_secret = json.loads(res["SecretString"])

    return cached_secret