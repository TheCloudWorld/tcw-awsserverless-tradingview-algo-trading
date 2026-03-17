import json
import os
import logging
from datetime import datetime

import boto3
from kiteconnect import KiteConnect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
sns_client = boto3.client("sns")

SECRET_NAME = os.environ["SECRET_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):

    try:
        # Log the incoming event for debugging purposes
        logger.info("Received event: %s", json.dumps(event))
        # Extract request_token from query parameters
        query_params = event.get("queryStringParameters")
        
        # Validate presence of request_token
        if not query_params or "request_token" not in query_params:
            return response(400, "request_token missing")
        
        # Extract the request_token
        request_token = query_params["request_token"]

        # Fetch API credentials from Secrets Manager
        secret_data = get_secret()
        # API Key & API Secret are static, stored in Secrets Manager.
        api_key = secret_data["api_key"]
        api_secret = secret_data["api_secret"]
        # Access token is generated dynamically and updated in Secrets Manager after generation.
        logger.info("Generating Zerodha access token")
        
        # Initialize KiteConnect with API key
        kite = KiteConnect(api_key=api_key)
        # Generate session using request_token and API secret to get access token
        session_data = kite.generate_session(
            request_token,
            api_secret=api_secret
        )
        # Extract access token from session data
        access_token = session_data["access_token"]
        # Update secret with new access token and request token
        update_secret(secret_data, access_token, request_token)
        # Send notification about successful token generation
        logger.info("Access token generated successfully, sending notification")
        send_notification()

        return response(200, "Access token generated and stored")

    except Exception as e:

        logger.exception("Authentication failed")

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Zerodha Login Failed",
            Message=str(e)
        )

        return response(
            500,
            f"Authentication failed: {str(e)}"
        )

# Get the secret from Secrets Manager
def get_secret():

    try:

        response = secrets_client.get_secret_value(
            SecretId=SECRET_NAME
        )

        secret = json.loads(response["SecretString"])

        return secret

    except Exception as e:

        logger.exception("Failed to retrieve secret")

        raise e

# Update the secret with new access token and request token
def update_secret(secret_data, access_token, request_token):

    # Update only dynamic fields
    secret_data["access_token"] = access_token
    secret_data["request_token"] = request_token
    secret_data["generated_at"] = datetime.utcnow().isoformat()

    secrets_client.update_secret(
        SecretId=SECRET_NAME,
        SecretString=json.dumps(secret_data)
    )

    logger.info("Secrets Manager updated successfully")

# Send notification to SNS topic about successful token generation
def send_notification():

    message = f"""
Zerodha login successful.

Access token generated and stored in Secrets Manager.

Time: {datetime.utcnow().isoformat()}

Trading system ready.
"""

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Zerodha Access Token Generated",
        Message=message
    )

# Standardized response format for API Gateway
def response(code, message):

    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "message": message
        })
    }