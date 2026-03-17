import json
import os
import logging
from datetime import datetime, timedelta, timezone

import boto3
from kiteconnect import KiteConnect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

SECRET_NAME = os.environ["SECRET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
TRADE_COUNTER_TABLE = os.environ["TRADE_COUNTER_TABLE"]
TRADE_HISTORY_TABLE = os.environ["TRADE_HISTORY_TABLE"]
MAX_TRADES_PER_DAY = int(os.environ["MAX_TRADES_PER_DAY"])
SNS_TOPIC = os.environ["SNS_TOPIC"]

table = dynamodb.Table(TABLE_NAME)
counter_table = dynamodb.Table(TRADE_COUNTER_TABLE)
history_table = dynamodb.Table(TRADE_HISTORY_TABLE)


def lambda_handler(event, context):

    try:

        secret = get_secret()

        for record in event["Records"]:

            body = json.loads(record["body"])

            symbol = body["symbol"]
            action = body["action"]

            logger.info(f"Received signal {symbol} {action}")

            if action == "BUY":

                if not check_daily_limit():
                    logger.info("Daily trade limit reached")
                    continue

                state = get_position(symbol)

                if state == "LONG":
                    logger.info("Position already open")
                    continue

                order_id = execute_trade(symbol, "BUY", secret)

                update_position(symbol, "LONG")
                record_trade(symbol, "BUY", order_id)
                increment_trade_counter()
                send_success_notification(symbol, "BUY", order_id)

            elif action == "SELL":

                state = get_position(symbol)

                if state != "LONG":
                    logger.info("No position open")
                    continue

                order_id = execute_trade(symbol, "SELL", secret)

                update_position(symbol, "NONE")
                record_trade(symbol, "SELL", order_id)
                send_success_notification(symbol, "SELL", order_id)

    except Exception as e:

        logger.exception("Execution failed")

        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject="Trading Error",
            Message=str(e)
        )


def get_secret():

    res = secrets_client.get_secret_value(
        SecretId=SECRET_NAME
    )

    return json.loads(res["SecretString"])


def get_position(symbol):

    res = table.get_item(Key={"symbol": symbol})

    if "Item" not in res:
        return "NONE"

    return res["Item"]["position"]


def update_position(symbol, position):

    table.put_item(
        Item={
            "symbol": symbol,
            "position": position,
            "updated_at": datetime.utcnow().isoformat()
        }
    )


def check_daily_limit():

    today = datetime.utcnow().strftime("%Y-%m-%d")

    res = counter_table.get_item(Key={"trade_date": today})

    if "Item" not in res:
        return True

    count = res["Item"]["trade_count"]

    return count < MAX_TRADES_PER_DAY


def get_expiry_time():

    IST = timezone(timedelta(hours=5, minutes=30))

    now = datetime.now(IST)

    tomorrow_midnight = (now + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    return int(tomorrow_midnight.timestamp())


def increment_trade_counter():

    today = datetime.utcnow().strftime("%Y-%m-%d")

    expiry_time = get_expiry_time()

    counter_table.update_item(
        Key={"trade_date": today},
        UpdateExpression="ADD trade_count :inc SET expiry_time = :ttl",
        ExpressionAttributeValues={
            ":inc": 1,
            ":ttl": expiry_time
        }
    )


def record_trade(symbol, action, order_id):

    trade_id = f"{symbol}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{action}"

    history_table.put_item(
        Item={
            "trade_id": trade_id,
            "symbol": symbol,
            "action": action,
            "order_id": order_id,
            "timestamp": datetime.utcnow().isoformat()
        }
    )


def execute_trade(symbol, action, secret):

    kite = KiteConnect(api_key=secret["api_key"])
    kite.set_access_token(secret["access_token"])

    transaction = "BUY" if action == "BUY" else "SELL"

    order_id = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=secret["exchange"],
        tradingsymbol=secret["tradingsymbol"],
        transaction_type=transaction,
        quantity=secret["quantity"],
        order_type="MARKET",
        product=secret["product"]
    )

    logger.info(f"Order placed {order_id}")

    return order_id


def send_success_notification(symbol, action, order_id):

    message = f"""
Trade Executed Successfully

Symbol: {symbol}
Action: {action}
Order ID: {order_id}
Time: {datetime.utcnow().isoformat()}
"""

    sns.publish(
        TopicArn=SNS_TOPIC,
        Subject="Trade Executed",
        Message=message
    )