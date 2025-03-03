import logging

import simplejson
from valr_python import Client
from Keys import *


def post_limit_order(side: str, quantity: float, price: int, customer_order_id: str,
                     pair: str = "BTCZAR", post_only: bool = True):
    """Post a order on the VALR exchange
    :param side: BUY or SELL
    :param quantity: amount of crypto
    :param price: the price of crypto
    :param customer_order_id: Your own id for the trade
    :param pair: trade pair
    :param post_only: True or False
    :return: a dict with id if correct
    """
    logging.info(f"{customer_order_id}, {side}: post_limit_order ")
    c = Client(api_key=API_KEY, api_secret=API_SECRET)
    limit_order = {
        "side": side,
        "quantity": f"{quantity}",
        "price": f"{price}",
        "pair": pair,
        "post_only": post_only,
        "customer_order_id": customer_order_id
    }
    return c.post_limit_order(**limit_order)


def delete_order(customer_order_id: str, pair: str = "BTCZAR"):
    """
    :param customer_order_id:
    :param pair:
    :return:
    """
    c = Client(api_key=API_KEY, api_secret=API_SECRET)
    limit_order = {
        "pair": pair,
        "customer_order_id": customer_order_id
    }
    try:
        c.delete_order(**limit_order)
    except simplejson.errors.JSONDecodeError:
        pass


def order_status(customer_order_id: str, pair: str = "BTCZAR"):
    """
    :param customer_order_id:
    :param pair:
    :return:
    """
    c = Client(api_key=API_KEY, api_secret=API_SECRET)
    limit_order = {
        "currency_pair": pair,
        "customer_order_id": customer_order_id
    }
    return c.get_order_status(**limit_order)


def last_trade_exchange(limit: int = 1, pair: str = "BTCZAR"):
    limit_order = {
        "currency_pair": pair,
        "limit": limit
    }
    c = Client(api_key=API_KEY, api_secret=API_SECRET)
    return c.get_market_data_trade_history(**limit_order)
