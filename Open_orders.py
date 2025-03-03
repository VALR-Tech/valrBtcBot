import logging

from valr_python import Client
from datetime import datetime
from sqlite3_functions import clear_table, add_all_open_orders
from Keys import *


def open_buy_orders(orders):
    """
    :param orders:
    :return:
    """
    placed_buys = []
    for order in orders:
        if order["side"] == "buy":
            try:
                placed_buys.append(order["customerOrderId"])
            except KeyError:
                pass
    return placed_buys


def open_sell_orders(orders):
    """
    :param orders:
    :return:
    """
    placed_sells = []
    for order in orders:
        if order["side"] == "sell":
            try:
                placed_sells.append(order["customerOrderId"])
            except KeyError:
                pass
    return placed_sells


def all_open_orders(api_key: str = API_KEY, api_secret: str = API_SECRET, currency_pair: str = "BTCZAR"):
    """
    :param api_key:
    :param api_secret:
    :param currency_pair:
    :return:
    """
    btc_orders = []
    u = Client(api_key=api_key, api_secret=api_secret)
    open_trades = u.get_all_open_orders()
    for trade in open_trades:
        if trade["currencyPair"] == currency_pair:
            btc_orders.append(trade)
    return btc_orders


def add_btc_orders(conn, orders):
    """
    :param conn:
    :param orders:
    :return:
    """
    clear_table(conn, "all_open_orders")  # clear table for all_open_orders
    for order in orders:
        try:
            add_all_open_orders(conn, order)
        except KeyError:
            # logging.info(f"{datetime.utcnow()}, OpenOrders - KeyError - Trade without customerOrderId")
            pass  # Want to skip open orders without customerOrderId
            # The get orders API call
        except Exception as e:
            logging.error(f"Exception occurred: {e}", exc_info=True)
        else:
            pass
