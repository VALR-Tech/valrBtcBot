import logging
from datetime import datetime

from Open_orders import open_buy_orders, open_sell_orders, all_open_orders, add_btc_orders

from Trade_data import TradeData
from sqlite3_functions import *
from Post_orders import delete_order, post_limit_order, order_status


def bot_market(data: dict):
    """The main  function that brings all the other functions together to create a trading bot.
    :param data: the candles from the VALR websockets.
    :return:
    """
    trade_data = TradeData(data)
    if trade_data.period60sec():
        utc_now = datetime.utcnow()  # start time
        # logging.info(f"{utc_now}, 1")  # log start time
        conn = create_connection("TradeDataBTCZAR.db")

        all_orders = all_open_orders()
        add_btc_orders(conn, all_orders)

        buy_orders = type_of_trade(all_orders, side="buy")
        sell_orders = type_of_trade(all_orders, side='sell')

        bought = check_bought(conn, buy_orders)
        print("Bought:", bought)
        sold = check_sold(conn, sell_orders)
        print("Sold:", sold)

        part_buy = check_part_buy(conn, buy_orders)
        part_sell = check_part_sell(conn, sell_orders)

        buys_placed = open_buy_orders(buy_orders)
        buys_to_place = get_all_buys_to_place(conn, trade_data.close_tic, trade_data.low_tic * 0.95)

        buy = check_buys_to_place(buys_to_place, buys_placed, part_buy, sell_orders)
        print("buy:", buy)
        cancel = check_buys_to_cancel(buys_placed, buys_to_place, part_buy)
        print("cancel:", cancel)
        print(1)

        cancel_placed_buy(conn, cancel)
        print(2)
        place_sell(conn, bought, trade_data.high_tic)
        print(3)
        place_buy(conn, buy)
        print(4)

        profit_placement(conn, sold)
        print(5)
        reset_process_position(conn, sold)
        # todo Add func to control total amount of orders
        # todo Add trailing prof
        conn.commit()

        # logging.info(f"{datetime.utcnow() - utc_now}, 6 \n")  # end time
        print(f"{datetime.utcnow() - utc_now}, 6 \n")
        # logging.info("")


def type_of_trade(orders, side: str):
    """ returns a list of open orders of the chosen side
    :param orders: all the open orders
    :param side: the side of open orders to be returned
    :return: a list of open orders of the chosen side
    """
    trades = []
    y = [i for i in orders if i["side"] == side]
    for i in y:
        try:
            if i["customerOrderId"]:
                trades.append(i)
        except KeyError:
            pass
        else:
            pass
    return trades


def check_bought(conn, buy_orders):
    """ Check for bought orders
    :param conn: the Connection object
    :param buy_orders:A list of dict with all the buy open orders
    :return: A list of customerOrderId that contain bought orders
    """
    bought = []
    buy = get_process_position(conn, process_position=1)  # History - info from trades bot sql
    part_buy = get_process_position(conn, process_position=2)
    buys_placed = open_buy_orders(buy_orders)  # present - info from open orders

    y = list(set([p[3] for p in buy]) - set(buys_placed)) + list(set([p[3] for p in part_buy]) - set(buys_placed))

    for i in y:
        res = order_status(i)
        if res["orderStatusType"] == "Filled":
            update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=3)
            bought.append(i)
        elif res["orderStatusType"] == "Cancelled":
            update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=0)
        elif res["orderStatusType"] == "Placed":  # do nothing
            logging.error(f'{res["customerOrderId"]} should have been in open_buy_orders func')
            pass
        elif res["orderStatusType"] == "Failed" \
                and res["failedReason"] == "Post only cancelled as it would have matched":
            update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=3)
            bought.append(i)
        else:
            print(res)
            logging.error(f'NB check bought: {res["orderStatusType"]}, {res["customerOrderId"]}')
            print(f'NB check bought: {res["orderStatusType"]}, {res["customerOrderId"]}')
    return bought


def check_sold(conn, sell_orders):
    """ Check for sold orders
    :param conn:the Connection object
    :param sell_orders:A list of dict with all the sell open orders
    :return: A list of customerOrderId that contain sold orders
    """
    sold = []
    sell = get_process_position(conn, process_position=4)  # History - info from trades bot sql
    part_sell = get_process_position(conn, process_position=5)  # History - info from trades bot sql
    sells_placed = open_sell_orders(sell_orders)  # present- info from open orders

    y = list(set([p[3] for p in sell]) - set(sells_placed)) + list(
        set([p[3] for p in part_sell]) - set(sells_placed))

    for i in y:
        res = order_status(i)
        if res["orderStatusType"] == "Filled":
            update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=6)
            sold.append(i)
        elif res["orderStatusType"] == "Cancelled":
            update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=0)
            logging.error(f'{res["customerOrderId"]} sell order was cancelled')
        elif res["orderStatusType"] == "Placed":  # do nothing as
            logging.error(f'{res["customerOrderId"]} should have been in open_sell_orders func')
            pass
        else:
            print(res)
            logging.error(f'NB check sold: {res["orderStatusType"]}, {res["customerOrderId"]}')
            print(f'NB check sold: {res["orderStatusType"]}')
    return sold


def check_part_buy(conn, buy_orders):
    """ Check for part bought orders
    :param conn: the Connection object
    :param buy_orders: A list of dict(s) of the buy side orders
    :return: A list of customerOrderId
    """
    part_buy = []
    for i in buy_orders:  # check for no partially filled orders
        u = float(i["filledPercentage"])

        if u > 0:
            update_process_position(conn, i["customerOrderId"], 2)
            part_buy.append(i["customerOrderId"])
    return part_buy


def check_part_sell(conn, sell_orders):
    """ Check for part sold orders
    :param conn: the Connection object
    :param sell_orders:A list of dict(s) of the sell side orders
    :return: A list of customerOrderId
    """
    part_sell = []
    for i in sell_orders:  # check for no partially filled orders
        u = float(i["filledPercentage"])

        if u > 0:
            update_process_position(conn, i["customerOrderId"], 5)
            part_sell.append(i["customerOrderId"])
    return part_sell


def check_buys_to_place(buys_to_place, buys_placed, part_buy, sell_orders):
    """checks for buys orders to place
    :param sell_orders:
    :param buys_to_place: buy orders that should be placed according to candle price
    :param buys_placed: buy orders that have been placed (open orders)
    :param part_buy: part buy orders
    :return: a list of customerOrderId of buy orders that should be placed.
    """
    sells_placed = open_sell_orders(sell_orders)
    return list(set(buys_to_place) - set(buys_placed) - set(part_buy) - set(sells_placed))


def check_buys_to_cancel(buys_placed, buys_to_place, part_buy):
    """checks for buys orders to cancel
    :param buys_placed:buy orders that have been placed (open orders)
    :param buys_to_place: buy orders that should be placed according to candle price
    :param part_buy: part buy orders
    :return:a list of customerOrderId of buy orders that should be cancelled. out candle price range
    """
    return list(set(buys_placed) - set(buys_to_place) - set(part_buy))


def cancel_placed_buy(conn, cancel):
    """A func to cancel placed buy orders
    :param conn: the Connection object
    :param cancel: a list of customerOrderId of buy orders that should be cancelled. out candle price range
    :return:
    """
    for item in cancel:
        delete_order(customer_order_id=item)

        res = order_status(item)
        if res["orderStatusType"] == "Cancelled":
            update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=0)
        elif res["orderStatusType"] == "Placed":  # do nothing
            logging.error(f'{res["customerOrderId"]} should have been cancelled in cancel_placed_buy func')
            pass
        else:
            logging.error(f'NB cancel placed buy: {res["orderStatusType"]}, {res["customerOrderId"]}')
            print(f'NB cancel placed buy: {res["orderStatusType"]}, {res["customerOrderId"]}')


def place_buy(conn, buy):
    """ A func to place buy orders on VALR
    :param conn: the Connection object
    :param buy: a list of customerOrderId of buy orders that should be placed on VALR
    :return:
    """
    for item in buy:
        info = get_info_customer_order_id(conn, customer_order_id=item)  # gets info from sql table
        if info[0][9] == 0:
            trade = post_limit_order(side="BUY", quantity=info[0][4], price=info[0][0], customer_order_id=info[0][3])

            if not trade["id"]:
                print(trade)
                logging.error(trade)
            """
            elif trade["failedReason"] == "Post only cancelled as it would have matched":
                update_process_position(conn, customer_order_id=item, process_position=3)
                print(trade)
            """

            res = order_status(item)
            if res["orderStatusType"] == "Placed":
                update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=1)
            elif res["orderStatusType"] == "Cancelled":  # do nothing
                logging.error(f'{res["customerOrderId"]} should have been Placed in place_buy func')
                pass
            elif res["orderStatusType"] == "Failed":  # do nothing
                logging.warning(f'{res["customerOrderId"]} order failed to place')
                pass
            else:
                logging.error(f'NB placed buy: {res["orderStatusType"]}, {res["customerOrderId"]}')
                print(f'NB placed buy: {res["orderStatusType"]}, {res["customerOrderId"]}')
        elif info[0][9] == 4:
            # Trade in already in sell
            pass
        elif info[0][9] == 6:
            update_process_position(conn, customer_order_id=item, process_position=5)
            # Trade just sold
            pass
        elif info[0][9] == 3:
            update_process_position(conn, customer_order_id=item, process_position=2)
            # to correct the system after a error change a bought into a part buy
        else:
            logging.error(f"process position is incorrect, should be 0 is {info[0][9]} {item}")


def place_sell(conn, bought: list, high_tic):
    """ A func to place sell orders on VALR
    :param high_tic:
    :param conn: the Connection object
    :param bought: a list of customerOrderId of sell orders that should be placed on VALR
    :return:
    """
    for item in bought:
        info = get_info_customer_order_id(conn, customer_order_id=item)  # gets info from sql table
        if info[0][9] == 3:
            sell_price = initial_sell_price(high_tic, info[0][0])
            trade = post_limit_order(side="SELL", quantity=info[0][4], price=sell_price, customer_order_id=item)

            if not trade["id"]:
                print(trade)
                logging.error(trade)

            res = order_status(item)
            if res["orderStatusType"] == "Placed":
                update_time_placed(conn, time=datetime.utcnow(), customer_order_id=item)
                update_sell_price(conn, customer_order_id=item, sell_price=sell_price)
                update_process_position(conn, customer_order_id=res["customerOrderId"], process_position=4)

            elif res["orderStatusType"] == "Cancelled":  # do nothing
                logging.error(f'{res["customerOrderId"]} should have been Placed in place_sell func')
                pass
            else:
                logging.error(f'NB placed sell: {res["orderStatusType"]}, {res["customerOrderId"]}')
                print(f'NB placed sell: {res["orderStatusType"]}, {res["customerOrderId"]}')

        else:
            logging.error(f"process position is incorrect, should be 3 is {info[0][9]} {item}")


def initial_sell_price(tic_high: int, buy_price: int) -> int:
    """ The calculation of the initial sell price placement of a bought trade
    Note: is for buy price to be on even number and sell to be odd,
            so that there is never a buy and sell trade on the same price.
    :param buy_price:
    :param tic_high: The high of the minute candle
    :return: The initial sell price placement of a bought trade
    """
    if tic_high > buy_price:
        tic_high = tic_high + 998
    else:
        tic_high = buy_price + 998

    if tic_high % 2 == 0:
        return tic_high + 1
    elif tic_high % 2 == 1:
        return tic_high
    else:
        logging.error(f"initial_sell_price calculation error")
        return tic_high


def profit_placement(conn, sold: list):
    """ A function that takes the profit of a trade to increase the quantity of the next trade.
    :param conn: The Connection object
    :param sold: A list of customerOrderId of sold orders that toke place on VALR.
    :return:
    """
    for sell in sold:
        info = get_info_customer_order_id(conn, sell)
        if round(info[0][4] * info[0][1] / info[0][0], 8) >= 0.0001:
            new_quantity = round(info[0][4] * (info[0][1] + info[0][0]*0.0002) / info[0][0], 8)
            # todo see if quantity is right
        else:
            new_quantity = 0.0001
        update_quantity(conn, customer_order_id=sell, quantity=new_quantity)


def reset_process_position(conn, sold: list, process_position: int = 0):
    for item in sold:
        update_process_position(conn, item, process_position)
        trade_amount(conn, item)


def __datetime(date_str):
    """Convert a string date into a datetime object
    :param date_str: Input date in a string data type
    :return: a datetime object
    """
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")


def delta_time(start_date, end_date):
    """Subtract the end time from the start time.
    :param start_date: start time
    :param end_date: end time
    :return: difference in time
    """
    return __datetime(end_date) - __datetime(start_date)
