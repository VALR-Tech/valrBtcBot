import sqlite3
from sqlite3 import Error


def create_connection(db_file: str):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def clear_table(conn, table: str):
    """Clear the data in a sqlite table
    :param conn: the Connection object
    :param table: the table
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table};")
    conn.commit()


def add_period60sec(conn, data: dict):
    """Add the 60sec trade data to the sqlite3 database
    :param conn: the Connection object
    :param data: the trade data
    :return:
    """
    cur = conn.cursor()
    cur.execute("INSERT INTO period60sec VALUES (?,?,?,?,?,?,?,?,?)", [data["currencyPairSymbol"],
                                                                       data["bucketPeriodInSeconds"],
                                                                       data["startTime"],
                                                                       data["open"],
                                                                       data["high"],
                                                                       data["low"],
                                                                       data["close"],
                                                                       data["close"],
                                                                       data["quoteVolume"]])
    conn.commit()


def add_all_open_orders(conn, data: dict):
    """Add all the open orders trade data to the sqlite3 database
    :param conn: the Connection object
    :param data: the trade data
    :return:
    """
    cur = conn.cursor()
    cur.execute("INSERT INTO all_open_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);",
                [data["createdAt"],
                 data["currencyPair"],
                 data["customerOrderId"],
                 data["filledPercentage"],
                 data["orderId"],
                 data["originalQuantity"],
                 data["price"],
                 data["remainingQuantity"],
                 data["side"],
                 data["status"],
                 data["timeInForce"],
                 data["type"],
                 data["updatedAt"]])
    conn.commit()


def get_open_orders_info(conn, buy_price: int):
    """Get a list of all the buy order that need/have been placed
        :param conn: the Connection object
        :param buy_price: the low of the candle
        :return: a list of all the buy orders that should be placed
        """
    cur = conn.cursor()
    print("\n")
    print(f"SELECT * FROM all_open_orders WHERE price = {buy_price};")
    cur.execute(f"SELECT * FROM all_open_orders WHERE price = {buy_price};")
    return cur.fetchall()


def get_all_buys_to_place(conn, high_price: int, low_price: int):
    """Get a list of all the buy order that need/have been placed
    :param conn: the Connection object
    :param low_price: the low of the candle - x amount
    :param high_price: the close of the candle
    :return: a list of all the buy orders that should be placed (buyPrice)
    """
    cur = conn.cursor()
    cur.execute(f"SELECT customerOrderId FROM trades_bot WHERE buyPrice BETWEEN {low_price} AND {high_price}")
    return [i[0] for i in cur.fetchall()]


def get_info_buy_price(conn, buy_price: int):
    """Get a list of all the buy order that need/have been placed
    :param conn: the Connection object
    :param buy_price: the low of the candle
    :return: a list of all the buy orders that should be placed
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM trades_bot WHERE buyPrice = {buy_price};")
    return cur.fetchall()


def get_info_customer_order_id(conn, customer_order_id: str):
    """Get a list of all the buy order that need/have been placed
    :param conn: the Connection object
    :param customer_order_id:
    :return: a list of all the buy orders that should be placed
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM trades_bot WHERE customerOrderId = '{customer_order_id}';")
    return cur.fetchall()


def update_process_position(conn, customer_order_id: str, process_position: int = 0):
    """UPDATE the process_position based on the customer_order_id
    :param conn: the Connection object
    :param customer_order_id:
    :param process_position:
                    0 - Wait to place buy
                    1 - Placed buy
                    2 - Part buy
                    3 - Bought
                    4 - Placed sell
                    5 - Part sell
                    6 - Sold
                    7 - Profit placement
                    8 - Reset
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"""UPDATE trades_bot SET processPosition = {process_position} 
                    WHERE customerOrderId = '{customer_order_id}';""")
    conn.commit()


def update_process_position_buy_price(conn, buy_price: int, process_position: int = 0):
    """UPDATE the process_position based on the customer_order_id
    :param conn: the Connection object
    :param buy_price: the low of the candle
    :param process_position:
                    0 - Wait to place buy
                    1 - Placed buy
                    2 - Part buy
                    3 - Bought
                    4 - Place sell
                    5 - Part sell
                    6 - Sold
                    7 - Profit placement
                    8 - Reset
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"""UPDATE trades_bot SET processPosition = {process_position} 
                    WHERE buyPrice = '{buy_price}';""")
    conn.commit()


def get_process_position(conn, process_position: int):
    """
    :param conn: the Connection object
    :param process_position: the trade position (int)
    :return: all the info for rows with the selected process_position. returns a list of tuples
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM trades_bot WHERE processPosition = {process_position};")
    return cur.fetchall()


def update_sell_price(conn, customer_order_id: str, sell_price: int):
    """
    :param conn: The Connection object
    :param customer_order_id: The id a customer chooses for a order
    :param sell_price: The sell price of a trade.
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"""UPDATE trades_bot SET sellPrice = {sell_price} 
                    WHERE customerOrderId = '{customer_order_id}';""")
    conn.commit()


def update_time_placed(conn, time, customer_order_id: str):
    """
    :param conn:
    :param time:
    :param customer_order_id:
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"""UPDATE trades_bot SET timePlaced = '{time}' 
                            WHERE customerOrderId = '{customer_order_id}';""")
    conn.commit()


def update_quantity(conn, customer_order_id: str, quantity: float):
    cur = conn.cursor()
    cur.execute(f"""UPDATE trades_bot SET quantity = {quantity} 
                            WHERE customerOrderId = '{customer_order_id}';""")
    conn.commit()


def trade_amount(conn, customer_order_id: str):
    cur = conn.cursor()
    cur.execute(f"SELECT amountTrades FROM trades_bot WHERE customerOrderId = '{customer_order_id}';")
    i = cur.fetchall()
    cur.execute(f"""UPDATE trades_bot SET amountTrades = {int(i[0][0]) + 1} 
                            WHERE customerOrderId = '{customer_order_id}';""")
    conn.commit()


def get_date(conn):
    dates = []
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM trades_bot WHERE processPosition = 4;")
    items = cur.fetchall()
    for i in items:
        if i[2] == '':
            pass
        else:
            dates.append(i)
    return dates
