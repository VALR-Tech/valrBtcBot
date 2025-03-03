
from sqlite3_functions import create_connection, add_period60sec


class TradeData:
    def __init__(self, data: dict):
        self.data = data

    def period60sec(self):
        """Add 60 sec data for VALR to sqlite3 DB and create variables
        :return: A Boolean True (working) or False
        """
        if self.data['data']['bucketPeriodInSeconds'] == 60:
            self.data = self.data['data']
            conn = create_connection("TradeDataBTCZAR.db")
            add_period60sec(conn, self.data)
            self.start_time = self.data['startTime']
            self.close_tic = int(self.data['close'])
            # self.open_tic = int(self.data['open'])
            self.high_tic = int(self.data['high'])
            self.low_tic = int(self.data['low'])
            # self.quoteVolume_tic = ['quoteVolume']
            # self.volume_tic = self.data['volume']
            return True
        else:
            return False
