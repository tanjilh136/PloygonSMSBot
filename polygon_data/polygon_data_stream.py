import os
from threading import Thread

import requests
from custom_time.custom_time import CustomTimeZone
import pandas as pd
from polygon import WebSocketClient, STOCKS_CLUSTER
import json
import glob
import time

from message_handler.messenger import Messenger


class PolygonRealTimeTradeData:
    MIN_CHANGE_DEFAULT = 100000
    MAX_CHANGE_DEFAULT = -100000
    BACKUP_PATH = "../app_data/backup_data"

    def __init__(self, messenger: Messenger, tickers="T.*",
                 packup_at_utc_tomorrow="19:59:59", backup_mode=True, backup_interval_min=5, recover_now=True):
        """
        default UTC TIME is equals california 12:59:59pm
        The program must not crash or stop receiving data for each 24 hrs. When the program starts it will
        Consider the first data as initial data. If it crashes, the calculation will effect the whole process
        :param messenger: Messenger() object by which incoming/outgoing messages are handled
        :param tickers:
        :param packup_at_utc_tomorrow:
        :param backup_mode: If True then data are saved every recovery_interval_min
        :param backup_interval_min: Backup every specified minute
        :param recover_now: Recover backup file if backup file is available
        """
        self.incoming_command_queue = messenger.received_command_queue  # Auto updates  from SMSBOT
        self.incoming_ticker_queue = messenger.received_ticker_queue  # Auto updates from SMSBOT
        self.key = 'Zay2cQZwZfUTozLiLmyprY4Sr3uK27Vp'  # Polygon Key
        self.my_client = WebSocketClient(cluster=STOCKS_CLUSTER, auth_key=self.key,
                                         process_message=self.on_message_received,
                                         on_close=self.on_close, on_error=self.on_error)
        self.target_tickers = tickers
        self.custom_time = CustomTimeZone()
        self.all_symbols = []
        self.init_all_symbol_names_from_file()

        self.last_msg_time_sec = {}
        self.init_last_msg_time_sec()

        self.prev_week_highest_volatility = {}
        self.init_prev_week_highest_volatility_from_file()

        self.current_day_data = {}
        self.init_current_day_data()

        self.today_sms_data = []
        self.current_week_sms_data = []
        self.init_current_week_sms_data_from_file()



        self.current_day_count = self.how_many_days_elapsed() + 1
        self.pack_up_current_day_at_utc_time = packup_at_utc_tomorrow
        self.packup_at_utc_sec = 0
        self.init_start_packup_timestamp()
        self.messenger = messenger

        if recover_now:
            # It will override today_sms_data, current_week_sms_data, current_day_data
            self.recover_backup_data_from_file()

        if backup_mode:
            self.turn_on_backup_mode(interval=backup_interval_min)
            print("Backup Mode Turned ON")

    def turn_on_backup_mode(self, interval=5):
        backup_thread = Thread(target=self.backup_data_for_recovery, args=[interval])
        backup_thread.start()

    def recover_backup_data_from_file(self):
        """
        Called on startup, if recover_now == True.
        Recovers Current Day data, today's msg data, weekly msg data that was backup last time
        :return:
        """
        try:
            with open(f"{self.BACKUP_PATH}/current_day.txt", "r") as day_data:
                self.current_day_data = json.load(day_data)
                print("**RECOVERED DATA**")
                print(f"__________CURRENT DAY DATA RECOVERED__________\n")

            with open(f"{self.BACKUP_PATH}/today_sms_data.txt", "r") as today_sms:
                self.today_sms_data = json.load(today_sms)
                print(f"___________TODAY SMS DATA RECOVERED___________\n{self.today_sms_data}")

            with open(f"{self.BACKUP_PATH}/current_week_sms_data.txt", "r") as week_sms:
                self.current_week_sms_data = json.load(week_sms)
                print(f"___________WEEKLY SMS DATA RECOVERED__________\n{self.current_week_sms_data}")

            with open(f"{self.BACKUP_PATH}/packup_utc_sec.txt", "r") as packup_time:
                self.packup_at_utc_sec = json.load(packup_time)
                print(f"___________PACKUP TIME RECOVERED__________\nNEXT PACKUP: {self.packup_at_utc_sec}")

        except Exception as e:
            print("Nothing to recover")

    def backup_data_for_recovery(self, interval):
        """
        1. Backup Current Day data
        2. Backup today sms data
        3. Backup current week sms data
        :param interval:
        :return:
        """

        while True:
            time.sleep(interval * 60)
            with open(f"{self.BACKUP_PATH}/current_day.txt", "w") as week:
                week.write(json.dumps(self.current_day_data))
                print("BACKUP SUCCESS: CURRENT DAY DATA")

            with open(f"{self.BACKUP_PATH}/today_sms_data.txt", "w") as week:
                week.write(json.dumps(self.today_sms_data))
                print("BACKUP SUCCESS: TODAY SMS DATA")

            with open(f"{self.BACKUP_PATH}/current_week_sms_data.txt", "w") as week:
                week.write(json.dumps(self.current_week_sms_data))
                print("BACKUP SUCCESS: CURRENT WEEK SMS DATA")

            with open(f"{self.BACKUP_PATH}/packup_utc_sec.txt", "w") as packup_time:
                packup_time.write(json.dumps(self.packup_at_utc_sec))
                print(f"BACKUP SUCCESS: PACKUP UTC TIME")

    def has_news(self, symbol):
        """
        Finds out if today we have a news on specified symbol
        :param symbol:
        :return:
        """
        curr_date = self.custom_time.get_current_utc_iso_date()
        api = f"https://api.polygon.io/v2/reference/news?limit=1&order=descending&sort=published_utc&ticker={symbol}&published_utc={curr_date}&apiKey={self.key}"
        res = requests.get(api)
        res = json.loads(res.text)
        if len(res["results"]) > 0:
            return True
        else:
            return False

    def packup_for_today(self):
        """
        1. Setup new timestamp for start and packup
        2. Dump current day_data
            a. If 7 days completes redefine the previous week highest volatility
            b. Delete day1.txt to day2.txt
        3. Dump Current week sms data (todays sms data are added in currentweek data. If not added, add them first then proceed)
        4. Initialize current day data with default value
        :return:
        """
        self.init_start_packup_timestamp()
        print("Packup time reset")
        self.dump_current_day_data()
        print("Current Day data dumped")
        self.dump_current_week_sms_data()
        print("Week data dumped")
        self.init_current_day_data()
        print("Init Current day data")

    def init_start_packup_timestamp(self):
        custom_time = CustomTimeZone()

        # Times are considered as NewYork time
        curr_year, curr_month, curr_day = custom_time.get_current_utc_iso_date().strip().split("-")
        pack_hr, pack_min, pack_sec = self.pack_up_current_day_at_utc_time.strip().split(":")
        self.packup_at_utc_sec = custom_time.get_utc_stamp(y=int(curr_year), m=int(curr_month), d=int(curr_day),
                                                           h=int(pack_hr),
                                                           mi=int(pack_min), s=int(pack_sec), plusday=1)
        print(f"NEXT PACKUP : {self.packup_at_utc_sec}")

    def init_current_week_sms_data_from_file(self):
        with open("../app_data/week_data/current_week_sms_data.txt") as file:
            self.current_week_sms_data = json.load(file)
            print("....SMS DATA INITIALIZED FROM FILE.....")
            print(self.current_week_sms_data)

    def reset_current_week_sms_data(self):
        self.current_week_sms_data = []
        with open("../app_data/week_data/current_week_sms_data.txt", "w") as file:
            file.write(json.dumps(self.current_week_sms_data))
            print("........CURRENT WEEK SMS DATA RESET SUCCESSFUL.........")
            print(self.current_week_sms_data)

    def dump_current_week_sms_data(self):
        """
        Whenever we send an sms on 20% growth, we add the symbol name in current_week_sms_data
        Use this function to make the list persistent
        :return:
        """
        with open("../app_data/week_data/current_week_sms_data.txt", "w") as file:
            file.write(json.dumps(self.current_week_sms_data))
            print(".......CURRENT WEEK SMS DATA DUMPING SUCCESSFUL........")
            print(self.current_week_sms_data)

    def init_last_msg_time_sec(self, value=0):
        """
        Initialize last_msg_time_sec data with initial value
        :return:
        """
        self.last_msg_time_sec = {}
        for sym in self.all_symbols:
            self.last_msg_time_sec[sym] = value

    def how_many_days_elapsed(self):
        """
        The day_data folder contains only day files. Count of those files starting
        with text 'day' is the total day elapsed
        :return:
        """
        list_of_days = glob.glob("../app_data/day_data/day*.txt")
        print("..........Day Data File Paths..........")
        print(list_of_days)
        return len(list_of_days)

    def reset_weekly_highest_volatility_file(self, value=0):
        """
        Resets the previous_week_highest_volatility.txt with initial value 0
        :return:
        """

        week_volatility = {}
        for sym in self.all_symbols:
            week_volatility[sym] = value
        with open("../app_data/week_data/previous_week_highest_volatility.txt", "w") as week:
            week.write(json.dumps(week_volatility))

    def init_prev_week_highest_volatility_from_file(self):
        """
        Read highest volatility from file and initialized it into prev_week_highest_volatility
        :return:
        """
        with open("../app_data/week_data/previous_week_highest_volatility.txt") as file:
            self.prev_week_highest_volatility = json.load(file)
            print("..........PREVIOUS_WEEK_VOLATILITY.............")
            print(self.prev_week_highest_volatility)

    def init_all_symbol_names_from_file(self):
        """
        Initialize all ticker symbol names from the csv file into all_symbols as list
        :return:
        """
        df = pd.read_csv("../app_data/company_tickers.csv")
        self.all_symbols = df["symbol"].to_numpy()

    def redefine_weekly_highest_volatility(self):
        """
        Find highest volatility of each symbol from day1.txt to day7.txt files and dump the highest
        volatility dictionary into the previous_week_volatility.txt file
        :return:
        """
        print("REDEFINING WEEKLY HIGHEST VOLATILITY")
        print("CALCULATING WEEKLY HIGHEST VOLATILITY")
        self.reset_weekly_highest_volatility_file()
        self.init_prev_week_highest_volatility_from_file()

        list_of_days = glob.glob("../app_data/day_data/day*.txt")
        for day_data_path in list_of_days:
            with open(day_data_path) as file:
                day_data = json.load(file)
                # Iterate over each ticker symbol and find the largest
                # day_data and initialize it into prev_week_highest_volatility
                for sym in self.all_symbols:
                    if self.prev_week_highest_volatility[sym] < day_data[sym]["volatility"]:
                        self.prev_week_highest_volatility[sym] = day_data[sym]["volatility"]
                print(f"Successfully Analyzed file : {day_data_path}")

        # Dump calculated volatility into previous_week_highest_volatility.txt file
        with open("../app_data/week_data/previous_week_highest_volatility.txt", "w") as week:
            week.write(json.dumps(self.prev_week_highest_volatility))
            print(".....SUCCESSFULLY DUMPED HIGHEST VOLATILITY OF PREV WEEK TICKERS.......")
            print(self.prev_week_highest_volatility)
            print("....Deleting Day1.txt to Day7.txt....")
            self.delete_last_days_file()

    def delete_last_days_file(self):
        """
        delete day files from day1.txt to day7.txt.
        must not face exception, otherwise bot will not work
        :return:
        """
        print("DELETING FILES")
        list_of_days = glob.glob("../app_data/day_data/day*.txt")
        for day_file in list_of_days:
            try:
                os.remove(day_file)
                print(f"DELETED : {day_file}")
            except Exception as e:
                print(
                    "Day file could not be deleted, please permit deleting."
                    "Otherwise bot will not work properly")
                print(e)

    def dump_current_day_data(self):
        """
        Dump current day data into day*.txt file, so that we can calculate highest volatility
        from 7 days When total 7 days count is completed, calculate the highest volatility
        and dump it into previous_week_highest_volatility
        :return:
        """
        elapsed_day = self.how_many_days_elapsed()
        with open(f"../app_data/day_data/day{elapsed_day + 1}.txt", "w") as week:
            week.write(json.dumps(self.current_day_data))
            self.today_sms_data = []
            print(f"DUMMPED {elapsed_day + 1}")

        if self.how_many_days_elapsed() == 7:
            print("WEEK COMPLETED")
            self.redefine_weekly_highest_volatility()
            self.reset_current_week_sms_data()
            self.today_sms_data = []

    def on_websocket_closed_dump_backup_data_on_crash(self):
        """
        When websocket is closed because of any error, this function will
        dump current_data_data into a txt file, so that it can be used later
        dump current_week_sms_data into a txt file, so that it can be used later
        :return:
        """
        print("Dump important data that need to be recovered on restart")

    def on_web_socket_open_recover_data_after_crash(self):
        """
        Recover from backup data, so that the program can continue without losing previously calculated data
        but will will not be able to fetch missing seconds data, as it was not calculated
        :return:
        """
        print("Read backup data so that bot can run smoothly")

    def init_current_day_data(self, init_price=0, min_change=100000, max_change=-100000, volatility=0):
        """
        Initialize current_day_data with initial value.
        It happens every day before starting the fetching process for each day stock data
        :return:
        """
        self.MIN_CHANGE_DEFAULT = min_change
        self.MAX_CHANGE_DEFAULT = max_change
        self.current_day_data = {}
        for symbol in self.all_symbols:
            ticker_dict = {
                "init_price": init_price,
                "min_change": min_change,
                "max_change": max_change,
                "volatility": volatility
            }
            self.current_day_data[symbol] = ticker_dict

    def start_fetching(self):
        self.my_client.run_async()
        self.my_client.subscribe(self.target_tickers)

    def on_close(self, ws):
        self.on_websocket_closed_dump_backup_data_on_crash()
        print(ws)

    def on_error(self, ws, error):
        print(f"Error Faced {error}")

    def delete_indexes(self, *args, target_indexes: list):
        """
        Delete indexes in place
        args will contain lists needs to be processed
        target_indexes must contain integer values and must not exceel index of lists
        1. preprocess target_indexes
            a. iterate over all indexes > i is always currently iterating index
            b. replace value of currentindex by decrementing i
        2. Iterate over all data in target_index> data is always current data
        3. iterate over args and pop required indexes
        :param args: contains arguments as list, where each list is of same length
        :param target_indexes: contains a list of integer which will be deleted from the list as index
        :return: None
        """
        temp_index = []
        for i in range(0, len(target_indexes)):
            temp_index.append(target_indexes[i] - i)
        #
        for index in temp_index:
            for list_ref in args:
                list_ref.pop(index)

    def send_sms_if_command_satisfies(self, tick, price):
        """
        When user send a Command in "Symbol Price" format, we have to check if that symbol reaches that certain price.
        When reaches we have to send a SMS to the user and delete the command from the checking queue
        :param tick:
        :param price:
        :return:
        """
        if tick in self.incoming_ticker_queue:
            delete_index_list = []
            for i in range(len(self.incoming_command_queue)):

                if (self.incoming_command_queue[i]["ticker"] == tick) and (
                        self.incoming_command_queue[i]["target_price"] >= price):
                    print(f"{tick} current price {price}")
                    print(f"REQUIREMENT MEET: {self.incoming_command_queue[i]}")
                    msg_body = f"{tick} has reached {self.incoming_command_queue[i]['target_price']}!"
                    self.messenger.send_msg(to_id=self.incoming_command_queue[i]["from_id"], body=msg_body)
                    # Mark this index for deletion
                    delete_index_list.append(i)
            self.delete_indexes(self.incoming_ticker_queue, self.incoming_command_queue,
                                target_indexes=delete_index_list)

    def on_message_received(self, message):
        message = json.loads(message)
        for msg in message:
            msg_time_in_sec = int(msg['t'] / 1000)  # milli to sec
            if self.last_msg_time_sec[msg["sym"]] < msg_time_in_sec:  # Skip milli sec data
                if self.packup_at_utc_sec <= msg_time_in_sec:
                    # When incoming msg time reaches packup time, then proceed packup
                    self.packup_for_today()  # Requires improvement on handling already received data
                    break  # loses some data and continues the process
                else:
                    # Continue current day calculation
                    self.last_msg_time_sec[msg["sym"]] = msg_time_in_sec
                    if self.current_day_data[msg["sym"]]["init_price"] == 0:
                        # Set initial value
                        self.current_day_data[msg["sym"]]["init_price"] = msg["p"]
                    else:
                        # Check if the price of that ticker meets user SMS Command if so then send sms
                        self.send_sms_if_command_satisfies(msg["sym"], msg["p"])

                        curr_change = ((msg["p"] - self.current_day_data[msg["sym"]]["init_price"]) /
                                       self.current_day_data[msg["sym"]]["init_price"]) * 100

                        if curr_change < self.current_day_data[msg["sym"]]["min_change"]:
                            if self.current_day_data[msg["sym"]]["min_change"] != self.MIN_CHANGE_DEFAULT:
                                self.current_day_data[msg["sym"]]["min_change"] = curr_change
                                self.current_day_data[msg["sym"]]["volatility"] = self.current_day_data[msg["sym"]][
                                                                                      "max_change"] - \
                                                                                  self.current_day_data[msg["sym"]][
                                                                                      "min_change"]

                            else:
                                # Initially this code is called once per day per ticker to adjust
                                # the min, max conflict
                                self.current_day_data[msg["sym"]]["min_change"] = curr_change
                                self.current_day_data[msg["sym"]]["max_change"] = curr_change

                        elif curr_change > self.current_day_data[msg["sym"]]["max_change"]:
                            self.current_day_data[msg["sym"]]["max_change"] = curr_change
                            self.current_day_data[msg["sym"]]["volatility"] = self.current_day_data[msg["sym"]][
                                                                                  "max_change"] - \
                                                                              self.current_day_data[msg["sym"]][
                                                                                  "min_change"]
                        print(f"time: {msg_time_in_sec}")
                        # Check if change is 20% or above
                        if curr_change >= 20:
                            # Check if we already sent msg today
                            if msg["sym"] not in self.today_sms_data:
                                # Check if we sent msg this week
                                if msg["sym"] not in self.current_week_sms_data:
                                    # Last 7 days first time sms requirement meet
                                    if (self.prev_week_highest_volatility[
                                            msg["sym"]] != 0) and (self.current_day_data[msg["sym"]]["volatility"] >= (
                                            self.prev_week_highest_volatility[msg["sym"]] * 3)):

                                        if self.has_news(msg["sym"]):
                                            # 3 times volatility than prev week highest volatility and has news
                                            print(
                                                "LAST_7_DAYS_FIRST_TIME_AND_3_TIMES_VOLATILITY_THAN_PREVIOUS_WEEK_HIGHEST_VOLATILITY_HAS_NEWS")
                                            print(
                                                f"SEND SMS: $ **{msg['sym']}** $ has news, prev_volatility = {self.prev_week_highest_volatility[msg['sym']]} , curr_volatility = {self.current_day_data[msg['sym']]['volatility']}")
                                            self.messenger.send_msg(body=f"$ **{msg['sym']}** $")
                                            self.today_sms_data.append(msg['sym'])
                                            self.current_week_sms_data.append(msg['sym'])
                                        else:
                                            # 3 times volatility than prev week highest volatility and no news
                                            print(
                                                "LAST_7_DAYS_FIRST_TIME_AND_3_TIMES_VOLATILITY_THAN_PREVIOUS_WEEK_HIGHEST_VOLATILITY")
                                            print(
                                                f"SEND SMS: * {msg['sym']} * prev_volatility = {self.prev_week_highest_volatility[msg['sym']]} , curr_volatility = {self.current_day_data[msg['sym']]['volatility']}")
                                            self.messenger.send_msg(body=f"* {msg['sym']} *")
                                            self.today_sms_data.append(msg['sym'])
                                            self.current_week_sms_data.append(msg['sym'])
                                    else:

                                        if self.has_news(msg["sym"]):
                                            # Last 7 days first time and has news
                                            print("LAST_7_DAYS_FIRST_TIME_HAS_NEWS")
                                            print(f"SEND SMS: $ {msg['sym']} * $")
                                            self.messenger.send_msg(body=f"$ {msg['sym']} * $")
                                            self.today_sms_data.append(msg['sym'])
                                            self.current_week_sms_data.append(msg['sym'])
                                        else:
                                            # Last 7 days first time and no news
                                            print("LAST_7_DAYS_FIRST_TIME")
                                            print(f"SEND SMS: {msg['sym']} *")
                                            self.messenger.send_msg(body=f"{msg['sym']} *")
                                            self.today_sms_data.append(msg['sym'])
                                            self.current_week_sms_data.append(msg['sym'])
                                else:
                                    # More than one time in current week but today is first
                                    if self.has_news(msg["sym"]):
                                        # Last 7 days More than one time and has news
                                        print("LAST_7_MORE_THAN_ONE_TIME_HAS_NEWS")
                                        print(f"SEND SMS: $ {msg['sym']} $")
                                        self.messenger.send_msg(body=f"$ {msg['sym']} $")
                                        self.today_sms_data.append(msg['sym'])
                                        self.current_week_sms_data.append(msg['sym'])
                                    else:
                                        # Last 7 days more than one time
                                        print("LAST_7_MORE_THAN_ONE_TIME")
                                        print(f"SEND SMS: {msg['sym']}")
                                        self.messenger.send_msg(body=f"{msg['sym']}")
                                        self.today_sms_data.append(msg['sym'])
                                        self.current_week_sms_data.append(msg['sym'])
                            else:
                                # Today we already sent a msg to the user
                                print(f"Today we already sent a message for {msg['sym']}. So skip it")
