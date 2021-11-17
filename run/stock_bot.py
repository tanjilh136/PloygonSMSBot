import time
from datetime import datetime
from message_handler.messenger import Messenger
from telegram_bot.telegram_bot import TelegramBot
from polygon_data.polygon_data_stream import PolygonRealTimeTradeData
from custom_time.custom_time import CustomTimeZone

KARIM_TELEGRAM_ID = 
MAMUN_TELEGRAM_ID = 
allowed_telegram_user_id_list = [MAMUN_TELEGRAM_ID]
growth_status_receiver_user_id = [MAMUN_TELEGRAM_ID]
TELEGRAM_TOKEN = ""


class StockBot:
    def __init__(self):
        self.received_command_queue = []
        self.received_ticker_queue = []

    def start_now(self, day_completes_at_utc):
        """
        Starts the bot immediately

        :return:
        """
        sid = ""
        auth = ""
        number = ""

        messenger = Messenger(growth_status_receiver_user_id=growth_status_receiver_user_id)
        bot = TelegramBot(msg_receiver=messenger.received_msg, token=TELEGRAM_TOKEN,
                          allowed_user_id=allowed_telegram_user_id_list)
        messenger.connect_msg_sender(msg_sender=bot.send_msg)
        bot.start()

        pol = PolygonRealTimeTradeData(messenger=messenger,
                                       tickers="T.*",
                                       packup_at_utc_tomorrow=day_completes_at_utc, backup_mode=True,
                                       backup_interval_min=5, recover_now=True)
        pol.start_fetching()

    def start(self, start_time_iso_utc="now", start_date_iso_utc="", count_24_hr_at_iso_utc_time="19:59:59"):
        """
        ALL OF THE TIME AND DATES MUST BE OF UTC Time zone
        Starts the bot at "start_time_iso" in a new thread

        :param start_time_iso_utc: "now" or "hr:m:s" 24hr format. If "now", then start_date_iso is skipped
                               ""
        :param start_date_iso_utc: "" or "Y-m-d" the date of the start_time_iso
        :param count_24_hr_at_iso_utc_time: A day completes at this specific time. Considered as valid time
        :return:
        """

        self.day_end_at = count_24_hr_at_iso_utc_time.strip()

        if start_time_iso_utc.lower() == "now":
            self.start_now(self.day_end_at)
            # Python signal module only works in main thread. So we cant start data fetching
            # from polygon from another thread
            # t1 = Thread(target=self.start_now, args=[self.day_end_at])
            # t1.start()
        else:
            time_date = CustomTimeZone()  # Time zone is America/New_York by default
            curr_time, curr_date = time_date.get_current_utc_iso_time_date_tuple()
            print(curr_time, curr_date)
            curr_year, curr_month, curr_day = curr_date.strip().split("-")
            curr_hr, curr_min, curr_sec = curr_time.strip().split(":")
            print(curr_year, curr_month, curr_day)
            print(curr_hr, curr_min, curr_sec)

            start_year, start_month, start_day = start_date_iso_utc.strip().split("-")
            start_hr, start_min, start_sec = start_time_iso_utc.strip().split(":")

            start_at = datetime(year=int(start_year), month=int(start_month), day=int(start_day), hour=int(start_hr),
                                minute=int(start_min), second=int(start_sec))
            curr_time_date = datetime(year=int(curr_year), month=int(curr_month), day=int(curr_day), hour=int(curr_hr),
                                      minute=int(curr_min), second=int(curr_sec))

            remaining_seconds = (start_at - curr_time_date).total_seconds()
            print(f"Bot will start at UTC time: {start_time_iso_utc}  {start_date_iso_utc}")
            print(f"A day will be completed at UTC time : {count_24_hr_at_iso_utc_time}")
            print(f"Sleeping for {remaining_seconds} seconds")
            time.sleep(remaining_seconds)
            self.start_now(self.day_end_at)
            # t1 = Thread(target=self.start_now, args=[self.day_end_at])
            # t1.start()

    def stop(self):
        """
        Stops the bot

        :return:
        """
