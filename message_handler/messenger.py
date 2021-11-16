import pandas as pd

SHOW_PENDING_COMMANDS = "show pending commands"
PRICE_OF_SYMBOL = "price of"
allowed_commands = [
    SHOW_PENDING_COMMANDS,
    PRICE_OF_SYMBOL
]


class Messenger:
    def __init__(self, growth_status_receiver_user_id: list, interactive_mode=True):
        self.all_symbols = self.get_all_symbol_names_from_file()
        self.received_command_queue = []
        self.received_ticker_queue = []
        self.interactive_mode = interactive_mode
        self.msg_sender = None
        self.growth_status_receiver_user_id = growth_status_receiver_user_id

    def addpadding(self, text, pad_char=" ", maximum=7):
        req = maximum - len(text)
        if req > 0:
            return text + (pad_char * req)
        else:
            return text

    def get_pending_commands(self):
        res = "Pending Commands\n"
        for command in self.received_command_queue:
            symbol_data = f"{command['ticker']}"
            price_data = f"{command['target_price']}"

            res = res + self.addpadding(symbol_data) + "  ::::  " + price_data + "\n"
        return res

    def get_all_symbol_names_from_file(self):
        """
        Initialize all ticker symbol names from the csv file into all_symbols as list
        :return:
        """
        df = pd.read_csv("../app_data/company_tickers.csv")
        return df["symbol"].to_numpy()

    def received_msg(self, from_id=0, body=""):
        """
        This function is called when we receive a message from allowed id

        :param from_id:
        :param body:
        :return:
        """
        print(from_id)
        print(body)
        msg = body.strip().split(" ")
        if len(msg) == 2:
            try:
                ticker = msg[0].upper()
                if ticker in self.all_symbols:
                    target_price = float(msg[1])
                    self.received_command_queue.append(
                        {"from_id": from_id, "ticker": ticker, "target_price": target_price})
                    self.received_ticker_queue.append(ticker)
                    print("NEW: " + str({"from_id": from_id, "ticker": ticker, "target_price": target_price}))
                    if self.interactive_mode:
                        self.send_msg(to_id=from_id, body="COMMAND RECEIVED")
                else:
                    print("Wrong ticker name received")
                    if self.interactive_mode:
                        self.send_msg(to_id=from_id, body="TICKER NAME IS NOT VALID")
            except:
                print("PRICE MUST BE A NUMBER")
                if self.interactive_mode:
                    self.send_msg(to_id=from_id, body="PRICE MUST BE A NUMBER")
        elif body.lower() in allowed_commands:
            command = body.lower()
            if command == SHOW_PENDING_COMMANDS:
                body = self.get_pending_commands()
                print(f"SENDING PENDING COMMANDS\n{body}")
                self.send_msg(to_id=from_id, body=body)
        elif body.lower().startswith(PRICE_OF_SYMBOL):
            try:
                _, _, symbol = body.lower().split()
                if symbol.upper() in self.all_symbols:
                    # Fetch current price of that symbol and send it
                    pass

            except Exception as e:
                self.send_msg(to_id=from_id, body="Wrong command")



        else:
            print("MESSAGE FORMAT ERROR")
            if self.interactive_mode:
                self.send_msg(to_id=from_id, body="MESSAGE FORMAT ERROR")

    def send_msg(self, to_id=None, body=""):
        if to_id is None:
            for user_id in self.growth_status_receiver_user_id:
                print(f"Sending msg to {user_id} with body = {body}")
                self.msg_sender(chat_id=user_id, body=body)
        else:
            print(f"Sending msg to {to_id} with body = {body}")
            self.msg_sender(chat_id=to_id, body=body)

    def connect_msg_sender(self, msg_sender=None):
        """
        Must connect a sms sender before calling send_msg()
        :param msg_sender:
        :return:
        """
        self.msg_sender = msg_sender
