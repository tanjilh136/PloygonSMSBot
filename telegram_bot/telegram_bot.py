from threading import Thread
import telebot


class TelegramBot:
    def __init__(self, msg_receiver, token, allowed_user_id: list = "*"):
        self.bot = telebot.TeleBot(token=token, parse_mode=None)
        self.allowed_user_id = allowed_user_id
        self.msg_receiver = msg_receiver
        self.add_msg_handler()

    def send_msg(self, chat_id, body):
        self.bot.send_message(chat_id=chat_id, text=body)

    def start(self):
        t1 = Thread(target=self.bot.polling)
        t1.start()

    def add_msg_handler(self):
        """
        I failed to use @bot.message_handler decorator from this class.
        This function uses protected property of Telegram API library and uses its internal mechanism to add a handler
        :return:
        """
        handler = self.bot._build_handler_dict(handler=self.msg_received, content_types=["text"],
                                               commands=None,
                                               regexp=None,
                                               func=None)
        self.bot.add_message_handler(handler)

    def msg_received(self, message):
        print(f"RECEIVED FROM : {message.from_user.id}")
        if self.is_allowed_user(user_id=message.from_user.id):
            self.msg_receiver(from_id=message.chat.id, body=message.text)

    def is_allowed_user(self, user_id):
        """
        Checks if the user ID is allowed to use the STOCK BOT
        :param user_id:
        :return:
        """
        if self.allowed_user_id == "*":
            return True
        elif user_id in self.allowed_user_id:
            return True
        else:
            return False
