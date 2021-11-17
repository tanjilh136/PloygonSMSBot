from twilio.rest import Client
import time
from threading import Thread


def sms_send_requested_notify(sid="", to="", body=""):
    """
    auto called when sms sending request is queued in twilio_sms. It calls if sending fails

    :param sid:
    :param to:
    :param body:
    :return:
    """
    pass


def sms_received_notify(sid="", from_number="", body=""):
    """
    auto called when a new sms is received [from only allowed number]

    :param sid:
    :param from_number: the number from which we received the message
    :param body: body of the message
    :return:
    """
    pass


class SmsSender:
    def __init__(self, twilio_sid, twilio_auth_token, owned_phone_number):
        """

        :param twilio_sid:
        :param twilio_auth_token:
        :param owned_phone_number: must have to be registered in twilio_sms. It will be used for inbound/outbound sms
        """
        self.owned_phone_number = owned_phone_number
        self.client = Client(twilio_sid, twilio_auth_token)

    def send_message(self, to="+14083934260", body=""):
        """
        Request twilio_sms to send message to a specific number. This function doesnt guaranty that the sms will be
        delivered {Twilio restricts outgoing/incoming sms based on a/c. The receiver phone number must be valid. and
        the a/c balance must be full}

        :param to: receivers phone number
        :param body: text to send in that phone number
        :return:
        """
        try:
            message = self.client.messages.create(
                body=body,
                from_=self.owned_phone_number,
                to=to
            )
            print(f"TO: {to}, BODY: {body}")
            print("Message sending request queued")
        except Exception as e:
            print(e)


class SmsHandler:
    def __init__(self, twilio_sid, twilio_auth_token, owned_phone_number, allowed_sender_phone="*"):
        """

        :param twilio_sid:
        :param twilio_auth_token:
        :param owned_phone_number: must have to be registered in twilio_sms. It will be used for inbound/outbound sms
        :param allowed_sender_phone: if "*" then receive notification of all inbound sms, if specific, then notification of inbound
                                     sms will be filtered
        """
        self.owned_phone_number = owned_phone_number
        self.allowed_sender_phone = allowed_sender_phone
        self.client = Client(twilio_sid, twilio_auth_token)

    def get_balance(self):
        return self.client.balance.fetch().balance

    def wait_for_inbound_sms(self, on_received_sms_listener=sms_received_notify, refresh_after=2):
        """
        Creates a new thread to receive sms. A infinite loop starts running

        :param on_received_sms_listener: callable function which will be called when a sms is received
        :param refresh_after: check for inbound message every "refresh_after" seconds
        :return:
        """
        waiting_thread = Thread(target=self.waiting_for_inbound_sms, args=[on_received_sms_listener, refresh_after])
        waiting_thread.start()

    def waiting_for_inbound_sms(self, on_received_sms_listener, refresh_after):

        while True:
            time.sleep(refresh_after)
            if self.allowed_sender_phone != "*":
                sid = self.client.messages.list(from_=self.allowed_sender_phone)
            else:
                sid = self.client.messages.list()

            print("waiting for inbound sms")
            for sid_ in sid:
                print("SMS RECEIVED, passing the data to received function and deleting the actual sms")
                on_received_sms_listener(sid_.sid, sid_.from_, sid_.body)
                sid_.delete()  # Delete the message we just received

    def send_message(self, to, body, on_send_message_listener=sms_send_requested_notify):
        """
        Request twilio_sms to send message to a specific number. This function doesnt guaranty that the sms will be delivered
        {Twilio restricts outgoing/incoming sms based on a/c. The receiver phone number must be valid.}

        :param to: receivers phone number
        :param body: text to send in that phone number
        :param on_send_message_listener: calls this function after sending request to the twilio_sms with sid,to,body
        :return:
        """
        try:
            message = self.client.messages.create(
                body=body,
                from_=self.owned_phone_number,
                to=to
            )
            print("Message sending request queued")
            on_send_message_listener(message.sid, to, body)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    from twilio_sms.twilio_credentials import TwilioCreds

    twilio_creds = TwilioCreds().load_creds(account_name="")
    PHONE_NUMBER = ""

    sms_handler = SmsHandler(twilio_sid=twilio_creds.get_sid(),
                             twilio_auth_token=twilio_creds.get_auth_token(),
                             owned_phone_number=twilio_creds.get_registered_number(),
                             allowed_sender_phone=KARIMS_PHONE_NUMBER)
    sms_handler.wait_for_inbound_sms(refresh_after=2)
