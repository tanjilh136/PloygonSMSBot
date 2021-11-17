from polygon import WebSocketClient, STOCKS_CLUSTER,CRYPTO_CLUSTER,FOREX_CLUSTER

key = ''  # Polygon Key
import time


def on_message_received(msg):
    print(msg)
    time.sleep(1)


my_client = WebSocketClient(cluster=STOCKS_CLUSTER, auth_key=key,
                            process_message=on_message_received)

my_client.run_async()
my_client.subscribe("T.*")
