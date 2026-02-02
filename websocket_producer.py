import json
import time
import websocket
from kafka import KafkaProducer
import threading

KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
KAFKA_TOPIC = 'crypto_prices'
SOCKET_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade/bnbusdt@trade"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    msg_count = 0

    def on_message(ws, message):
        nonlocal msg_count
        data = json.loads(message)
        payload = data['data']
        
        symbol = payload['s'].replace('USDT', '')
        price = float(payload['p'])
        event_time = payload['T'] / 1000.0
        
        kafka_msg = {
            'source': 'BinanceWebSocket',
            'symbol': symbol,
            'price': price,
            'timestamp': event_time
        }
        
        producer.send(KAFKA_TOPIC, value=kafka_msg)
        
        msg_count += 1
        if msg_count % 50 == 0:  # heartbeat cada 50 msgs
            print(f"⚡ [x50] Último dato: {symbol} a ${price} (Total procesados: {msg_count})")

    def on_error(ws, error):
        print(f"Error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("### Conexión Cerrada ###")

    def on_open(ws):
        print("### Conectado a Binance WebSocket ###")

    ws = websocket.WebSocketApp(SOCKET_URL,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    print(f"🚀 Iniciando escucha en tiempo real para: BTC, ETH, SOL, BNB")
    ws.run_forever()

if __name__ == "__main__":
    main()