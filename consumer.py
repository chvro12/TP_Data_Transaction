from confluent_kafka import Consumer, KafkaError
import json
import datetime as dt
from pytz import timezone

USD_TO_EUR_RATE = 0.92

LOCAL_TIMEZONE = timezone('Europe/Paris')

config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

consumer = Consumer(config)
consumer.subscribe(['transaction'])

def convert_usd_to_eur(amount, currency):
    if currency == "USD":
        return round(amount * USD_TO_EUR_RATE, 2)
    return amount

def convert_string_to_date(date_string):
    try:
        return dt.datetime.fromisoformat(date_string).astimezone(LOCAL_TIMEZONE).isoformat()
    except ValueError:
        return "Invalid date format"

try:
    while True:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue  
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition reached {message.topic()}[{message.partition()}] at offset {message.offset()}')
            else:
                print(f'Error: {message.error()}')
        else:
            msg_data = json.loads(message.value().decode('utf-8'))
            if msg_data['moyen_paiement'] != 'erreur' and msg_data['utilisateur']['adresse'] is not None:
                original_amount = msg_data['montant']
                original_currency = msg_data['devise']
                msg_data['montant'] = convert_usd_to_eur(msg_data['montant'], msg_data['devise'])
                msg_data['date'] = convert_string_to_date(msg_data['date'])
                print(f"Original Amount: {original_amount} {original_currency}, Converted Amount: {msg_data['montant']} EUR")
                print(f"Received message: {msg_data}")
finally:
    consumer.close()
