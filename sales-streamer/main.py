import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from faker import Faker

faker = Faker()


def generate_sales_transactions():
    user = faker.simple_profile()

    product_data = random.choice([
        {
            "productName": "Smartphone",
            "productCategory": "Electronics",
            "productBrand": random.choice(['Apple', 'Samsung', 'Huawei', 'Xiaomi'])
        },
        {
            "productName": "Sneakers",
            "productCategory": "Fashion",
            "productBrand": random.choice(['Nike', 'Adidas', 'Converse', 'New Balance'])
        },
        {
            "productName": "Fitness Tracker",
            "productCategory": "Health",
            "productBrand": random.choice(['Fitbit', 'Garmin', 'Apple', 'Xiaomi'])
        },
        {
            "productName": "Speaker",
            "productCategory": "Home",
            "productBrand": random.choice(['Bose', 'Sony', 'JBL', 'Sonos'])
        },
        {
            "productName": "Skincare",
            "productCategory": "Beauty",
            "productBrand": random.choice(['Nivea', 'Olay', 'Neutrogena', 'CeraVe'])
        },
        {
            "productName": "Drone",
            "productCategory": "Toys",
            "productBrand": random.choice(['DJI', 'Parrot', 'Yuneec', 'Skydio'])
        },
    ])

    return {
        "customerId": user['username'],
        "transactionId": faker.uuid4(),
        "transactionDate": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": product_data['productName'],
        "productCategory": product_data['productCategory'],
        "productPrice": round(random.uniform(10, 1000), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": product_data['productBrand'],
        "currency": random.choice(['USD', 'INR']),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
    }


def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition()}]')


def main():
    topic = 'sales_transactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
    })

    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 120:
        try:
            producer.poll(0)

            transaction = generate_sales_transactions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']

            print(transaction)

            producer.produce(topic, key=transaction['transactionId'], value=json.dumps(transaction),
                             on_delivery=delivery_report)

            time.sleep(5)

        except BufferError:
            print('BufferError waiting for message..')
            time.sleep(1)

        except Exception as e:
            print(e)

    producer.flush()


if __name__ == '__main__':
    main()
