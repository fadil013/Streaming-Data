import json
from collections import Counter
from itertools import combinations
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Initialize MongoDB client and connect to the database
client = MongoClient('localhost', 27017)
db = client['DB-con1']
collection = db['consumer1_collection']

def apriori_frequent_itemsets(data, min_support=2):
    # Count the occurrences of each product ID
    item_counts = Counter(data)

    frequent_itemsets = []
    for itemset in combinations(item_counts.items(), 2):
        # Check if the combined count of the items meets the minimum support
        combined_count = sum(count for _, count in itemset)
        if combined_count >= min_support:
            frequent_itemsets.append(itemset)

    return frequent_itemsets

def process_message(msg):
    # Process the message and extract relevant data
    try:
        data = json.loads(msg.value().decode('utf-8'))
        also_buy_data = data.get('also_buy', [])  # Extract 'also_buy' data or use an empty list if not present
        frequent_itemsets = apriori_frequent_itemsets(also_buy_data)
        print(f"Apriori frequent itemsets: {frequent_itemsets}")
        
        # Store frequent itemsets in MongoDB
        collection.insert_one({'frequent_itemsets': frequent_itemsets})

    except Exception as e:
        print(f"Error processing message: {e}")

def consume_data(topic):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer_group_2',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Process the message
            process_message(msg)

    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_topic = 'topic1'  # Replace 'topic2' with your actual Kafka topic name for Consumer 2
    consume_data(kafka_topic)

