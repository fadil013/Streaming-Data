import json
from collections import Counter, defaultdict
from itertools import combinations
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Initialize MongoDB client and connect to the database
client = MongoClient('localhost', 27017)
db = client['DB-con3']
collection = db['consumer3_collection']  # MongoDB collection to store frequent itemsets

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer_group_3',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['topic1'])  # Replace 'topic1' with your actual Kafka topic name

# Initialize Online Sampling with FP-stream Algorithm parameters
min_support = 15  # Adjust as needed
max_transactions = 1000  # Maximum number of transactions to keep for sampling
bucket_size = 1000  # Adjust as needed

# Initialize Online Sampling with FP-stream Algorithm data structures
sampled_transactions = []  # List to store sampled transactions
frequent_itemsets = Counter()
buckets = defaultdict(Counter)  # Using defaultdict to avoid key errors

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
    global sampled_transactions, frequent_itemsets, buckets
    
    try:
        data = json.loads(msg.value().decode('utf-8'))
        also_buy_data = data.get('also_buy', [])  # Extract 'also_buy' data or use an empty list if not present
        
        # Add the new transaction to the sampled transactions
        sampled_transactions.extend(also_buy_data)  # Use extend instead of append
        
        # If the maximum number of transactions is reached, update frequent itemsets using FP-stream Algorithm
        if len(sampled_transactions) >= max_transactions:
            # Increment item counts in buckets
            for pair in combinations(sampled_transactions, 2):
                bucket_index = hash(tuple(pair)) % bucket_size  # Convert the list to a tuple for hashing
                buckets[bucket_index][tuple(pair)] += 1  # Convert the list to a tuple for dictionary keys
            
            # Update frequent itemsets based on buckets
            frequent_itemsets = Counter({itemset: count for bucket in buckets.values() for itemset, count in bucket.items() if count >= min_support})
            
            # Store frequent itemsets in MongoDB
            for itemset, support in frequent_itemsets.items():
                frequent_itemset_data = {
                    'itemset': list(itemset),  # Convert tuple to list for MongoDB
                    'support': support
                }
                collection.insert_one(frequent_itemset_data)
            
            # Clear the sampled transactions for the next batch
            sampled_transactions = []
            
            # Print real-time insights and associations
            print("Online Sampling with Incremental Mining - Real-time insights and associations:")
            for itemset, support in frequent_itemsets.items():
                print(f"Itemset: {itemset}, Support: {support}")

    except Exception as e:
        print(f"Error processing message: {e}")

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

