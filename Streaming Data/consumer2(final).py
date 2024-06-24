from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import Counter
from itertools import combinations
import json

# Initialize Kafka consumer
consumer = KafkaConsumer('topic1',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         group_id='group2')

# Initialize PCY parameters
bucket_size = 1000  # Adjust as needed
min_support = 5  # Adjust as needed

# Initialize MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['DB-Con2']  # Replace 'your_database' with your database name
collection = db['frequent_itemsets']  # Replace 'frequent_itemsets' with your collection name

# Initialize PCY data structures
buckets = [Counter() for _ in range(bucket_size)]
frequent_itemsets = Counter()

# Process messages
for message in consumer:
    data = json.loads(message.value)
    items = data.get('also_buy', [])  # Extract 'also_buy' data or use an empty list if not present
    
    # Increment item counts in buckets
    for pair in combinations(items, 2):
        bucket_index = hash(pair) % bucket_size
        buckets[bucket_index][pair] += 1
    
    # Update frequent itemsets based on buckets
    for bucket in buckets:
        frequent_itemsets.update({itemset: count for itemset, count in bucket.items() if count >= min_support})
    
    # Store frequent itemsets in MongoDB
    for itemset, support in frequent_itemsets.items():
        # Use a unique identifier for each itemset, for example, a concatenated string of items
        item_id = '_'.join(sorted(itemset))
        # Upsert the itemset document in the collection
        collection.update_one({'_id': item_id}, {'$inc': {'count': 1}}, upsert=True)
    
    # Show real-time insights and associations
    print("Real-time insights and associations:")
    for itemset, support in frequent_itemsets.items():
        print(f"Itemset: {itemset}, Support: {support}")

# Close the consumer
consumer.close()
