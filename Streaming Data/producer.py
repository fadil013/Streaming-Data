import json
from confluent_kafka import Producer

def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} partition {msg.partition()}")

def produce_data(file_path, topic):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    with open(file_path, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                break  # End of file here

            try:
                data = json.loads(line.strip())  # loading data line by line to avoid buffer issues
                # Produce each JSON item as a message to Kafka topic
                producer.produce(topic, value=json.dumps(data), callback=delivery_callback)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

            # Check if the producer queue is full
            if producer.flush(0.1) > 0:  # Flush every 0.1 seconds
                print("Producer queue full, flushing messages")

    # Wait for all messages to be delivered
    producer.flush()

if __name__ == "__main__":
    file_path = r'C:\Users\abdul\OneDrive\Desktop\cleaned4.json'
    kafka_topic = 'topic1'
    produce_data(file_path, kafka_topic)
