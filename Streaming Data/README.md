*Data Cleaning:*
The data cleaning process is crucial for ensuring that the data is in a suitable format for analysis. It involves:
- Reading the JSON dataset in manageable chunks to avoid memory issues with large datasets.
- Utilizing regular expressions to remove HTML tags and URLs from text fields, ensuring that textual data is clean and devoid of unnecessary markup.
- Filtering the dataset to retain only relevant columns ('also_buy' and 'asin'), which are likely pertinent for subsequent analysis.
- Iterating through each chunk of data, applying the cleaning operations, and then writing the cleaned data to a new JSON file ('cleaned4.json') for further processing.

*Producer:*
The producer script is responsible for:
- Reading the cleaned JSON data from the 'cleaned4.json' file.
- Using the confluent_kafka library to interact with Kafka, specifically the Producer class for producing messages.
- Converting each line of cleaned JSON data into JSON format and publishing it as a message to the specified Kafka topic ('topic1').
- Employing a delivery callback function (delivery_callback) to handle acknowledgment of message delivery, ensuring reliability in message publishing.

*Consumer 1:*
Consumer 1 performs the following tasks:
- Subscribing to the Kafka topic ('topic1') to consume messages.
- Upon receiving a message, parsing the JSON data and storing it directly into a MongoDB collection ('consumer1_collection').
- Utilizing the pymongo library to interface with MongoDB, establishing a connection to the local MongoDB instance and inserting documents into the specified collection.
- Printing confirmation messages to indicate successful storage of data in MongoDB, providing transparency in the process.

*Consumer 2:*
Consumer 2 implements the PCY (Park-Chen-Yu) algorithm for frequent itemset mining:
- Subscribing to the Kafka topic to consume messages containing transactional data.
- Processing each message by extracting the 'also_buy' data, which represents items purchased together in each transaction.
- Employing the PCY algorithm to efficiently identify frequent itemsets from the transactional data.
- Storing the identified frequent itemsets in a MongoDB collection ('frequent_itemsets') for further analysis.
- Printing real-time insights and associations based on the mined frequent itemsets, enabling monitoring and understanding of patterns in the data.

*Consumer 3:*
Consumer 3 implements Online Sampling with the FP-stream Algorithm for real-time processing of streaming data:
- Subscribing to the Kafka topic to consume incoming messages.
- Sampling transactions from the streamed data and updating frequent itemsets using the FP-stream Algorithm.
- Storing the updated frequent itemsets in the same MongoDB collection ('frequent_itemsets') used by Consumer 2.
- Printing real-time insights and associations based on the incremental mining of frequent itemsets, allowing for continuous monitoring and analysis of the data stream.

In summary, each component plays a specific role in the data pipeline: data cleaning prepares the data for analysis, the producer publishes the cleaned data to Kafka, and the consumers process the data using different algorithms and techniques, storing the results in MongoDB for further exploration and analysis.
