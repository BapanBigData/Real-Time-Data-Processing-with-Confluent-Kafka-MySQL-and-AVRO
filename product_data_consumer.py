import os
import json
import sys
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the credentials
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")


# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': bootstrap_servers,       # Adjust to your Kafka broker
    'group.id': 'productConsumerGroup',          # Adjust to your consumer group id
    'auto.offset.reset': 'latest'                # Adjust to your desired offset reset policy
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': schema_registry_url                  # Adjust to your Schema Registry URL
})

# Fetch the latest Avro schema for the value
subject_name = 'products_stream-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Define Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# Subscribe to the 'products_stream' topic
consumer.subscribe(['products_stream'])


def msg_poll(consumer_num):
    
    #Continually read messages from Kafka
    try:
        while True:
            msg = consumer.poll(1.0) # How many seconds to wait for message

            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue

            print(f'Successfully consumed record with key {msg.key()} and value {msg.value()}!')
            json_string = json.dumps(msg.value())
            
            ## define the file path, where consumer will store the data
            file_path = f"consumer_{consumer_num}.json"
            
            # Check if the file exists
            if not os.path.isfile(file_path):
                # Create the file and write the initial data
                with open(file_path, 'w') as file:
                    file.write(json_string + '\n')
            else:
                # Append the data to the existing file
                with open(file_path, 'a') as file:
                    file.write(json_string + '\n')
                    
            print("JSON string data is added to the JSON file!")
                
            file.close()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <consumer_num>")
        sys.exit(1)
    
    consumer_num = sys.argv[1]
    msg_poll(consumer_num)