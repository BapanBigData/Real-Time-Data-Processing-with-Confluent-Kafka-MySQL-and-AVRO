from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import mysql.connector
import json
import time
from dotenv import load_dotenv
import os



def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
        return

    print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    return


# Load environment variables from .env file
load_dotenv()

# Access the credentials
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")


# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': bootstrap_servers     # Adjust to your Kafka broker
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': schema_registry_url         # Adjust to your  Schema Registry URL
})

# Fetch the latest Avro schema for the value
subject_name = 'products_stream-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.serializer': key_serializer,      # Key will be serialized as a string
    'value.serializer': avro_serializer    # Value will be serialized as Avro
})



def connect_and_publish():
    
    connection = None
    
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        
        if connection.is_connected():
            print("Successfully connected to the Database!")
            cursor = connection.cursor()
            
            # Load the last read timestamp from the config file
            config_data = {}

            try:
                with open('config.json') as f:
                    config_data = json.load(f)
                    last_read_timestamp = config_data.get('last_read_timestamp')
            except FileNotFoundError:
                pass

            # Set a default value for last_read_timestamp
            if last_read_timestamp is None:
                last_read_timestamp = 0
            
            # Use the last_read_timestamp in the SQL query
            query = f"SELECT * FROM products01 WHERE last_updated > {last_read_timestamp};"
            
            # Execute the SQL query
            cursor.execute(query)
            
            # Check if there are any rows fetched
            rows = cursor.fetchall()
            
            if not rows:
                print("No rows to fetch.")
            else:
                for row in rows:
                    columns = [column[0] for column in cursor.description]
                    data = dict(zip(columns, row))
                    
                    # Produce to Kafka
                    producer.produce(topic='products_stream', key=str(data['id']), value=data, on_delivery=delivery_report)
                    producer.flush()
                    
                    time.sleep(1) ## wait for 1 sec 

                
                # Fetch any remaining rows to consume the result
                cursor.fetchall()

                query = "SELECT MAX(last_updated) as last_updated FROM products01;"
                cursor.execute(query)

                # Fetch the result
                result = cursor.fetchone()
                max_date = result[0]  

                # Update the value in the config.json file
                config_data['last_read_timestamp'] = max_date

                with open('config.json', 'w') as file:
                    json.dump(config_data, file)
                
                print("Data successfully published to Kafka Topic!!!")
                
        else:
            print("Failed to connect to the Database.")
            
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        
    finally:
        if connection is not None and connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection closed.")


if __name__ == "__main__":
    connect_and_publish()