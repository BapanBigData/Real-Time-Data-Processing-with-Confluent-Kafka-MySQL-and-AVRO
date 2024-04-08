import random
import time
import mysql.connector
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the credentials
mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")


# Function to insert a single record into MySQL table
def insert_record_into_mysql(record):
    connection = None
    
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )

        cursor = connection.cursor()

        query = "INSERT INTO products01 (id, name, category, price, last_updated) VALUES (%s, %s, %s, %s, %s)"
        val = (record['id'], record['name'], record['category'], record['price'], record['last_updated'])
        cursor.execute(query, val)
        connection.commit()
        
        print(f"Product with id {record['id']} inserted successfully!")

    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table:", error)

    finally:
        if connection is not None and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")
            

# Function to generate mock data for the given schema
def generate_mock_data(num_records):
    
    config_data = {}

    try:
        with open('product_config.json') as f:
            config_data = json.load(f)
            product_id = config_data.get('product_id')
    except FileNotFoundError:
        pass

    if product_id is None:
        product_id = 1
        
    for _ in range(num_records):
        
        record = {
            'id': product_id,
            'name': random.choice(['Laptop', 'Phone', 'Tablet', 'Headphones', 'Charger', 'Books', 'Sunglasses', 'Backpack', 'Desk Lamp', 'Running Shoes', 'Gaming Console']),
            'category': random.choice(['Category A', 'Category B', 'Category C', 'Category D', 'Category E']),
            'price': round(random.uniform(1.0, 1000.0), 2),
            'last_updated': int(time.time())  # Current Unix timestamp
        }
        
        
        # Insert the record into MySQL table
        insert_record_into_mysql(record)
        
        product_id += 1
        
        time.sleep(random.choice([1, 2, 3, 4, 5, 6, 8, 7, 9, 10]))
    
    # Update product id
    config_data['product_id'] = product_id 

    with open('product_config.json', 'w') as file:
        json.dump(config_data, file)
        
    return 


if __name__ == "__main__":
    # Generate mock data for 10 records
    generate_mock_data(120)