import logging
from kafka import KafkaConsumer
import json

# Set up logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_messages():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        'users_created',  # Topic name
        group_id='my_consumer_group',  # Consumer group ID
        auto_offset_reset='earliest',  # Start from the earliest message if no offset is stored
        enable_auto_commit=True,  # Automatically commit offsets
        bootstrap_servers=['localhost:9092'],  # Kafka broker address
        consumer_timeout_ms=10000  # Timeout to avoid infinite waiting if no messages are available
    )

    logger.info("Consuming messages from Kafka topic 'users_created'...")

    try:
        # Continuously consume messages
        for message in consumer:
            user_data = json.loads(message.value.decode('utf-8'))
            logger.info(f"Received message: {user_data}")
            print(f"User Data: {user_data}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Close the consumer
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    consume_messages()
