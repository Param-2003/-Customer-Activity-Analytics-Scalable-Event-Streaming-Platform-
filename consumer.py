import json
import logging
import os
from datetime import datetime
from typing import List, Dict

import pandas as pd
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='consumer.log'
)
logger = logging.getLogger(__name__)

class UserActivityConsumer:
    """
    A robust Kafka consumer for processing user activity events.
    
    Design Principles:
    - Reliable event consumption
    - Flexible batch processing
    - Error handling and logging
    - Support for multiple data storage strategies
    """
    
    def __init__(
        self, 
        bootstrap_servers: str = 'localhost:9092', 
        topic: str = 'user_activity',
        group_id: str = 'user_activity_group'
    ):
        """
        Initialize Kafka consumer with configurable parameters.
        
        :param bootstrap_servers: Kafka broker address
        :param topic: Kafka topic to consume from
        :param group_id: Consumer group identifier
        """
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.topic = topic
        
        # Ensure data directories exist
        os.makedirs('data/raw', exist_ok=True)
        os.makedirs('data/processed', exist_ok=True)
    
    def create_consumer(self):
        """
        Create and configure Kafka consumer.
        
        :return: Configured Kafka consumer
        """
        self.consumer.subscribe([self.topic])
        return self.consumer
    
    def process_events(
        self, 
        batch_size: int = 10, 
        max_wait_time: float = 5.0,
        storage_strategy: str = 'csv'
    ):
        """
        Consume and process events from Kafka topic.
        
        :param batch_size: Number of events to process in a batch
        :param max_wait_time: Maximum time to wait for messages
        :param storage_strategy: Storage method (csv, parquet, json)
        """
        events_data: List[Dict] = []
        
        try:
            while True:
                message = self.consumer.poll(max_wait_time)
                
                if message is None:
                    if events_data:
                        self.save_events(events_data, storage_strategy)
                        events_data.clear()
                    continue
                
                if message.error():
                    self._handle_error(message)
                    continue
                
                try:
                    event = self._parse_message(message)
                    events_data.append(event)
                    
                    if len(events_data) >= batch_size:
                        self.save_events(events_data, storage_strategy)
                        events_data.clear()
                        
                    # Manual offset commit for better control
                    self.consumer.commit(message)
                    
                except Exception as e:
                    logger.error(f"Event processing error: {e}")
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        
        finally:
            if events_data:
                self.save_events(events_data, storage_strategy)
            self.consumer.close()
    
    def _parse_message(self, message):
        """
        Parse and validate Kafka message.
        
        :param message: Kafka message
        :return: Parsed event dictionary
        """
        try:
            event = json.loads(message.value().decode('utf-8'))
            # Optional: Add validation logic here
            return event
        except json.JSONDecodeError:
            logger.error("Invalid JSON in message")
            raise
    
    def _handle_error(self, message):
        """
        Handle Kafka consumer errors.
        
        :param message: Error message from Kafka
        """
        if message.error().code() == KafkaError._PARTITION_EOF:
            logger.warning("Reached end of partition")
        else:
            logger.error(f"Kafka error: {message.error()}")
    
    def save_events(
        self, 
        events_data: List[Dict], 
        storage_strategy: str = 'csv'
    ):
        """
        Save events using different storage strategies.
        
        :param events_data: List of event dictionaries
        :param storage_strategy: Storage method
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = pd.DataFrame(events_data)
        
        strategies = {
            'csv': self._save_csv,
            'parquet': self._save_parquet,
            'json': self._save_json
        }
        
        save_func = strategies.get(storage_strategy, self._save_csv)
        save_func(df, timestamp)
    
    def _save_csv(self, df: pd.DataFrame, timestamp: str):
        """Save events as CSV"""
        filename = f'data/processed/events_{timestamp}.csv'
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(df)} events to {filename}")
    
    def _save_parquet(self, df: pd.DataFrame, timestamp: str):
        """Save events as Parquet"""
        filename = f'data/processed/events_{timestamp}.parquet'
        df.to_parquet(filename)
        logger.info(f"Saved {len(df)} events to {filename}")
    
    def _save_json(self, df: pd.DataFrame, timestamp: str):
        """Save events as JSON"""
        filename = f'data/processed/events_{timestamp}.json'
        df.to_json(filename, orient='records')
        logger.info(f"Saved {len(df)} events to {filename}")

def main():
    """
    Main function to initialize and run the user activity consumer.
    """
    consumer = UserActivityConsumer()
    consumer.create_consumer()
    consumer.process_events(
        batch_size=10, 
        storage_strategy='csv'
    )

if __name__ == '__main__':
    main()
