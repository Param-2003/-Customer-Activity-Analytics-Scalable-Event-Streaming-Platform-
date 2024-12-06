import json
import random
import time
import logging
from datetime import datetime

from confluent_kafka import Producer
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='producer.log'
)
logger = logging.getLogger(__name__)

class UserActivityProducer:
    """
    A sophisticated Kafka producer for generating synthetic user activity events.
    
    Design Principles:
    - Generate realistic, varied user interaction events
    - Ensure data quality and randomness
    - Provide robust error handling and logging
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='user_activity'):
        """
        Initialize the Kafka producer with configuration.
        
        :param bootstrap_servers: Kafka broker address
        :param topic: Kafka topic to produce events to
        """
        self.fake = Faker()
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'user-activity-producer'
        }
        self.producer = Producer(self.producer_config)
        self.topic = topic
        
        # Event type probabilities (realistic distribution)
        self.event_types = {
            'page_view': 0.6,    # Most common
            'add_to_cart': 0.3,  # Less frequent
            'purchase': 0.1       # Least frequent
        }
    
    def generate_event(self):
        """
        Generate a synthetic user activity event with realistic characteristics.
        
        :return: Dictionary representing a user activity event
        """
        event_type = self._weighted_choice(self.event_types)
        
        event = {
            'user_id': self.fake.uuid4(),
            'session_id': self.fake.uuid4(),
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'product_id': self.fake.random_int(min=1, max=100),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'platform': random.choice(['web', 'ios', 'android']),
            'additional_metadata': {
                'page_url': self.fake.url() if event_type == 'page_view' else None,
                'product_category': self.fake.word() if event_type in ['add_to_cart', 'purchase'] else None
            }
        }
        
        return event
    
    def _weighted_choice(self, choices):
        """
        Select an event type based on predefined probabilities.
        
        :param choices: Dictionary of event types and their probabilities
        :return: Selected event type
        """
        r = random.random()
        cumulative_probability = 0.0
        for event, probability in choices.items():
            cumulative_probability += probability
            if r <= cumulative_probability:
                return event
    
    def delivery_report(self, err, msg):
        """
        Kafka message delivery callback for tracking message status.
        
        :param err: Error object (None if successful)
        :param msg: Kafka message
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def produce_events(self, events_per_second=1, duration=None):
        """
        Continuously produce user activity events to Kafka.
        
        :param events_per_second: Number of events to generate per second
        :param duration: Total runtime in seconds (None for infinite)
        """
        start_time = time.time()
        
        try:
            while duration is None or time.time() - start_time < duration:
                event = self.generate_event()
                
                try:
                    # Produce message with async delivery
                    self.producer.produce(
                        self.topic, 
                        value=json.dumps(event).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)  # Trigger message delivery
                    
                    logger.info(f"Produced event: {event}")
                    
                except Exception as e:
                    logger.error(f"Error producing event: {e}")
                
                time.sleep(1 / events_per_second)
        
        except KeyboardInterrupt:
            logger.info("Producer stopped by user.")
        
        finally:
            # Ensure all messages are delivered
            self.producer.flush()
            logger.info("Kafka producer shutdown complete.")

def main():
    """
    Main function to initialize and run the user activity producer.
    """
    producer = UserActivityProducer()
    producer.produce_events(events_per_second=2, duration=3600)  # 2 events/sec for 1 hour

if __name__ == '__main__':
    main()
