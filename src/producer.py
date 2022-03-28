from kafka import KafkaProducer
import logging
import uuid

from constants import PRODUCER_API_VERSION

class Producer(object):
    """Wrapper for a Kafka Producer
    :param host: Hostname of the bootstrap server
    :type host: str
    :param port: Port of the bootstrap server
    :type port: str
    """
    def __init__(self, host, port):
        """Constructor
        """
        try:
            self.producer = KafkaProducer(bootstrap_servers=[f'{host}:{port}'], api_version=PRODUCER_API_VERSION)
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))
    
    def publish(self, topic, message):
        """
        Publish a message in the Kafka Broker

        :param topic: topic where the message will be published
        :type topic: str
        :param message: The value of the message
        :type message: str
        :raises KafkaTimeoutError: timeout when sending message or flushing the buffer
        """
        # TODO: splitting
        key = bytes(str(uuid.uuid4()), encoding='utf-8')
        value_bytes = bytes(message, encoding='utf-8')
        logging.info(f'Publish on topic {topic}: {key}')
        self.producer.send(topic, key=key, value=value_bytes)
        self.producer.flush()
