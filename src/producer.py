from kafka import KafkaProducer
import logging
import uuid
import json
from constants import PRODUCER_API_VERSION
from helper import split_rss_feed


class Producer(object):
    """Wrapper for a Kafka Producer
    :param host: hostname of the bootstrap server
    :type host: str
    :param port: Port of the bootstrap server
    :type port: str
    """

    def __init__(self, host, port):
        """Constructor
        """
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=[f'{host}:{port}'], api_version=PRODUCER_API_VERSION)
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))

    def publish(self, topic, message):
        """Publish a message in the Kafka Broker

        :param topic: topic where the message will be published
        :type topic: str
        :param message: the value of the message
        :type message: str
        :raises KafkaTimeoutError: timeout when sending message or flushing the buffer
        """
        if ("rss" in topic):
            self.__publish_rss(topic, message)
        elif ("twitter" in topic):
            self.__publish_twitter(topic, message)
        else:
            key = bytes(str(uuid.uuid4()), encoding='utf-8')
            value_bytes = bytes(message, encoding='utf-8')
            logging.info(f'Publish on topic {topic}: {key}')
            self._producer.send(topic, key=key, value=value_bytes)
        self._producer.flush()

    def __publish_rss(self, topic, raw_feed):
        """Publish rss articles in the Kafka Broker

        :param topic: topic where the message will be published
        :type topic: str
        :param raw_feed: RSS-Feed in XML-Format
        :type raw_feed: str
        :raises KafkaTimeoutError: timeout when sending message or flushing the buffer
        """
        messages = split_rss_feed(raw_feed)
        for message in messages:
            key = bytes(str(uuid.uuid4()), encoding='utf-8')
            value_bytes = bytes(json.dumps(message), encoding='utf-8')
            logging.info(f'Publish on topic {topic}: {key}')
            self._producer.send(topic, key=key, value=value_bytes)

    def __publish_twitter(self, topic, tweets):
        messages = tweets
        for message in messages:
            key = bytes(str(uuid.uuid4()), encoding='utf-8')
            value_bytes = bytes(json.dumps(message), encoding='utf-8')
            logging.info(f'Publish on topic {topic}: {key}')
            self._producer.send(topic, key=key, value=value_bytes)
