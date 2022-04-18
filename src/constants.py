import os
from dotenv import load_dotenv

load_dotenv()

# Constants for config
CONFIG_KAFKA = 'Kafka'
CONFIG_KAFKA_ENV_LOCAL = 'Local'
CONFIG_KAFKA_ENV_DOCKER = 'Docker'
CONFIG_KAFKA_HOST = 'Host'
CONFIG_KAFKA_PORT = 'Port'
CONFIG_GENERAL = 'General'
CONFIG_GENERAL_MAX_WORKERS = 'MaxWorkers'
CONFIG_RSS_HEADER = 'RssHeader'

REDDIT_CLIENT_ID = os.environ['REDDIT_CLIENT_ID']
REDDIT_CLIENT_SECRET = os.environ['REDDIT_CLIENT_SECRET']
TWITTER_CONSUMER_KEY = os.environ['TWITTER_CONSUMER_KEY']
TWITTER_CONSUMER_SECRET = os.environ['TWITTER_CONSUMER_SECRET']
TWITTER_BEARER_TOKEN = os.environ['TWITTER_BEARER_TOKEN']

# Constants for argument parser
DATA_SOURCE_RSS = 'rss'
DATA_SOURCE_REDDIT = 'reddit'
DATA_SOURCE_TWITTER = 'twitter'

# Constants for producer
PRODUCER_API_VERSION = (0, 10)

# Constants for data collector classes
TIMEOUT_RSS_REQUEST = 5
FEED_ENTRY_REGEX = r'<a class="ext" .*?>.*?</a>'
FEED_URL_REGEX = r'href="(http.+?)"'
