import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
from requests.exceptions import RequestException

from constants import CONFIG_GENERAL, CONFIG_GENERAL_MAX_WORKERS, CONFIG_KAFKA, CONFIG_KAFKA_HOST, \
    CONFIG_KAFKA_PORT, CONFIG_RSS_HEADER, DATA_SOURCE_REDDIT, DATA_SOURCE_RSS, DATA_SOURCE_TWITTER, \
    CONFIG_REDDIT_CLIENT_ID, CONFIG_REDDIT_CLIENT_SECRET, CONFIG_TWITTER_CONSUMER_KEY, CONFIG_TWITTER_CONSUMER_SECRET, CONFIG_TWITTER_BEARER_TOKEN
from data_collectors import RedditDataCollector, RssDataCollector, TwitterDataCollector
from data_streamers import TwitterDataStreamer
from producer import Producer


def get_arguments():
    """Get script arguments from the argument parser
    """
    parser = argparse.ArgumentParser(
        description='Script for collecting data from different data sources and publishing it to a Kafka broker')
    parser.add_argument('--config', required=True,
                        help='Configuration file for the data collection script')
    subparsers = parser.add_subparsers(dest='data_source')
    rss_parser = subparsers.add_parser(
        'rss', help='Scrape data from RSS feeds')
    reddit_parser = subparsers.add_parser(
        'reddit', help='Scrape data from reddit')
    twitter_parser = subparsers.add_parser(
        'twitter', help='Scrape data from twitter')
    rss_parser.add_argument('--base_url', required=True,
                            help='URL of a RSS feed database where links to relevant RSS feeds can be found')
    twitter_parser.add_argument('--stream', action='store_true', default=False, dest='stream',
                                help='Additional flag for activating tweets streaming')
    return parser.parse_args()


def get_config(config_path):
    """Get config from config file

    :param config_path: Path to the config file
    :type config_path: str
    :return: key-value-pairs of the config fields
    :rtype: dict
    """
    with open(config_path, 'r') as config_file:
        return json.load(config_file)


def get_data_collector_instance(args, config):
    """Get the instance of data collector

    :param args: arguments of the script
    :type args: Namespace
    :raises NotImplementedError: no data collector implemented for given data source
    :return: instance of the specific data collector
    :rtype: subclass of BaseDataCollector
    """
    if args.data_source == DATA_SOURCE_RSS:
        return RssDataCollector(args.base_url, config[CONFIG_RSS_HEADER])
    elif args.data_source == DATA_SOURCE_REDDIT: 
        return RedditDataCollector(config["Reddit"][CONFIG_REDDIT_CLIENT_ID],
                                   config["Reddit"][CONFIG_REDDIT_CLIENT_SECRET])
    elif args.data_source == DATA_SOURCE_TWITTER:
        return TwitterDataCollector(config["Twitter"][CONFIG_TWITTER_CONSUMER_KEY],
                                    config["Twitter"][CONFIG_TWITTER_CONSUMER_SECRET],
                                    config["Twitter"][CONFIG_TWITTER_BEARER_TOKEN])
    else:
        raise NotImplementedError


def get_data_streamer_instance(args, config):
    """Get the instance of data streamer

    :param args: arguments of the script
    :type args: Namespace
    :raises NotImplementedError: no data streamer implemented for given data source
    :return: instance of the specific data streamer
    :rtype: subclass of BaseDataStreamer
    """
    if args.data_source == DATA_SOURCE_TWITTER:
        return TwitterDataStreamer(config["Twitter"][CONFIG_TWITTER_CONSUMER_KEY],
                                   config["Twitter"][CONFIG_TWITTER_CONSUMER_SECRET],
                                   config["Twitter"][CONFIG_TWITTER_BEARER_TOKEN])
    else:
        raise NotImplementedError


def main():
    args = get_arguments()
    config = get_config(args.config)
    kafka_host = config[CONFIG_KAFKA][CONFIG_KAFKA_HOST]
    kafka_port = config[CONFIG_KAFKA][CONFIG_KAFKA_PORT]
    producer = Producer(kafka_host, kafka_port)
    
    if args.stream:
        data_streamer = None
        try:
            data_streamer = get_data_streamer_instance(args, config)
        except NotImplementedError:
            logging.error(
                f'Data streaming not implemented for data source {args.data_source}')

        data_streamer.stream_into_producer(producer)
    
    else:
        data_collector = None
        try:
            data_collector = get_data_collector_instance(args, config)
        except NotImplementedError:
            logging.error(
                f'Data collection not implemented for data source {args.data_source}')

        max_workers = int(config[CONFIG_GENERAL][CONFIG_GENERAL_MAX_WORKERS])
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = data_collector.get_data_collection_futures(executor=executor)
            
            for future in as_completed(futures):
                try:
                    response = future.result()
                    if(isinstance(response, str)):
                        message = response
                    else:
                        message = future.result().text
                    producer.publish(args.data_source, message)
                except RequestException as e:
                    logging.warning(f'Error in GET-Request: {e}')
                    continue
                except Exception:
                    continue


if __name__ == '__main__':
    main()
