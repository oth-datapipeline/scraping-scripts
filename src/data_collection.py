import argparse
from concurrent.futures import ThreadPoolExecutor
import configparser
from functools import partial
import logging
from requests.exceptions import RequestException

from constants import CONFIG_GENERAL, CONFIG_GENERAL_MAX_WORKERS, CONFIG_KAFKA, CONFIG_KAFKA_HOST, \
    CONFIG_KAFKA_PORT, DATA_SOURCE_REDDIT, DATA_SOURCE_RSS, DATA_SOURCE_TWITTER
from data_collectors import RedditDataCollector, RssDataCollector, TwitterDataCollector
from producer import Producer


def get_arguments():
    parser = argparse.ArgumentParser(description='Script for collecting data from different data sources and publish it to a Kafka broker')
    parser.add_argument('--config', required=True, help='Configuration file for the data collection script')
    subparsers = parser.add_subparsers(dest='data_source')
    rss_parser = subparsers.add_parser('rss', help='Scrape data from RSS feeds')
    rss_parser.add_argument('--base_url', required=True, help='URL of a RSS feed database where links to relevant RSS feeds can be found')
    return parser.parse_args()


def get_config(config_path): 
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def get_data_collector_instance(args):
    if args.data_source == DATA_SOURCE_RSS:
        return RssDataCollector(args.base_url)
    elif args.data_source == DATA_SOURCE_REDDIT: 
        return RedditDataCollector()
    elif args.data_source == DATA_SOURCE_TWITTER:
        return TwitterDataCollector()
    else:
        raise NotImplementedError


def main():
    args = get_arguments()
    config = get_config(args.config)
    kafka_host = config[CONFIG_KAFKA][CONFIG_KAFKA_HOST]
    kafka_port = config[CONFIG_KAFKA][CONFIG_KAFKA_PORT]
    producer = Producer(kafka_host, kafka_port)
    data_collector = None
    try:
        data_collector = get_data_collector_instance(args)
    except NotImplementedError:
        logging.error(f'Data collection not implemented for data source {args.data_source}')
    
    max_workers = int(config[CONFIG_GENERAL][CONFIG_GENERAL_MAX_WORKERS])
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            futures = data_collector.get_data_collection_futures(executor=executor)
            for future in futures:
                future.add_done_callback(partial(producer.publish, args.data_source, future.result().text))
        except RequestException as e:
            logging.error(f'Error in GET-Request: {e}')



if __name__ == '__main__':
    main()