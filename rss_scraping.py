import requests
import pandas as pd
import re
import uuid
import functools
import concurrent.futures
from kafka import KafkaProducer, KafkaConsumer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer

def curried_get_request(timeout):
    def get_request(url):
        return requests.get(url, timeout=timeout)
    return get_request

def get_raw_feeds_futures(urls, executor): 
    try:
        get_request = curried_get_request(5)
        futures = list(map(lambda url: executor.submit(get_request, url), urls))
        return futures
    except requests.exceptions.RequestException as e:
        print(e)

def get_feed_urls(base_url):
    try:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "sec-ch-ua": "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"99\", \"Google Chrome\";v=\"99\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "cookie": "blog=true; ru=https://blog.feedspot.com/world_news_rss_feeds/; _ga=GA1.2.1834597446.1648146314; _gid=GA1.2.1086655725.1648146314; _gat=1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        pattern = '<a class="ext" .*?>.*?<\/a>'
        html_tags = re.findall(pattern, response.text)
        html_tags_string = "".join(html_tags)
        filtered_tags = list(filter(lambda tag: "href" in tag, html_tags_string.split(" ")))
        rss_urls = list(map(lambda tag: re.search('href="(.*)"', tag).group(1), filtered_tags))
        return rss_urls

    except requests.exceptions.RequestException as e:
        print(e)

if __name__ == '__main__':
    url = "https://blog.feedspot.com/world_news_rss_feeds/"
    rss_feeds = get_feed_urls(url)
    producer = connect_kafka_producer()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = get_raw_feeds_futures(rss_feeds, executor=executor)
        
        #publish raw xml data to kafka as soon as future is finished
        map(lambda future: future.add_done_callback(functools.partial(publish_message, producer, "rss", str(uuid.uuid4()), future.result().text)), futures)


    #Debug Code to see if publishing worked
    consumer = KafkaConsumer("rss", auto_offset_reset='earliest',
                            bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        
    for msg in consumer:
        print(msg.value)
