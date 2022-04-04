import os
import sys

import requests_mock

sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "..", "src"))
from data_collectors import RssDataCollector
from helper import split_rss_feed

TEST_DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data")
PATH_FEED_DATABASE = os.path.join(TEST_DATA_FOLDER, 'example_feed_database.html')
PATH_RAW_FEED = os.path.join(TEST_DATA_FOLDER, 'raw_feed.xml')

REQUIRED_KEYS = ['title', 'title_detail', 'summary', 'summary_detail', 'links', 'link', 'id', 'guidislink', 'published', 'published_parsed', 'media_content', 'feed_source']

TEST_CONFIG_RSS_REQUEST_HEADERS = {
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


@requests_mock.Mocker(kw='mocked_request')
def test_get_feed_urls(**kwargs):
    base_url = 'http://test.com'
    with open(PATH_FEED_DATABASE, encoding='utf-8') as html_file:
        response = html_file.read()
        kwargs['mocked_request'].get(base_url, text=response)

    rss_data_collector = RssDataCollector(base_url=base_url, request_headers=TEST_CONFIG_RSS_REQUEST_HEADERS)
    feeds = rss_data_collector._get_feed_urls()
    assert kwargs['mocked_request'].called
    assert len(feeds) == 5

def test_split_rss_feeds():
    with open(PATH_RAW_FEED, 'r') as raw_feed_file:
        raw_feed = raw_feed_file.read()
        parsed_feed = split_rss_feed(raw_feed)
        assert len(parsed_feed) == 2

        for feed_entry in parsed_feed:
            assert len(feed_entry.keys()) == len(REQUIRED_KEYS)
