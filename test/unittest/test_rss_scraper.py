import os
import sys

sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "..", "src"))
from data_collectors import RssDataCollector

TEST_DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data")
PATH_FEED_DATABASE = os.path.join(TEST_DATA_FOLDER, 'example_feed_database.html')


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

def test_get_feed_urls(requests_mock):
    base_url = 'http://test.com'
    with open(PATH_FEED_DATABASE, encoding='utf-8') as html_file:  
        requests_mock.get(base_url, text=html_file.read())

    rss_data_collector = RssDataCollector(base_url=base_url, request_headers=TEST_CONFIG_RSS_REQUEST_HEADERS)
    feeds = rss_data_collector._get_feed_urls()
    assert len(feeds) == 5
