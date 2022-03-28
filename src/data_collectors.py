import abc
from concurrent.futures import Future
from functools import partial
import requests
import re

from constants import TIMEOUT_RSS_REQUEST
from helper import get_request_with_timeout

class BaseDataCollector(object): 
    def __init__(self):
        pass
    
    @abc.abstractmethod
    def get_data_collection_futures(self, executor):
        pass


class RssDataCollector(BaseDataCollector): 
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
    

    def get_data_collection_futures(self, executor):
        feed_urls = self._get_feed_urls()
        get_request = get_request_with_timeout(TIMEOUT_RSS_REQUEST)
        futures = list(map(lambda url: executor.submit(get_request, url), feed_urls))
        return futures

    def _get_feed_urls(self):
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
        response = requests.get(self.base_url, headers=headers)
        pattern = '<a class="ext" .*?>.*?<\/a>'
        html_tags = re.findall(pattern, response.text)
        html_tags_string = "".join(html_tags)
        filtered_tags = list(filter(lambda tag: "href" in tag, html_tags_string.split(" ")))
        rss_urls = list(map(lambda tag: re.search('href="(.*)"', tag).group(1), filtered_tags))
        return rss_urls


class RedditDataCollector(BaseDataCollector): 
    def __init__(self):
        super().__init__()

    def get_collect_function(self):
        # TODO: Implement logic for Reddit
        pass


class TwitterDataCollector(BaseDataCollector): 
    def __init__(self):
        super().__init__()

    def get_collect_function(self):
        # TODO: Implement logic for Twitter
        pass