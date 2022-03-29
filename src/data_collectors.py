import abc
import re
import requests

from constants import TIMEOUT_RSS_REQUEST
from helper import get_request_with_timeout


class BaseDataCollector(object):
    """Base class for data collectors from different data sources
    """
    def __init__(self):
        pass

    @abc.abstractmethod
    def get_data_collection_futures(self, executor):
        """Get futures where the data from the data source is collected

        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        pass


class RssDataCollector(BaseDataCollector):
    """Data collector for RSS feeds

    :param base_url: URL of a RSS feed database where links to relevant RSS feeds can be found
    :type base_url: str
    :param request_headers: Header fields for the GET request
    :type request_headers: dict
    """
    def __init__(self, base_url, request_headers):
        super().__init__()
        self.base_url = base_url
        self.request_headers = request_headers
    
    def get_data_collection_futures(self, executor):
        """Get futures where data is collected from RSS feeds 

        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        feed_urls = self._get_feed_urls()
        get_request = get_request_with_timeout(TIMEOUT_RSS_REQUEST)
        futures = list(map(lambda url: executor.submit(get_request, url), feed_urls))
        return futures

    def _get_feed_urls(self):
        headers = self.request_headers
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

    def get_data_collection_futures(self, executor):
        """Get futures where data is collected from Reddit

        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        # TODO: Implement logic for Reddit
        pass


class TwitterDataCollector(BaseDataCollector): 
    def __init__(self):
        super().__init__()

    def get_data_collection_futures(self, executor):
        """Get futures where data is collected from Twitter 

        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        # TODO: Implement logic for Twitter
        pass