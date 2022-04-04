import abc
import re
import requests
import tweepy
import json

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
        rss_urls = list(map(lambda tag: re.search('href="(.*?)"', tag).group(1), filtered_tags))
        rss_urls = list(filter(lambda link: len(link) > 0, rss_urls))
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
    def __init__(self, consumer_key, consumer_secret, bearer_token):
        super().__init__()
        auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret)
        self._API = tweepy.API(auth) # from Twitter APIv1.1
        self._CLIENT = tweepy.Client(bearer_token) # from Twitter APIv2

    def get_data_collection_futures(self, executor):
        """Get futures where data is collected from Twitter 

        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        trending_locations = self._get_trending_locations()
        queries = self._get_trending_topics(trending_locations)
        futures = list(map(lambda query: executor.submit(self._process_query, query), queries))
        return futures

    def _get_trending_locations(self):
        trending_locations = set()
        # Worldwide
        trending_locations.add(1)
        # Germany
        trending_locations.add(23424829)
        # Nearby to Regensburg
        lat_rgb = 49.1
        long_rgb = 12.6
        locations = self._API.closest_trends(lat_rgb, long_rgb)
        for location in locations:
            trending_locations.add(location['woeid'])
        return list(trending_locations)

    def _get_trending_topics(self, place_ids):
        queries = set()
        for place_id in place_ids:
            # The exclusion of hashtags might lead to a bit more serious collection of the latest news topics
            results = self._API.get_place_trends(place_id, exclude='hashtags')[0]
            for trend in results['trends']:
                queries.add(trend['query'])
        return list(queries)

    def _process_query(self, query):
        # Refine the query with additional statements
        query += ' -is:retweet -is:reply is:verified (lang:en OR lang:de)'

        # Search Tweets request to the Twitter APIv2
        tweets = self._CLIENT.search_recent_tweets(query,
            tweet_fields=['text', 'created_at', 'lang', 'geo'],
            expansions=['geo.place_id'],
            max_results=100)

        # Get places list from the includes object
        places = {}
        if 'places' in tweets.includes:
            places = {place.id: place.full_name for place in tweets.includes['places']}

        # Build results dict
        results = {}
        for tweet in tweets.data:
            place = ''
            if tweet.geo:
                place = places[tweet.geo['place_id']]
            results[tweet.id] = {
                'text': tweet.text,
                'created_at': str(tweet.created_at),
                'lang': tweet.lang,
                'place': place
            }
        
        # Stringify results dict
        return json.dumps(results)
