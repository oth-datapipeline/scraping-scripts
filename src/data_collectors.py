import abc
import re
import requests
import praw
import tweepy
import json
import time
from datetime import datetime

from constants import FEED_ENTRY_REGEX, FEED_URL_REGEX, TIMEOUT_RSS_REQUEST
from helper import get_request_with_timeout


class BaseDataCollector(object):
    """Base class for data collectors from different data sources
    """

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
        futures = list(
            map(lambda url: executor.submit(get_request, url), feed_urls))
        return futures

    def _get_feed_urls(self):
        headers = self.request_headers
        site_content = requests.get(self.base_url, headers=headers)
        feed_entries = re.findall(FEED_ENTRY_REGEX, site_content.text)

        rss_urls = []
        for feed_entry in feed_entries:
            url = re.search(FEED_URL_REGEX, feed_entry)
            rss_urls.append(url.group(1)) if url else None
        return rss_urls


class RedditDataCollector(BaseDataCollector):
    def __init__(self, client_id, client_secret):
        super().__init__()
        self._API = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent='oth-datapipeline')

    def get_data_collection_futures(self, executor):
        """Get futures where data is collected from Reddit
        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        subreddits = ['worldnews', 'news', 'europe', 'politics', 'upliftingnews', 'truereddit', 'inthenews', 'nottheonion']
        submissions = self._get_submissions(subreddits)
        futures = list(map(lambda submission: executor.submit(self._process_submission, submission), submissions))
        return futures
    
    def _get_submissions(self, subreddits):
        query = 'all'
        if len(subreddits) > 0:
            query = '+'.join(subreddits)
        subreddit = self._API.subreddit(query)
        submissions = subreddit.top('day')
        return submissions
    
    def _process_submission(self, submission):
        # Fetch the top ten comments
        submission.comment_sort = 'top'
        submission.comment_limit = 10
        submission.comments.replace_more(limit=0)
        comment_forest = submission.comments.list()
        
        # Build up comments list
        comments = []
        for comment in comment_forest:
            comments.append({
                'text': comment.body,
                'created': str(datetime.fromtimestamp(comment.created)), # CEST
                'score': comment.score
            })
        
        # Build result dict
        result = {
            'id': submission.id,
            'title': submission.title,
            'selftext': submission.selftext,
            'created': str(datetime.fromtimestamp(submission.created)), # CEST
            'score': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'domain': submission.domain,
            'comments': comments
        }
        
        # Stringify result dict
        return json.dumps(result)


class TwitterDataCollector(BaseDataCollector):
    # Storing the current trends in a static variable
    # Purpose: Fetching trends has a significantly lower rate limit than searching tweets
    _current_trends = {'queries': [], 'fetched_at': None}
    
    def __init__(self, consumer_key, consumer_secret, bearer_token):
        super().__init__()
        auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret)
        self._API = tweepy.API(auth)  # from Twitter APIv1.1
        self._CLIENT = tweepy.Client(bearer_token)  # from Twitter APIv2
        # Initially fetching the current trends
        self._update_current_trends()

    def get_data_collection_futures(self, executor):
        """Get futures where data is collected from Twitter

        :param executor: Executor where the futures are submitted to
        :type executor: concurrent.futures.Executor
        :return futures: futures
        :rtype: concurrent.futures.Future
        """
        last_fetched_at = self._current_trends['fetched_at']
        # Update the current trends at earliest after 15 min
        if (time.time() - last_fetched_at) / 60 > 15:
            self._update_current_trends()
        queries = self._current_trends['queries']
        futures = list(map(lambda query: executor.submit(self._process_query, query), queries))
        return futures
    
    def _update_current_trends(self):
        # Trending location: Worldwide (woeid: 1)
        trending_location = 1
        results = self._API.get_place_trends(trending_location)[0]
        queries = set()
        for trend in results['trends']:
            queries.add(trend['name'])
        self._current_trends['queries'] = list(queries)
        self._current_trends['fetched_at'] = time.time()

    def _process_query(self, trending_topic):
        # Refine the query with additional statements
        query = trending_topic + ' -is:retweet -is:reply -is:nullcast lang:en'

        # Search Tweets request to the Twitter APIv2
        tweets = self._CLIENT.search_recent_tweets(query,
                                                   tweet_fields=['text', 'created_at', 'lang', 'public_metrics', 'geo'],
                                                   user_fields=['username', 'verified', 'public_metrics'],
                                                   expansions=['author_id', 'geo.place_id'],
                                                   sort_order='relevancy',
                                                   max_results=100)

        # Get users and places lists from the includes object
        users = {}
        if 'users' in tweets.includes:
            users  = {user.id: {'username': user.username,
                                'verified': user.verified,
                                'num_followers': user.public_metrics['followers_count']} for user in tweets.includes['users']}
        places = {}
        if 'places' in tweets.includes:
            places = {place.id: place.full_name for place in tweets.includes['places']}

        # Build results dict
        results = {}
        for tweet in tweets.data:
            if tweet.public_metrics['like_count'] < 1000:
                continue
            author = {}
            if tweet.author_id:
                author = users[tweet.author_id]
            place = ''
            if tweet.geo:
                place = places[tweet.geo['place_id']]
            results[tweet.id] = {
                'text': tweet.text,
                'created_at': str(tweet.created_at),
                'metrics': tweet.public_metrics,
                'author': author,
                'place': place,
                'trend': trending_topic
            }

        # Stringify results dict
        return json.dumps(results)
