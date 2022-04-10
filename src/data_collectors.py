import abc
import re
import requests
import praw
from datetime import datetime
import json

from constants import FEED_ENTRY_REGEX, FEED_URL_REGEX, TIMEOUT_RSS_REQUEST
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