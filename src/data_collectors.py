import abc
import re
import requests
import praw
import tweepy
import json
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
        subreddits = [
            'worldnews',
            'news',
            'europe',
            'politics',
            'liberal',
            'conservative',
            'upliftingnews',
            'truereddit',
            'inthenews',
            'nottheonion']

        submissions = []
        for subreddit in subreddits:
            submissions += self._get_submissions(subreddit)

        futures = list(map(lambda submission: executor.submit(self._process_submission, submission), submissions))
        return futures

    def _get_submissions(self, query):
        """Fetch up to 100 hot submissions of given subreddit
        :param query: Name of reddit to fetch submissions from
        :type query: str
        :return submissions: Fetched submissions
        :rtype: praw.models.listing.generator.ListingGenerator
        """
        subreddit = self._API.subreddit(query)
        submissions = subreddit.hot(limit=100)
        return submissions

    def _get_author_information(self, obj):
        """Collects relevant author information of given object
        :param obj: Source of author information, either submission or comment
        :type obj: praw.models.reddit.comment.Comment_or_praw.models.reddit.submission.Submission
        :return: Author information dictionary
        :rtype: dict
        """
        redditor = obj.author.stream.redditor
        return {
            'name': redditor.name,
            'member_since': (datetime.now() - datetime.fromtimestamp(redditor.created)).total_seconds(),
            'karma': {
                'awardee': redditor.awardee_karma,
                'awarder': redditor.awarder_karma,
                'comment': redditor.comment_karma,
                'link': redditor.link_karma,
                'total': redditor.total_karma,
            }
        }

    def _process_submission(self, submission):
        """
        Processes given submission by extracting up to 20 comments and building a result dictionary
        consisting of relevant information of the submission and a dict of comments.
        :param submission: Submission to be processed
        :type submission: praw.models.reddit.submission.Submission
        :return: Result dictionary with processed data of given submission
        :rtype: dict
        """
        # Fetch the top 20 comments
        submission.comment_sort = 'top'
        submission.comment_limit = 20
        submission.comments.replace_more(limit=0)
        comment_forest = submission.comments.list()

        # Build up comments list
        comments = []
        for comment in comment_forest:
            comments.append({
                'author': self._get_author_information(comment),
                'text': comment.body,
                'created': str(datetime.fromtimestamp(comment.created)),  # CEST
                'score': comment.score
            })

        # Build result dict
        result = {
            'id': submission.id,
            'title': submission.title,
            'author': self._get_author_information(submission),
            # 'selftext': submission.selftext, # seems to always be empty, dump it for now
            'created': str(datetime.fromtimestamp(submission.created)),  # CEST
            'score': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'domain': submission.domain,
            'url': submission.url,
            'reddit': {
                'subreddit': submission.subreddit.display_name,
                'url': submission.permalink,
            },
            # sort by comment score, start with highest
            'comments': sorted(comments, key=lambda c: c['score'], reverse=True)
        }

        # Stringify result dict
        return json.dumps(result)


class TwitterDataCollector(BaseDataCollector):
    def __init__(self, consumer_key, consumer_secret, bearer_token):
        super().__init__()
        auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret)
        self._API = tweepy.API(auth)  # from Twitter APIv1.1
        self._CLIENT = tweepy.Client(bearer_token)  # from Twitter APIv2

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
