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


from praw.models.redditors import Redditors

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
        self._client_id = client_id
        self._client_secret = client_secret
        self._agent_name = 'linux:oth.datapipeline:v0.1'
        self._API = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=self._agent_name)

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
        subreddit = praw.Reddit(client_id=self._client_id, client_secret=self._client_secret, user_agent=self._agent_name).subreddit(query)
        return subreddit.hot(limit=25)
        

    def _get_author_information(self, obj):
        """Collects relevant author information of given object
        :param obj: Source of author information, either submission or comment
        :type obj: praw.models.reddit.comment.Comment_or_praw.models.reddit.submission.Submission
        :return: Author information dictionary
        :rtype: dict
        """

        redditor = obj.author

        if (not redditor):
            return {}

        member_since = (datetime.now() - datetime.fromtimestamp(redditor.created)).total_seconds() if 'created' in redditor.__dict__ else -1
        created = redditor.created if 'created' in redditor.__dict__ else -1
        ret = {
            'name': redditor.name,
            'member_since': member_since,
            'created': created,
            'karma': {
                'awardee': redditor.awardee_karma,
                'awarder': redditor.awarder_karma,
                'comment': redditor.comment_karma,
                'link': redditor.link_karma,
                'total': redditor.total_karma,
            }
        }
        return ret

    def _get_author_info_from_partial(self, partial_redditor):
        """Extract relevant author information of given PartialRedditor
        :param partial_redditor: Partial redditor object created by praw
        :type partial_redditor:  praw.models.redditors.PartialRedditor
        :return: Author information dictionary
        :rtype: dict
        """

        if (not partial_redditor):
            return {}

        member_since = (datetime.utcnow() - datetime.fromtimestamp(partial_redditor.created_utc)).total_seconds() if 'created_utc' in partial_redditor.__dict__ else -1
        created_utc = partial_redditor.created_utc if 'created_utc' in partial_redditor.__dict__ else -1
        ret = {
            'name': partial_redditor.name,
            'member_since': member_since,
            'created_utc': created_utc,
            'karma': {
                'comment': partial_redditor.comment_karma,
                'link': partial_redditor.link_karma
            }
        }
        return ret

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
        comment_list = submission.comments


        # Build up comments list
        comments = []
        for comment in comment_list:
            comments.append({
                'author': self._get_author_information(comment),
                'text': comment.body,
                'created': str(datetime.fromtimestamp(comment.created)),  # CEST
                'score': comment.score
            })

        # TODO: Decide which author variant to use
        # authors = [comment.author.fullname for comment in comment_forest if comment.author]
        # redditors = Redditors(self._API, None)
        # partials = redditors.partial_redditors(authors)
        # for comment, partial in zip(comments, partials):
        #     comment["author"] = self._get_author_info_from_partial(partial)

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
    # Storing the current trends in a static variable
    # Purpose: Fetching trends has a significantly lower rate limit than searching tweets
    _current_trends = {'queries': [], 'fetched_at': None}
    
    def __init__(self, consumer_key, consumer_secret, bearer_token):
        """Data collector for tweets connected to the latest worldwide
        Twitter trends

        :param consumer_key: Authentication key
        :type consumer_key: str
        :param consumer_secret: Authentication secret
        :type consumer_key: str
        :param bearer_token: Bearer token
        :type bearer_token: str
        """
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
        """Update the static variable _current_trends with the latest
        trending topics of Twitter Worldwide trends list
        """
        # Trending location: Worldwide (woeid: 1)
        # trending_location = 1
        # 23424977 is id of USA
        trending_location = 23424977 
        results = self._API.get_place_trends(trending_location)[0]
        queries = set()
        for trend in results['trends']:
            queries.add(trend['name'])
        self._current_trends['queries'] = list(queries)
        self._current_trends['fetched_at'] = time.time()

    def _process_query(self, trending_topic):
        """Search and process the most relevant tweets for a certain
        trending topic

        :param trending_topic: phrase connected to current Twitter trend
        :type trending_topic: str
        :return results_json: json-stringified results list with tweets
        :rtype str
        """
        # Refine the query with additional statements
        and_insensitive = re.compile(' and ', re.IGNORECASE)
        trending_topic = and_insensitive.sub(' \"and\" ', trending_topic)
        query = trending_topic + ' -is:retweet -is:reply -is:nullcast lang:en'

        # Search Tweets request to the Twitter APIv2
        tweets = self._CLIENT.search_recent_tweets(query,
                                                   tweet_fields=['text', 'created_at', 'lang', 'public_metrics', 'geo'],
                                                   user_fields=['username', 'created_at', 'verified', 'public_metrics'],
                                                   expansions=['author_id', 'geo.place_id'],
                                                   sort_order='relevancy',
                                                   max_results=100)

        # Get users and places lists from the includes object
        users = {}
        if 'users' in tweets.includes:
            users  = {user.id: {'username': user.username,
                                'created_at': user.created_at,
                                'member_since': (datetime.now() - user.created_at).total_seconds(),
                                'verified': user.verified,
                                'num_followers': user.public_metrics['followers_count']} for user in tweets.includes['users']}
        places = {}
        if 'places' in tweets.includes:
            places = {place.id: place.full_name for place in tweets.includes['places']}

        # Build results list
        results = []
        if tweets.data is not None:
            for tweet in tweets.data:
                if tweet.public_metrics['like_count'] < 100:
                    continue
                result = {}
                result['tweet_id'] = tweet.id
                result['text'] = tweet.text
                result['created_at'] = str(tweet.created_at)
                result['metrics'] = tweet.public_metrics
                result['author'] = users[tweet.author_id]
                if tweet.geo:
                    result['place'] = places[tweet.geo['place_id']]
                result['trend'] = trending_topic
                results.append(result)

        # Stringify results list
        return results
