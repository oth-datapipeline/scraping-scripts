import abc
import tweepy
import json
import threading
import logging

class BaseDataStreamer(object):
    """Base class for data streamers from different data sources
    """
    @abc.abstractmethod
    def stream_into_producer(self, producer):
        """Stream data from the data source into a Kafka producer

        :param producer: Producer where the streamed data is published to
        :type producer: Producer
        """
        pass


class TwitterDataStreamer(BaseDataStreamer):
    """Data streamer fetching data from twitter and publishing into
    a Kafka producer
    """

    def __init__(self, consumer_key, consumer_secret, bearer_token):
        """Data streamer fetching tweets connected to the latest worldwide
        Twitter trends and publishing them into a Kafka producer

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
        self.bearer_token = bearer_token
        self.stop_thread = False

    def schedule(self, client):
        """Thread function which schedules a query of the latest trends
        from Twitter API in certain time intervals

        :param client: TwitterStreamingClient
        :type client: TwitterStreamingClient
        """
        while not self.stop_thread:
            self.timer.start()
            logging.debug('Fetching current trends started')
            self.timer.join()
            logging.debug('Fetching current trends ended')
            self.timer = threading.Timer(interval=30, function=self._get_current_trends, args=(client,))

    def _get_current_trends(self, client):
        """Thread function which fetches the latest trends from Twitter API
        and modifies the StreamingClient's StreamRules according to the trends

        :param client: TwitterStreamingClient
        :type client: TwitterStreamingClient
        """
        # Trending location: Worldwide (woeid: 1)
        trending_location = 1

        # Get trends from API
        results = self._API.get_place_trends(trending_location)[0]
        trends = [trend['name'] for trend in results['trends']]
        rules = client.get_rules().data

        # Delete old trends from streaming rules
        delete_ids = []
        for rule in rules:
            if rule.tag in trends:
                trends.remove(rule.tag)
            else:
                delete_ids.append(rule.id)
        if len(delete_ids) > 0:
            client.delete_rules(delete_ids)

        # Add all new trends
        new_rules = []
        for trend in trends:
            query = trend + ' -is:retweet -is:reply -is:nullcast lang:en'
            new_rules.append(tweepy.StreamRule(value=query, tag=trend))
        if len(new_rules) > 0:
            client.add_rules(new_rules)

    def stream_into_producer(self, producer):
        """Stream data from Twitter into a Kafka producer

        :param producer: Producer where the streamed data is published to
        :type producer: Producer
        """
        client = TwitterStreamingClient(self.bearer_token, producer)
        self._get_current_trends(client)

        self.timer = threading.Timer(interval=30, function=self._get_current_trends, args=(client,))

        thread = threading.Thread(target=self.schedule, args=(client,), daemon=True)
        thread.start()

        client.filter(tweet_fields=['text', 'created_at', 'lang', 'public_metrics', 'geo'],
                      user_fields=['username', 'verified', 'public_metrics'],
                      expansions=['author_id', 'geo.place_id'])

        self.stop_thread = True
        self.timer.cancel()


class TwitterStreamingClient(tweepy.StreamingClient):
    def __init__(self, bearer_token, producer):
        """StreamingClient for Twitter, which processes the streamed tweets
        and pushes it into a Kafka producer

        :param bearer_token: Bearer token
        :type bearer_token: str
        :param producer: Producer where the streamed data is published to
        :type producer: Producer
        """
        super().__init__(bearer_token, wait_on_rate_limit=True)
        self.producer = producer
        logging.debug('Twitter streaming client started')

    def on_response(self, response):
        """Generic callback function for processing a response from Twitter's
        Streaming API

        :param response: Response object containing a tweet with includes or errors
        :type response: tweepy.StreamResponse
        """
        tweet = response.data
        includes = response.includes
        rules = response.matching_rules

        result_json = self._process_tweet(tweet, includes, rules)

        self.producer.publish('twitter-stream', result_json)

    def on_errors(self, errors):
        """Callback function for processing a errors from Twitter's Streaming API

        :param errors: Errors object
        :type response: dict
        """
        logging.error(errors)
        self.disconnect()

    def _process_tweet(self, tweet, includes, rules):
        """Processing a tweet as it is fetched from the Streaming API

        :param tweet: Tweet object
        :type tweet: tweepy.tweet.Tweet
        :param includes: Additional information to tweet
        :type includes: dict
        :param rules: Matching StreamRules
        :type rules: List[tweepy.StreamRules]
        :return: Json-stringified tweet
        :rtype: str
        """
        author = {}
        if 'users' in includes and tweet.author_id in includes['users']:
            user = includes['users'][tweet.author_id]
            author = {'username': user.username,
                      'verified': user.verified,
                      'num_followers': user.public_metrics['followers_count']}
        place = ''
        if 'places' in includes and tweet.geo and tweet.geo['place_id'] in includes['places']:
            place = includes['places'][tweet.geo['place_id']].full_name

        # Build results dict
        result = {}
        result['tweet_id'] = tweet.id
        result['text'] = tweet.text
        result['created_at'] = str(tweet.created_at)
        result['metrics'] = tweet.public_metrics
        result['author'] = author
        if place != '':
            result['place'] = place
        result['trend'] = rules[0].tag

        return json.dumps([result])
