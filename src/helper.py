import requests
import feedparser

def get_request_with_timeout(timeout):
    """Wrapper for a GET-request with a timeout

    :param timeout: Time in seconds that a request will wait for a response
    :type timeout: int
    """
    def get_request(url):
        try:
            res = requests.get(url, timeout=timeout)
        except Exception(e):
            res = None
        return res 
    return get_request

def split_rss_feed(full_feed):
    """Function for splitting a raw RSS-Feed into single documents

    :param full_feed: Contents of a RSS-Feed in XML-Format
    :type full_feed: str
    """
    parsed = feedparser.parse(full_feed)
    return [dict(entry, **{"feed_source": parsed.feed.title}) for entry in parsed.entries]