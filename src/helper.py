import os
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
        except Exception:
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


def build_logging_filepath(path): 
    """Function for building the path where the logs are saved to
       If a logfile with this path already exists, a counter is added to the filename

    :param path: The path to the file where the 
    """
    
    filename, extension = os.path.splitext(path)
    counter = 1
    while(os.path.exists(path)):
        path = f'{filename}_{str(counter)}{extension}'
        counter = counter + 1
    return path
