import requests


def get_request_with_timeout(timeout):
    """Wrapper for a GET-request with a timeout

    :param timeout: Time in seconds that a request will wait for a response
    :type timeout: int
    """
    def get_request(url):
        return requests.get(url, timeout=timeout)
    return get_request