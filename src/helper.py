import requests

def get_request_with_timeout(timeout):
    def get_request(url):
        return requests.get(url, timeout=timeout)
    return get_request