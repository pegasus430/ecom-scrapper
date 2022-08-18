import random
import logging

from scrapy import Request
from scrapy.downloadermiddlewares.retry import RetryMiddleware

logger = logging.getLogger(__name__)


class ProxyRequest(Request):
    pass


class ProxyContext(object):
    PROXY_ATTRIBUTE = 'use_proxies'
    request = None

    def __init__(self, request):
        assert isinstance(request, Request)
        setattr(request, self.PROXY_ATTRIBUTE, True)
        self.request = request

    def __enter__(self):
        return self.request

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            logger.exception('Error in proxy context {}'.format(exc_tb))


class ProxyRetryDownloaderMiddleware(RetryMiddleware):
    def process_request(self, request, spider):
        proxies = spider.settings.get('proxies')
        if proxies and getattr(request, ProxyContext.PROXY_ATTRIBUTE, None):
            spider_name = spider.name.split('_', 1)[0]
            # TODO implement getting config from bucket
            if isinstance(proxies, dict):
                proxy_endpoints = proxies.get(spider_name, {})
                if proxy_endpoints:
                    chosen_endpoint = self._weighted_choice(proxy_endpoints)
                    request.meta['proxy'] = chosen_endpoint

    def _retry(self, request, reason, spider):
        request = super(ProxyRetryDownloaderMiddleware, self)._retry(request, reason, spider)
        if isinstance(request, Request) and spider.settings.get('proxies'):
            request.headers.update({'Connection': 'close'})
        return request

    @staticmethod
    def _weighted_choice(choices):
        total = sum(choices.values())
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices.items():
            if upto + w >= r:
                return c
            upto += w
