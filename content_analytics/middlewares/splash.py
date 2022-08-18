import logging
import traceback
import json

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from io import BytesIO
from PIL import Image

from scrapy.utils.request import request_fingerprint
from scrapy.http.request import Request
from scrapy_splash.middleware import SplashMiddleware
from scrapy_splash.request import SlotPolicy
from twisted.internet import reactor, defer
from twisted.internet.error import ConnectionRefusedError, ConnectionDone, \
        TCPTimedOutError, TimeoutError, ConnectionLost
from twisted.web.client import ResponseFailed
from twisted.web._newclient import ResponseNeverReceived


logger = logging.getLogger(__name__)

def get_splash_args(request):
    return request.meta.setdefault('splash', {}).setdefault('args', {})

def crop_image(image, height, width, left, top):
    if height and width:
        width = int(width)
        height = int(height)

        with Image.open(BytesIO(image)) as pil_img:
            box = (left,
                   top,
                   left + width,
                   top + height)
            cropped_pil_img = pil_img.crop(box)
            output = BytesIO()
            cropped_pil_img.save(output, 'PNG')
            new_image = output.getvalue()
            return new_image
    return image


class SplashContext(object):
    """Class to alter requests to render pages (or make screenshots)).

    Usage:
        req = Request(...)
        with SplashContext(req) as new_req:
            yield new_req

    In case of making a screenshot it will be returned in the body of response.
    Scrapy-splash related options may be passed through request.meta['splash']
    (e.g. splash endpoint), and splash related options may be passed through
    meta['splash']['args'] dict as key-value pairs. (e.g. viewport, timeout, etc).
    For splash options go to http://splash.readthedocs.io/en/stable/api.html
    """

    _request = None

    def __init__(self, request):
        assert isinstance(request, Request)
        request = request.replace(headers=request.headers.copy(),
                                  meta=request.meta.copy())
        request.meta.setdefault('splash', {})
        request.meta['initial_fingerprint'] = request_fingerprint(request)
        self._request = request

    def __enter__(self):
        return self._request

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            logger.exception('Error in splash context {}'.format(traceback.format_exc()))


class CustomSplashMiddleware(SplashMiddleware):

    def process_request(self, request, spider):
        if 'splash' in request.meta:
            logger.debug('processing splash request to {}'.format(request.url))
            if request.headers.get('Accept-Encoding'):   # splash can't handle Accept-Encoding
                del request.headers['Accept-Encoding']   # and can't decode gzip files
            meta = request.meta
            splash_meta = meta.get('splash')
            splash_meta.setdefault('endpoint', 'render.html')
            splash_meta.setdefault('slot_policy', SlotPolicy.PER_DOMAIN)
            splash_meta.setdefault('dont_process_response', False)
            splash_meta.setdefault('magic_response', True)
            splash_meta.setdefault('dont_send_headers', False)
            splash_meta.setdefault('http_status_from_error_code', True)
            if splash_meta['endpoint'].strip('/') == 'execute':
                splash_meta.setdefault('session_id', 'default')

            args = get_splash_args(request)
            args.setdefault('url', request.url)
            args.setdefault('timeout', 90)
            args.setdefault('wait', 5)
            args.setdefault('viewport', '1280x1024')

            proxy = request.meta.get('proxy', None)
            if proxy:
                args.setdefault('proxy', proxy)

            return super(CustomSplashMiddleware, self).process_request(request, spider)

class SplashRetryMiddleware(RetryMiddleware):
    # Splash has memory leaks, so sometimes docker needs to relaunch container
    # which takes some time, and during that time splash is unavailable.

    DEFAULT_URL = "http://127.0.0.1:8050"

    EXCEPTIONS_TO_RETRY = (ConnectionRefusedError, ConnectionDone, ResponseFailed,
                           TCPTimedOutError, TimeoutError, ResponseFailed,
                           ResponseNeverReceived, ConnectionLost)

    def __init__(self, settings):
        self.splash_url = settings.get('SPLASH_URL', self.DEFAULT_URL)
        self.splash_wait_time = settings.get('SPLASH_WAIT_TIME', 30)

        super(SplashRetryMiddleware, self).__init__(settings)
        self.max_retry_times = settings.getint('SPLASH_RETRY_TIMES', 20)

    def _retry(self, request, reason, spider):
        if request.url.startswith(self.splash_url) and isinstance(reason, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            retry_request = super(SplashRetryMiddleware, self)._retry(request, reason, spider)
            if retry_request:
                spider.crawler.stats.inc_value('retry/count/splash_unavailable')
                destination_url = json.loads(retry_request.body).get('url')
                logger.debug("Retrying request to splash for {url} in {delay} sec"
                             .format(url=destination_url, delay=self.splash_wait_time))
                deferred = defer.Deferred()
                reactor.callLater(self.splash_wait_time, deferred.callback, retry_request)
                return deferred
