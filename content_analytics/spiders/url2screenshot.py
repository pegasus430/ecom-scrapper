import urlparse
import datetime
import base64

from scrapy.spiders import Spider
from scrapy.item import Item, Field
from scrapy.http.request import Request

from content_analytics.middlewares.splash import SplashContext, get_splash_args, crop_image
from content_analytics.lua_scripts import DEFAULT_MAIN


class URL2ScreenshotItem(Item):
    url = Field()
    screenshot = Field()
    creation_datetime = Field()

    def __repr__(self):
        '''
        Print [image data] for 'screenshot' field when present
        so that logs wouldn't be littered with image bytes.
        '''
        temp_single = self.get('screenshot')
        if temp_single:
            self['screenshot'] = '[image data]'
        ret = super(URL2ScreenshotItem, self).__repr__()
        if temp_single:
            self['screenshot'] = temp_single
        return ret


def _get_domain(url):
    return urlparse.urlparse(url).netloc.replace('www.', '')


class URL2ScreenshotSpider(Spider):
    name = "url2screenshot_products"

    _message = {}

    DEFAULT_WALMART_STORE = '5260'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36'
    }

    def __init__(self, *args, **kwargs):
        if kwargs.get('message'):
            self._message = kwargs.get('message')
        self.url = kwargs.get('product_url')
        if self._message:
            self.url = self._message.get('url')
        self.width = kwargs.get('height', '1696')
        self.height = kwargs.get('width', '1280')
        self.timeout = kwargs.get('timeout', 180)
        self.wait = kwargs.get("wait", 5)  # splash wait time
        self.crop_left = kwargs.get('crop_left', 0)
        self.crop_top = kwargs.get('crop_top', 0)
        self.crop_width = kwargs.get('crop_width', 0)
        self.crop_height = kwargs.get('crop_height', 0)
        self.render_all = kwargs.get('render_all', True)  # whether to render the whole page
        self.debug_screenshot_path = kwargs.get("debug_path")  # where to save image locally for debugging
        if self.debug_screenshot_path and not self.debug_screenshot_path.endswith('.png'):
            self.debug_screenshot_path = ''
            self.logger.warning('debug_path option must end with ".png"')

        super(URL2ScreenshotSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        domain = _get_domain(self.url)
        if domain == 'jet.com':
            yield self._jet_request()
        elif domain == 'walmart.com':
            yield self._walmart_request()
        else:
            yield self._default_request()

    def parse(self, response):
        item = URL2ScreenshotItem()
        item['url'] = self.url
        item['creation_datetime'] = datetime.datetime.utcnow().isoformat()
        cropped_image = crop_image(response.body, self.crop_height, self.crop_width,
                                   self.crop_left, self.crop_top)
        if self.debug_screenshot_path:
            self._save_locally(cropped_image, self.debug_screenshot_path)
        item['screenshot'] = base64.b64encode(cropped_image)
        return item

    def _default_request(self):
        with SplashContext(Request(url=self.url)) as splash_request:
            self._fill_splash_args(splash_request)
            return splash_request

    def _walmart_request(self):
        request = self._default_request()
        cookies = {'t-loc-psid': "1517356298995|{}".format(self.DEFAULT_WALMART_STORE),
                   't-loc-zip': "1517356208058|72758"}
        request.cookies.update(cookies)
        return request

    def _jet_request(self):
        request = self._default_request()
        request.meta['splash']['endpoint'] = 'execute'
        args = get_splash_args(request)
        args['lua_source'] = DEFAULT_MAIN
        return request

    def _save_locally(self, image, path):
        try:
            with open(path, 'wb') as handle:
                handle.write(image)
                self.logger.debug("Saved screenshot to {}".format(self.debug_screenshot_path))
        except (OSError, IOError) as e:
            self.logger.warning("Couldn't save screenshot locally: {}".format(e.message))

    def _fill_splash_args(self, request):
        request.meta.setdefault('splash', {})['endpoint'] = 'render.png'
        args = get_splash_args(request)
        args['render_all'] = self.render_all
        args['viewport'] = 'x'.join((self.width, self.height))
        args['timeout'] = self.timeout
        args['wait'] = self.wait
        return request
