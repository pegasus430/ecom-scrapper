import json
import logging
import os
import re
import traceback
import csv
from os import path

from scrapy.item import Item
from scrapy.http.request import Request
from fuzzywuzzy import fuzz, process
from OpenSSL import SSL
from scrapy.core.downloader.contextfactory import ScrapyClientContextFactory
from twisted.internet._sslverify import ClientTLSOptions

logger = logging.getLogger(__name__)


# Exceptions handling
def catch_dictionary_exception(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except (TypeError, KeyError):
            logger.warning(
                'Can not retrieve value for the key: {}'.format(traceback.format_exc())
            )
    return wrapper


def catch_json_exceptions(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except (ValueError, IndexError):
            logger.warning(
                'Can not load json: {}'.format(traceback.format_exc())
            )
    return wrapper


def aws_from_settings(settings, prefix=''):
    AWS_REGION_NAME = settings.get('{}AWS_REGION_NAME'.format(prefix)) or os.environ.get('AWS_REGION_NAME')
    AWS_ACCESS_KEY_ID = settings.get('{}AWS_ACCESS_KEY_ID'.format(prefix)) or os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = settings.get('{}AWS_SECRET_ACCESS_KEY'.format(prefix)) or os.environ.get('AWS_SECRET_ACCESS_KEY')

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return {
            'region_name': AWS_REGION_NAME,
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
        }


def identity(x):
    return x


def replace_http_with_https(url):
    return re.sub('^http://', 'https://', url)


def cond_set_value(item, key, value, conv=identity):
    """Conditionally sets the given value to the given dict.

    The condition is that the key is not set in the item or its value is None.
    Also, the value to be set must not be None.
    """
    if item.get(key) is None and value is not None and conv(value) is not None:
        item[key] = conv(value)
    return item


def deep_search(needle, haystack):
    found = []

    if isinstance(haystack, dict):
        if needle in haystack.keys():
            found.append(haystack[needle])

        elif len(haystack.keys()) > 0:
            for key in haystack.keys():
                result = deep_search(needle, haystack[key])
                found.extend(result)

    elif isinstance(haystack, list):
        for node in haystack:
            result = deep_search(needle, node)
            found.extend(result)

    return found


# Return the string which is found between first string and last string in string s
def find_between(s, first, last, offset=0):
    try:
        start = s.index(first, offset) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ''


# Return whether an rgb value is black, white, or gray
def get_color(rgb):
    if rgb == (0, 0, 0):
        return 'black'
    if rgb == (255, 255, 255):
        return 'white'
    return 'gray'


def parse_all_webcollage(wc_page_contents, product):
    if wc_page_contents is not None:
        # webcollage 360
        wc_360 = _parse_wc_360(wc_page_contents)
        cond_set_value(product, 'wc_360', wc_360)

        # webcollage emc
        wc_emc = _parse_wc_emc(wc_page_contents)
        cond_set_value(product, 'wc_emc', wc_emc)

        # webcollage images
        wc_images = _parse_wc_images(wc_page_contents)
        cond_set_value(product, 'webcollage_image_urls', wc_images)
        cond_set_value(product, 'webcollage_images_count', len(wc_images))

        # webcollage pdf
        wc_pdf = _parse_wc_pdf(wc_page_contents)
        cond_set_value(product, 'wc_pdf', bool(wc_pdf))
        cond_set_value(product, 'webcollage_pdfs_count', len(wc_pdf))

        # webcollage prodtour
        wc_prodtour = _parse_wc_prodtour(wc_page_contents)
        cond_set_value(product, 'wc_prodtour', wc_prodtour)

        # webcollage videos
        wc_videos = _parse_wc_videos(wc_page_contents)
        cond_set_value(product, 'wc_video', bool(wc_videos))
        cond_set_value(product, 'webcollage_video_urls', wc_videos)
        cond_set_value(product, 'webcollage_videos_count', len(wc_videos))


def _parse_wc_360(wc_page_contents):
    return bool(wc_page_contents.xpath('//div[@data-section-tag="360-view"]'))


def _parse_wc_emc(wc_page_contents):
    return bool(wc_page_contents.xpath('//div[contains(@class, "wc-responsive")]'))


def _parse_wc_images(wc_page_contents):
    return wc_page_contents.xpath('//img[contains(@class,"wc-image")]/@src') + \
            wc_page_contents.xpath('//div[contains(@class, "wc-gallery-thumb")]//img/@wcobj')


def _parse_wc_pdf(wc_page_contents):
    return wc_page_contents.xpath('//img[@wcobj-type="application/pdf"]/@wcobj')


@catch_json_exceptions
def _parse_wc_prodtour(wc_page_contents):
    wc_json_data = wc_page_contents.xpath('//div[@class="wc-json-data"]/text()')

    if wc_json_data:
        wc_json_data = json.loads(wc_json_data[0])

        if wc_json_data.get('tourViews'):
            return True

    return False


def _parse_wc_videos(wc_page_contents):
    return wc_page_contents.xpath('//div[@itemprop="video"]/meta[@itemprop="contentUrl"]/@content')


def guess_brand(text):
    # pylint: disable=W0601
    # TODO rework brands from global? Pylint asks to define it at module level
    global brands

    if not text or not isinstance(text, basestring):
        return

    if 'brands' not in globals():
        # read brands once in a dict: key=brand, value=brand group
        brands = {}

        with open(path.join(path.dirname(__file__) + '/data/brands.csv'), 'r') as brands_file:
            brands_list = csv.reader(brands_file)

            for row in brands_list:
                row = [x.strip() for x in row]

                for brand in row:
                    brands[brand] = row[0]

    # try the first word
    # scorer: token_sort_ratio
    # ratio: 90
    first_word = text.split(' ')[0]
    match = process.extractOne(first_word, brands.keys(), scorer=fuzz.token_sort_ratio, score_cutoff=90)
    if match:
        return brands[match[0]]

    # use the whole text
    # scorer: token_set_ratio
    # ratio: 100
    matches = process.extractBests(text, brands.keys(), scorer=fuzz.token_set_ratio, score_cutoff=100, limit=None)
    if matches:
        # select the longest brand
        return brands[max(matches, key=lambda (brand, score): len(brand))[0]]


def fetch_product_from_req_or_item(req_or_item):
    if isinstance(req_or_item, Item):
        return req_or_item
    elif isinstance(req_or_item, Request):
        return req_or_item.meta.get('item')


class CustomClientContextFactory(ScrapyClientContextFactory):
    def getContext(self, hostname=None, port=None):
        ctx = ScrapyClientContextFactory.getContext(self)
        ctx.set_options(SSL.OP_ALL)
        if hostname:
            ClientTLSOptions(hostname, ctx)
        return ctx
