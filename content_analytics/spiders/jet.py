# -*- coding: utf-8 -*-
import re
import json
import traceback
import unicodedata

from six import string_types
from urlparse import urlparse, parse_qs

from scrapy import Request
from scrapy.item import Field

from content_analytics.data_parsing.buyer_reviews.powerreviews_reviews import PowerReviews
from content_analytics.utils import cond_set_value
from content_analytics.items import SiteProductItem
from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.lua_scripts import DEFAULT_MAIN
from content_analytics.middlewares.splash import get_splash_args


class JetProductItem(SiteProductItem):
    # For future comparison
    _raw_images = Field()
    _x500_images = Field()
    _x1500_images = Field()


class JetProductsSpider(BaseProductsSpider):
    name = 'jet_products'
    allowed_domains = ['jet.com']

    START_URL = 'https://jet.com'

    SEARCH_URL = 'https://jet.com/api/search/'

    PRODUCT_URL = 'https://jet.com/api/product/v2'

    PRODUCT_URL_FORMAT = 'https://jet.com/product/{slug}/{id}'

    SEARCH_TERM_SCREENSHOT_URL = 'https://jet.com/search?term={search_term}'

    REVIEWS_HEADERS = {
        'Authorization': '3ff84632-35e9-49b7-8a3a-7638cdd208cf',
        'Referer': None,
    }

    SORT_MODES = {
        'relevance': 'relevance',
        'pricelh': 'price_low_to_high',
        'pricehl': 'price_high_to_low',
        'member_savings': 'smart_cart_bonus'
    }

    csrf_token = None

    custom_settings = {
        'USER_AGENT': 'Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)',
    }

    def __init__(self, *args, **kwargs):
        super(JetProductsSpider, self).__init__(*args, **kwargs)
        self.zip_code = kwargs.get('zip_code', '94117')

    def start_requests(self):
        yield Request(
            url=self.START_URL,
            callback=self.make_csrf_requests
        )

    @staticmethod
    def parse_410(response):
        product = response.meta.get('item')
        cond_set_value(product, 'not_found', True)
        return product

    def make_csrf_requests(self, response):
        self.csrf_token = self._parse_csrf_token(response)
        if not self.csrf_token:
            self.logger.error('Could not retrieve CSRF-token from initial page')
            return

        for request in super(JetProductsSpider, self).start_requests():
            yield request

    def get_default_item(self, *args, **kwargs):
        return JetProductItem()

    # Search term
    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(JetProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        yield request.replace(
            method='POST',
            body=json.dumps({
                'term': search_term,
                'origination': 'PLP',
                'sort': self.SORT_MODES.get('relevance'),
                'zipcode': self.zip_code
            }),
            headers={
                'x-csrf-token': self.csrf_token,
                'jet-referer': '/search?term={}'.format(search_term),
                'content-type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
            }
        )

    def get_search_term_url(self, _):
        return self.SEARCH_URL

    def get_search_term_next_page(self, response):
        data = self._parse_product_data(response)
        page = response.meta.get('page', 1)
        if page <= data.get('total', 1) // data.get('query', {}).get('size', 24):
            page += 1
            search_term = response.meta.get('search_term')
            request = next(self.make_search_term_requests(search_term))
            body = json.loads(request.body)
            body.update({'page': page})
            request = request.replace(body=json.dumps(body))
            request.meta.update({'page': page})
            return request

    def parse_search_term_items(self, response):
        data = self._parse_product_data(response)
        for product in data.get('products', []):
            yield self.build_url(
                title=product.get('title'),
                _id=product.get('id')
            )

    def parse_search_term_total_matches(self, response):
        data = self._parse_product_data(response)
        try:
            return int(data.get('totalFull'))
        except Exception:
            self.logger.warning('Could not retrieve total matches {}'.format(traceback.format_exc()))

    def parse_search_term_results_per_page(self, response):
        data = self._parse_product_data(response)
        try:
            return int(data.get('query', {}).get('size'))
        except Exception:
            self.logger.warning('Could not retrieve results_per_page {}'.format(traceback.format_exc()))

    # Shelf page
    def make_shelf_page_requests(self, url, *args, **kwargs):
        def prepare_query(_query):
            """
            Cleanups query string of shelf page URL.
            Converts all values and single values in lists into string.
            Removes empty values.
            :param _query: url query converted by `urlparse.prase_qs` method
            :return: dictionary with query parameters
            """
            assert isinstance(_query, dict)
            for k, v in _query.items():
                if not v:
                    del _query[k]
                if isinstance(v, list):
                    if len(v) == 1:
                        _query[k] = v[0]
            return _query

        page = kwargs.get('page', 1)
        query = kwargs.get('query') or parse_qs(urlparse(url).query)
        if not query:   # new shelf url format
            category_id = re.search(r"\w\/(\d+)\b", url)
            category_id = category_id.group(1) if category_id else None
            query = {"category": category_id}
        request = next(super(JetProductsSpider, self).make_shelf_page_requests(url, *args, **kwargs))
        request.meta.update({'query': query, 'page': page})
        request = request.replace(
            url=self.SEARCH_URL,
            method='POST',
            body=json.dumps(prepare_query({
                'origination': 'PLP',
                'categories': query.get('category'),
                'attributes': query.get('attribute'),
                'filters': query.get('filter'),
                'rating': query.get('rating'),
                'page': str(page),

            })),
            headers={
                'x-csrf-token': self.csrf_token,
                'jet-referer': url,
                'content-type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
            }
        )
        yield request

    def get_shelf_page_next_page(self, response):
        page = response.meta.get('page', 1) + 1
        query = response.meta.get('query', {})
        return next(self.make_shelf_page_requests(self.SEARCH_URL, query=query, page=page))

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)

    # Single product
    def make_single_product_requests(self, url, *args, **kwargs):
        request = next(super(JetProductsSpider, self).make_single_product_requests(url, *args, **kwargs)).replace(
            url=self.PRODUCT_URL,
            method='POST',
            body=json.dumps({
                'sku': self._parse_site_product_id(url),
                'origination': 'none',
            }),
            headers={
                'x-csrf-token': self.csrf_token,
                'referer': url,
                'jet-referer': url,
                'content-type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
            }
        )
        request.meta.get('item', {}).update({'url': url})
        request.meta.update({'handle_httpstatus_list': [400, 404, 410]})
        yield request

    def parse_404(self, response):
        product = response.meta.get('item')
        cond_set_value(product, 'not_found', True)
        yield product

    def parse_400(self, response):
        return self.parse_404(response)

    def parse_product(self, response):
        data = self._parse_product_data(response)
        if not data:
            return self.parse_404(response)
        return self._parse_product(response, data)

    def _parse_product_data(self, response):
        try:
            return json.loads(response.body, encoding='utf-8').get('result')
        except Exception:
            self.logger.error('Error while parsing product data {}'.format(traceback.format_exc()))

    @staticmethod
    def _parse_csrf_token(response):
        csrf_token = response.xpath(
            '//*[@data-id="csrf"]'
            '/@data-val'
        ).extract_first()
        if csrf_token:
            return csrf_token.replace('"', '')

    def build_url(self, title, _id):
        slug = unicodedata.normalize('NFKD', title).encode('ascii', 'ignore').decode('ascii')
        slug = re.sub(r'[^\w\s-]', '', slug).strip()
        slug = re.sub(r'[-\s]+', '-', slug)
        return self.PRODUCT_URL_FORMAT.format(slug=slug, id=_id)

    ############################################
    ############################################

    def _parse_product(self, response, data):
        product = response.meta.get('item')

        # bullets
        cond_set_value(product, 'bullets', self._parse_bullets(data))

        # short_description
        cond_set_value(product, 'short_description', data['description'])

        # departments
        cond_set_value(product, 'departments', data['categoryPath'].split('|'))

        # image urls
        cond_set_value(product, 'image_urls', self._parse_image_urls(data))
        cond_set_value(product, 'image_url', product['image_urls'][0] if product['image_urls'] else None)

        # image_dimensions & zoom image dimensions
        cond_set_value(product, 'image_dimensions', [None] * len(data.get('images')))
        cond_set_value(product, 'zoom_image_dimensions', [None] * len(data.get('images')))

        def make_image_requests(data_key, field):
            image_urls = [i.get(data_key) for i in data.get('images', [])]
            cond_set_value(product, field, [None] * len(image_urls))

            for i, image_url in enumerate(image_urls):
                yield MergeRequest(
                    url=image_url,
                    item=product,
                    method='HEAD',
                    callback=self._parse_dimensions,
                    meta={
                        'index': i,
                        'field': field
                    },
                    dont_filter=True
                )

        for req in make_image_requests('raw', '_raw_images'):
            yield req
        for req in make_image_requests('x500', '_x500_images'):
            yield req
        for req in make_image_requests('x1500', '_x1500_images'):
            yield req

        # in stores
        cond_set_value(product, 'in_stores', False)

        # model
        cond_set_value(product, 'model', data.get('part_no'))

        # price amount
        cond_set_value(product, 'price_amount', self._parse_price_amount(data))

        # new price
        cond_set_value(product, 'now_price', self._parse_price_amount(data))

        # old price
        was_price = self._parse_was_price(data)
        cond_set_value(product, 'was_price', was_price)

        # volume measure
        cond_set_value(product, 'volume_measure', self._parse_volume_measure(data))

        # price per volume
        cond_set_value(product, 'price_per_volume', self._parse_price_per_volume(data))

        # promotions
        cond_set_value(product, 'promotions', self._parse_promotions(data))

        # primary seller
        cond_set_value(product, 'primary_seller', data.get('manufacturer'))

        # site product id
        cond_set_value(product, 'site_product_id', self._parse_site_product_id(product.get('url')))

        # buyer reviews
        product_id = self._parse_site_product_id(product.get('url').lower())  # api can't handle uppercase sku
        yield MergeRequest(
            url=PowerReviews.compile_url(product_id=product_id, group_id='786803'),
            item=product,
            callback=self._parse_buyer_reviews,
            headers=self.REVIEWS_HEADERS,
            dont_filter=True
        )

        # site_online
        cond_set_value(product, 'site_online', True)

        # is_out_of_stock
        cond_set_value(product, 'is_out_of_stock', self._parse_is_out_of_stock(data))

        # site_online out of stock
        cond_set_value(product, 'site_online_out_of_stock', self._parse_site_online_out_of_stock(data))

        # specs
        cond_set_value(product, 'specs', self._parse_specs(data))

        # temp price cut
        cond_set_value(product, 'temp_price_cut', self._parse_temp_price_cut(data))

        # title
        cond_set_value(product, 'title', data['title'])

        # brand
        cond_set_value(product, 'brand', self._parse_brand(data))

        # owned
        cond_set_value(product, 'owned', self._parse_owned(product))

        # marketplace bool
        cond_set_value(product, 'marketplace_bool', not product['owned'])

        # upc
        cond_set_value(product, 'upc', data['upc'][-12:])

        # variants
        cond_set_value(product, 'variants', self._parse_variants(data))

        # marketplaces
        if product.get('price_amount'):
            setattr(product,
                    '_sc_marketplace',
                    [
                        {
                            'currency': 'USD',
                            'price': product.get('price_amount'),
                            'name': 'Jet.com',
                            'seller_type': 'site'
                        }
                    ]
                    )

        variant_skus = [
            variant.get('sku')
            for variant in product.get('variants', [])
            if not variant.get('selected')
        ]

        def _make_variant_request(sku, _cb=None):
            _url = self.build_url(u'qwerty', sku)
            request = next(self.make_single_product_requests(_url))
            request.meta.update({'item': product})
            return request.replace(
                callback=_cb or self.parse_variant_details,
                body=json.dumps({
                    'sku': sku,
                    'origination': 'none'
                })
            )

        def _variants_chained_cb(response):
            self.parse_variant_details(response)
            if variant_skus:
                yield _make_variant_request(variant_skus.pop(), _variants_chained_cb)

        if variant_skus:
            yield _make_variant_request(variant_skus.pop(), _cb=_variants_chained_cb)

        # ingredients
        cond_set_value(product, 'ingredients', self._parse_ingredients(data))

        # secondary_id is crawled in shelf
        if self.shelf_url:
            secondary_id = None
            if product.get('variants'):
                for variant in product['variants']:
                    if variant.get('selected'):
                        secondary_id = variant.get('sku')
                        break
            else:
                secondary_id = product.get('site_product_id')

            cond_set_value(product, 'secondary_id', secondary_id)

        yield product

    ############################################
    ############################################

    @staticmethod
    def _parse_dimensions(response):
        product = response.meta.get('item')
        field = response.meta.get('field')
        index = response.meta.get('index')

        product[field][index] = response.headers.get('Content-Length')

        try:
            raw = product.get('_raw_images')[index]
            x500 = product.get('_x500_images')[index]
            if raw == x500:
                product['image_dimensions'][index] = 0
            else:
                product['image_dimensions'][index] = 1
        except Exception:
            pass

        try:
            raw = product.get('_raw_images')[index]
            x1500 = product.get('_x1500_images')[index]
            if raw == x1500:
                product['zoom_image_dimensions'][index] = 0
            else:
                product['zoom_image_dimensions'][index] = 1
        except Exception:
            pass

    @staticmethod
    def _parse_brand(data):
        return data.get('manufacturer')

    @staticmethod
    def _parse_bullets(data):
        if data.get('bullets'):
            return '\n'.join(data.get('bullets'))

    @staticmethod
    def _parse_buyer_reviews(response):
        product = response.meta.get('item')
        cond_set_value(product, 'buyer_reviews', PowerReviews.parse_reviews(response))

    @staticmethod
    def _parse_image_urls(data):
        return [
            i.get('raw')
            for i in data.get('images', [])
        ]

    @staticmethod
    def _parse_owned(product):
        if product.get('brand') and product.get('primary_seller'):
            return product.get('brand').lower() == product.get('primary_seller').lower()
        return False

    @staticmethod
    def _parse_price(data):
        price = data.get('productPrice', {}).get('referencePrice')
        if price:
            return '$' + str(price)

    @staticmethod
    def _parse_price_amount(data):
        return data.get('productPrice', {}).get('referencePrice')

    @staticmethod
    def _parse_was_price(data):
        old_price = data.get('productPrice', {}).get('listPrice')

        if old_price:
            return old_price

    @staticmethod
    def _parse_volume_measure(data):
        return data.get('typeOfUnitForPricePerUnit')

    @staticmethod
    def _parse_price_per_volume(data):
        if JetProductsSpider._parse_volume_measure(data):
            return data.get('productPrice', {}).get('pricePerUnit')

    @staticmethod
    def _parse_promotions(data):
        return bool(JetProductsSpider._parse_was_price(data))

    @staticmethod
    def _parse_site_product_id(url):
        assert isinstance(url, string_types)
        return urlparse(url).path.split('/')[-1]

    @staticmethod
    def _parse_site_online_out_of_stock(data):
        return not bool(data.get('display'))

    @staticmethod
    def _parse_is_out_of_stock(data):
        return bool(data.get('productPrice', {}).get('status'))

    @staticmethod
    def _parse_specs(data):
        return {
            attribute.get('name'): attribute.get('value')
            for attribute in data.get('attributes', [])
            if attribute.get('display')
        } or None

    @staticmethod
    def _parse_temp_price_cut(data):
        return bool(data.get('productPrice', {}).get('listPrice'))

    def parse_variant_details(self, response):
        product = response.meta.get('item')

        try:
            prod_data = json.loads(response.body)['result']
        except Exception:
            self.logger.warning('Error while parsing variants data {}'.format(traceback.format_exc()))
        else:
            for variant in product['variants']:
                if variant.get('sku') == prod_data.get('retailSkuId'):
                    variant['price'] = prod_data.get('productPrice', {}).get('referencePrice')
                    variant["in_stock"] = prod_data.get('addToCart', False)
                    break
            return product

    @staticmethod
    def _parse_variants(data):
        def _parse_variant_data(_data, selected=False):
            return {
                'selected': selected,
                'properties': {
                    variant['type']: variant['value']
                    for variant in _data.get('variantProperties') or _data.get('activeVariantProperties') or [{}]
                    if variant.get('type') and variant.get('value')
                },
                'price': _data.get('productPrice', {}).get('referencePrice'),
                'in_stock': _data.get('addToCart', False),
                'image_url': _data.get('images')[0].get('raw') if _data.get('images') else None,
                'sku': _data.get('retailSkuId')  # include sku
            }

        variants = []

        # parse the main variant
        variant = _parse_variant_data(data, selected=True)
        # malformed variants don't have properties
        if variant.get('properties'):
            variants.append(variant)

        # parse the other variants
        for variant_data in data.get('productVariations', []):
            variant = _parse_variant_data(variant_data)
            # malformed variants don't have properties
            if variant.get('properties'):
                variants.append(variant)

        if len(variants) > 1:
            return variants

    @staticmethod
    def _parse_ingredients(data):
        ingredients = re.search(r'Ingredients:(.*?)\.', data['description'])
        if ingredients:
            ingredients = [x.strip() for x in ingredients.group(1).split(', ')]

        return ingredients if ingredients else None

    def _fill_splash_args(self, request):
        super(JetProductsSpider, self)._fill_splash_args(request)
        request.headers['User-Agent'] = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                         "Chrome/63.0.3239.132 Safari/537.36")
        request.meta['splash']['endpoint'] = 'execute'
        args = get_splash_args(request)
        args['lua_source'] = DEFAULT_MAIN
