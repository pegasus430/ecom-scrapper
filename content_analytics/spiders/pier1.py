import re
import json
import urlparse
import math
from datetime import datetime
from scrapy import Request

from . import BaseProductsSpider, MergeRequest
from ..items import BuyerReviews
from ..utils import catch_dictionary_exception, catch_json_exceptions


class Pier1ProductsSpider(BaseProductsSpider):
    name = "pier1_products"
    allowed_domains = ['pier1.com']

    # Urls
    SEARCH_URL = 'https://www.pier1.com/on/demandware.store/Sites-pier1_us-Site/default/Search-Show?' \
                 'q=furniture&start={offset}&sz=60&format=ajax'

    PRICE_URL = 'https://www.pier1.com/api/1.0/carousel/prices?skus={skus}'

    REVIEW_URL = 'https://www.pier1.com/on/demandware.store/Sites-pier1_us-Site/default/Reviews-Get?' \
                 'pid={pid}&' \
                 'limit=1000&' \
                 'offset={offset}'

    AVAILABILITY_URL = 'https://www.pier1.com/on/demandware.store/Sites-pier1_us-Site/default/' \
                       'Availability-DeliveryOptions?sku={sku}'

    PRODS_PER_PAGE = 60

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }

    # Single component
    def parse_product(self, response):
        product = response.meta.get('item')

        # Embedded json
        data = self._parse_main_inline_json(response)
        if data:

            sku = self._parse_sku(data)

            product.update(
                {
                    # General fields
                    'title': self._parse_title(data.get('products', {}).get(sku, {})) if sku else None,
                    'short_description': self._parse_description(data),
                    'long_description': self._parse_long_description(data),
                    'image_urls': self._parse_image_urls(data),
                    'bundle': self._parse_bundle(data),

                    # CH fields
                    'features': self._parse_features(response),

                    # Price
                    'price': self._parse_price(data),
                    'price_amount': self._parse_price_amount(data),
                    'price_currency': self._parse_price_currency(response),
                    'was_price': self._parse_was_price(data),
                    'now_price': self._parse_price_amount(data),
                    'temp_price_cut': self._parse_temp_price_cut(data),

                    # Categorization
                    'brand': self._parse_brand(response),
                    'departments': self._parse_departments(response),

                    # Identification
                    'sku': sku,
                    'site_product_id': self._parse_site_product_id(response.url),

                    # Availability
                    'in_stores': True,
                    'site_online': self._parse_site_online(data),
                }
            )

            yield MergeRequest(
                self.AVAILABILITY_URL.format(sku=product.get('sku')),
                item=product,
                callback=self._parse_is_out_of_stock,
            )

            variant_requests = self._get_variant_requests(product, data)
            for req in variant_requests:
                yield req

            reviews_requests = self._parse_buyer_reviews(response, data)
            for req in reviews_requests:
                yield req
        else:
            product.update({
                'not_found': True
            })
            self.log('Can not find the product')
            yield product

    @catch_json_exceptions
    def parse_site_online_out_of_stock(self, response):
        product = response.meta.get('item')
        data = json.loads(response.body_as_unicode())
        stock_data = data['responseInfos'][0]
        product['site_online_out_of_stock'] = not(stock_data['shippingEligible'])

    # Parsing inline product json blocs
    @catch_json_exceptions
    def _parse_main_inline_json(self, response):
        data = re.search(r'var productAttributes = ({.*?});', response.body)
        return json.loads(data.group(1)) if data else None

    @catch_json_exceptions
    def _parse_json_from_response(self, response):
        return json.loads(response.body)

    @catch_dictionary_exception
    def _get_variant_requests(self, product, data):
        if data.get('isVariant') and data.get('reviewSkus'):
            for variant_sku in data['reviewSkus']:
                yield MergeRequest(
                    self.AVAILABILITY_URL.format(sku=variant_sku),
                    item=product,
                    callback=self._parse_variant,
                    meta={
                        'product_json': data,
                        'current_sku': variant_sku
                    }
                )

    def _parse_is_out_of_stock(self, response):
        product = response.meta.get('item')
        availability_data = self._parse_json_from_response(response)
        product.update({'is_out_of_stock': self._get_is_out_of_stock(availability_data)})

    @catch_json_exceptions
    def _parse_variant(self, response):
        product = response.meta.get('item')
        data = response.meta.get('product_json')
        variant_sku = response.meta.get('current_sku')
        availability_data = self._parse_json_from_response(response)
        for swatch in data.get('products', {}).get(product.get('sku')).get('variations', []):
            for attrs in swatch.get('attributes', []):
                if variant_sku in attrs.get('swatchImg', []):
                    product.setdefault('variants', []).append({
                        'selected': attrs.get('selected'),
                        'img_url': attrs.get('swatchImg'),
                        'price': self._parse_variant_price(availability_data),
                        'in_stock': not self._get_is_out_of_stock(availability_data),
                        'properties': {
                            swatch.get('displayName').lower(): attrs.get('displayValue')
                        },
                        'sku_id': variant_sku
                    })

    @staticmethod
    @catch_json_exceptions
    def _parse_variant_price(availability_data):
        price = re.search(r'\d{1,3}[,\.\d{3}]*\.?\d*', availability_data.get('products', [{}])[0].get('productPrice'))
        if price:
            return float(price.group())

    @staticmethod
    @catch_dictionary_exception
    def _get_is_out_of_stock(availability_data):
        """
        CON-44219, availability algorithm was taken from `availability.js`
        :param availability_data: dict, availability json data for specified sku
        :return: bool, `is_out_od_stock`
        """
        if isinstance(availability_data, dict):
            product_data = next(iter(availability_data.get('products') or []), {})
            onlineOnly = product_data.get('pickup', {}).get('status') == 'onlineOnly'
            notAvailable = product_data.get('omnAvail', {}).get('pickup') in ['NO', 'NA'] and \
                           product_data.get('omnAvail', {}).get('home') in ['NO', 'NA'] and \
                           product_data.get('omnAvail', {}).get('direct') == 'NO'
            return bool(product_data.get('isAddToBasketDisabledFromServer') or (onlineOnly and notAvailable))
        return False

    @catch_json_exceptions
    def _parse_features(self, response):
        features = response.xpath(
            '//ul[@class="tab-detail-list"]/li/text()'
        ).extract()
        return [i.strip() for i in features if i.strip()] if features else None

    # Parsing json data
    @catch_dictionary_exception
    def _parse_title(self, data):
        return data.get('title', {}).get('productName')

    @staticmethod
    def _parse_brand(response):
        return response.xpath('//meta[@itemprop="brand"]/@content').extract_first()

    @staticmethod
    def _parse_departments(response):
        return response.xpath('//li[@itemprop="itemListElement" and not(@class)]'
                              '//span[@itemprop="name"]/text()').extract()

    @catch_dictionary_exception
    def _parse_buyer_reviews(self, response, data=None):
        prod = response.meta.get('item')
        buyer_reviews = prod.get(
            'buyer_reviews',
            BuyerReviews(**{
                'average': 0,
                'count': 0,
                'stars': {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            })
        )
        offset = 8
        pid = ''
        if data:
            buyer_reviews['average'] = data.get('avgRating')
            count = data.get('reviewsCount')
            buyer_reviews['count'] = count
            reviews = data.get('reviews', [])
            if reviews:
                for i in reviews:
                    if i.get('reviewedDate'):
                        review_date = i['reviewedDate']
                        buyer_reviews['last_review_date'] = datetime.strptime(
                            review_date.split('T')[0], '%Y-%m-%d'
                        ).date()
                pid = ','.join(data.get('reviewSkus', []))
            else:
                return
        else:
            data = self._parse_json_from_response(response) or {}
            data = data.get('reviews', {})
            offset = data.get('offset', offset) + 1000
            count = data.get('total', 0)
            reviews = data.get('results', [])

        if reviews:
            for review in reviews:
                buyer_reviews['stars'][review.get('rating')] += 1

        prod['buyer_reviews'] = buyer_reviews
        if offset < count and data:
            # we can skip first 8 reviews as these are existing in first response
            for i in range(0, int(math.ceil(float(count - 8) / 1000))):
                yield MergeRequest(
                    url=self.REVIEW_URL.format(offset=(8 + 1000*i), pid=pid),
                    callback=self._parse_buyer_reviews,
                    item=prod
                )

    @catch_dictionary_exception
    def _parse_description(self, data):
        if not data.get('isProductSet'):
            return data.get('descriptionPreview')

    @catch_dictionary_exception
    def _parse_long_description(self, data):
        return data.get('longDescription')

    @catch_dictionary_exception
    def _parse_image_urls(self, data):
        return [
            image.get('URL')
            for image in data.get('images', {}).get('pdpLarge', [])
            if image.get('URL')
        ]

    @catch_dictionary_exception
    def _parse_bundle(self, data):
        return data.get('isProductSet')

    @staticmethod
    def _parse_site_online(data):
        return data.get('availableOnline')

    # Codes parsing
    @catch_dictionary_exception
    def _parse_sku(self, data):
        return data.get('imaxSku')

    @staticmethod
    def _parse_site_product_id(url):
        site_product_id = re.search(r'(.*?)\.html', urlparse.urlparse(url).path.split('/')[-1])
        return site_product_id.group(1) if site_product_id else None

    # Price parsing
    @catch_dictionary_exception
    def _parse_price(self, data):
        if data.get('pricing', {}).get('isPriceRange'):
            return '-'.join([
                data.get('pricing', {}).get('salesMinPrice', {}).get('formatted', ''),
                data.get('pricing', {}).get('salesMaxPrice', {}).get('formatted', '')
            ])
        return data.get('pricing', {}).get('salesPrice', {}).get('value')

    @catch_dictionary_exception
    def _parse_price_amount(self, data):
        if data.get('pricing', {}).get('isPriceRange'):
            return data.get('pricing', {}).get('salesMinPrice', {}).get('formatted', '')
        return data.get('pricing', {}).get('salesPrice', {}).get('value')

    def _parse_temp_price_cut(self, data):
        if self._parse_was_price(data) and self._parse_price_amount(data):
            return False
        return True

    @catch_dictionary_exception
    def _parse_was_price(self, data):
        if data.get('price', {}).get('isPriceRange'):
            return '-'.join([
                data.get('pricing', {}).get('standardMinPrice', {}).get('formatted', ''),
                data.get('pricing', {}).get('standardMaxPrice', {}).get('formatted', '')
            ])
        return data.get('pricing', {}).get('standardPrice', {}).get('value')

    @staticmethod
    def _parse_price_currency(response):
        price_currency = response.xpath('//meta[@itemprop="priceCurrency"]/@content').extract()
        return price_currency[0] if price_currency else 'USD'

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term, offset=0)

    def parse_search_term_items(self, response):
        links = response.xpath('//a[@class="thumb-link item-link"]/@href').extract()
        for link in links:
            yield urlparse.urljoin(response.url, link)

    def get_search_term_next_page(self, response):
        current_page = response.meta.get('current_page', 1)
        total_matches = self.parse_search_term_total_matches(response)
        if current_page * self.PRODS_PER_PAGE < total_matches:
            current_page += 1
            url = self.SEARCH_URL.format(
                search_term=response.meta.get('search_term'),
                offset=current_page * self.PRODS_PER_PAGE
            )
            meta = response.meta
            meta['current_page'] = current_page
            return Request(
                url,
                meta=meta,
                callback=self.process_search_terms_response,
            )

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//span[@class="lm-itm-count"]/text()').re(r'\d+')
        if total_matches:
            return int(total_matches[0])

    def parse_search_term_results_per_page(self, response):
        pass

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        current_page = response.meta.get('current_page', 1)
        total_matches = self.parse_shelf_page_total_matches(response)
        if current_page * self.PRODS_PER_PAGE < total_matches:
            current_page += 1
            url = urlparse.urlparse(response.url).path + '?sz=60&start={}'.format(current_page * self.PRODS_PER_PAGE)
            response.meta['current_page'] = current_page
            return url

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
