# -*- coding: utf-8 -*-
import re
import json
import traceback

from scrapy import Request
from HTMLParser import HTMLParser

from . import BaseProductsSpider, MergeRequest
from ..items import BuyerReviews
from ..utils import catch_dictionary_exception, catch_json_exceptions


class ThrivemarketProductsSpider(BaseProductsSpider):
    name = 'thrivemarket_products'
    allowed_domains = ['thrivemarket.com']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36'
    }

    SEARCH_URL = 'https://thrivemarket.com/api/v1/products?reqsource=web&page_size={page_size}' \
                 '&cur_page={current_page}&filter[search]={search_term}'

    SHELF_URL = 'https://thrivemarket.com/api/v1/products?reqsource=web&filter[categories]={c_id}' \
                '&sort[position_category_{c_id}]=asc&cur_page={current_page}&page_size={page_size}'

    REVIEW_URL = 'https://thrivemarket.com/api/v1/product/{product_id}/powerreviews'

    PRODS_PER_PAGE = 60

    def parse_product(self, response):
        product = response.meta.get('item')

        data = self._parse_main_inline_json(response)
        if data:
            product_details = data.get('productDetails', {}).get('data')

            product.update(
                {
                    'title': self._parse_title(product_details),
                    'short_description': self._parse_description(product_details),
                    'long_description': self._parse_long_description(product_details),
                    'image_urls': self._parse_image_urls(product_details),
                    'nutrition_fact_count': self._parse_nutrition_fact_count(product_details),
                    'ingredients': self._parse_ingredients(response),
                    'price_amount': self._parse_price_amount(product_details),
                    'price_currency': self._parse_price_currency(product_details),
                    'was_price': self._parse_was_price(product_details),
                    'now_price': self._parse_price_amount(product_details),
                    'brand': self._parse_brand(product_details),
                    'departments': self._parse_departments(data),
                    'sku': self._parse_sku(product_details),
                    'reseller_id': self._parse_sku(product_details),
                    'is_out_of_stock': self._parse_is_out_of_stock(product_details),
                    'in_stores': True,
                    'site_online': self._parse_site_online(product_details),
                    'site_online_out_of_stock': self._parse_is_out_of_stock(product_details)
                }
            )

            product_id = data.get('productDetails', {}).get('data', {}).get('id')
            if product_id:
                yield MergeRequest(
                    self.REVIEW_URL.format(
                        product_id=product_id
                    ),
                    item=product,
                    callback=self.parse_buyer_reviews
                )
            else:
                yield product
        else:
            product.update({
                'not_found': True
            })
            self.logger.info('Can not find the product')
            yield product

    # Extract json block
    @catch_json_exceptions
    def _parse_main_inline_json(self, response):
        data = re.search(r'window\.__INITIAL_STATE__ = (.*\})', response.body)
        if data:
            return json.loads(data.group(1))

    # Parse fields from json block above
    @catch_dictionary_exception
    def _parse_title(self, data):
        return HTMLParser().unescape(data.get('meta', {}).get('sailthru', {}).get('title'))

    @catch_dictionary_exception
    def _parse_brand(self, data):
        return data.get('meta', {}).get('sailthru', {}).get('product.brand')

    @catch_dictionary_exception
    def _parse_departments(self, data):
        return [breadcrumb['label'] for breadcrumb in data.get('breadcrumbs', {}).get('breadcrumbs')
                if breadcrumb.get('label')][1:]

    @catch_dictionary_exception
    def parse_buyer_reviews(self, response):
        product = response.meta.get('item')
        try:
            reviews = json.loads(response.body)

            buyer_reviews = BuyerReviews(
                stars={5 - idx: int(star) for idx, star in enumerate(reviews['rating_histogram'])},
                average=reviews['average_rating'],
                count=reviews['review_count']
            )
            product['buyer_reviews'] = buyer_reviews
        except:
            self.logger.info('Empty buyer_reviews')

    @catch_dictionary_exception
    def _parse_nutrition_fact_count(self, data):
        return len(data.get('nutrition', {}).get('rows')) if not data.get('nutrition') == [] else None

    @staticmethod
    def _parse_ingredients(response):
        ingredients = response.xpath('//p[@class="product-detail__text-ingredients-copy"]/text()').extract()
        if ingredients:
            ingredients = ingredients[0].replace('(', ',').replace(')', ',').split(',')
            return [
                i.strip()
                for i in ingredients
                if i.strip()
                ]

    @catch_dictionary_exception
    def _parse_description(self, data):
        return data.get('description')

    @catch_dictionary_exception
    def _parse_long_description(self, data):
        return data.get('manufacturer_content_html')

    @catch_dictionary_exception
    def _parse_image_urls(self, data):
        return data.get('gallery', [])

    @catch_dictionary_exception
    def _parse_is_out_of_stock(self, data):
        return not data.get('in_stock')

    @staticmethod
    def _parse_site_online(data):
        return data.get('availableOnline')

    @catch_dictionary_exception
    def _parse_sku(self, data):
        return data.get('sku')

    @catch_dictionary_exception
    def _parse_price_amount(self, data):
        return float(data.get('meta', {}).get('og', {}).get('price:amount'))

    @catch_dictionary_exception
    def _parse_was_price(self, data):
        return data.get('msrp')

    @catch_dictionary_exception
    def _parse_price_currency(self, data):
        return data.get('meta', {}).get('og', {}).get('price:currency')

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term,
                                      page_size=self.PRODS_PER_PAGE,
                                      current_page=1)

    def parse_search_term_items(self, response):
        try:
            data = json.loads(response.body)
            if data:
                products = data.get('products')
                for product in products:
                    yield product.get('url')
        except:
            self.logger.info('Can not load json from response: {}'.format(traceback.format_exc()))

    def get_search_term_next_page(self, response):
        current_page = response.meta.get('current_page', 1)
        total_matches = self.parse_search_term_total_matches(response)
        if current_page * self.PRODS_PER_PAGE < total_matches:
            current_page += 1
            url = self.SEARCH_URL.format(
                search_term=response.meta.get('search_term'),
                page_size=self.PRODS_PER_PAGE,
                current_page=current_page)
            meta = response.meta
            meta['current_page'] = current_page
            return Request(
                url,
                meta=meta,
                callback=self.process_search_terms_response,
            )

    def parse_search_term_total_matches(self, response):
        try:
            json_response = json.loads(response.body)
            return json_response.get('total') if json_response else None
        except:
            self.logger.info('Can not load json from response: {}'.format(traceback.format_exc()))

    def parse_search_term_results_per_page(self, response):
        pass

    # Shelf component
    def parse_shelf_page_items(self, response):
        data = self._parse_main_inline_json(response)
        if data:
            products = data.get('product', {}).get('products', {}).get('byId')
            if products:
                for product in products.values():
                    yield product.get('url')


    def get_shelf_page_next_page(self, response):
        current_page = response.meta.get('current_page', 1)
        category_id = response.meta.get('category_id')
        total_matches = self.parse_shelf_page_total_matches(response)
        if current_page * self.PRODS_PER_PAGE < total_matches:
            current_page += 1
            url = self.SHELF_URL.format(
                search_term=response.meta.get('search_term'),
                page_size=self.PRODS_PER_PAGE,
                c_id=category_id,
                current_page=current_page)
            meta = response.meta
            meta['current_page'] = current_page
            return url

    def parse_shelf_page_total_matches(self, response):
        data = self._parse_main_inline_json(response)
        if data:
            products_url = data.get('category', {}).get('products_url')
            if products_url:
                category_id = re.search(r'\d+', products_url)
                if category_id:
                    meta = response.meta
                    meta['category_id'] = category_id.group(0)
            return data.get('product', {}).get('total')
        return 0

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
