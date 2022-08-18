# -*- coding: utf-8 -*-
import json

from scrapy import Request
from six.moves import urllib

from content_analytics.data_parsing.buyer_reviews.turnto_reviews import TurntoReviews
from . import BaseProductsSpider, MergeRequest, cond_set_value
from ..utils import catch_dictionary_exception, catch_json_exceptions


class VitaminshoppeProductsSpider(BaseProductsSpider):
    name = 'vitaminshoppe_products'
    allowed_domains = ['vitaminshoppe.com', 'static.www.turnto.com']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }

    PRODUCT_URL = 'https://content.vitaminshoppe.com/pdp?skuId={sku}'

    SEARCH_URL = 'https://www.vitaminshoppe.com/search?search={search_term}&format=json&rpp={page_size}&page={current_page}'

    SHELF_URL = '{path}?format=json&rpp={page_size}&page={current_page}'

    PRODS_PER_PAGE = 48

    def parse_product(self, response):
        product = response.meta.get('item')

        data = self._parse_main_json(response)

        if data and data.get('RESPONSE_CODE', '') == 'PRODUCT_FOUND':
            product_details = data.get('sku', {})

            product.update(
                {
                    'title': self._parse_title(product_details),
                    'brand': self._parse_brand(product_details),
                    'image_url': self._parse_image_url(product_details),
                    'sku': self._parse_sku(product_details),
                    'reseller_id': self._parse_reseller_id(product_details),

                    'short_description': self._parse_description(product_details),
                    'long_description': self._parse_long_description(product_details),
                    'directions': self._parse_directions(product_details),
                    'warnings': self._parse_warnings(product_details),

                    'departments': self._parse_departments(product_details),
                    'variants': self._parse_variants(product_details),

                    'nutrition_fact_count': self._parse_nutrition_fact_count(product_details),
                    'ingredients': self._parse_ingredients(product_details),
                    'specs': self._parse_specs(product_details),

                    'price_amount': self._parse_price_amount(product_details),
                    'price_currency': 'USD',

                    'is_out_of_stock': False,
                    'in_stores': True,
                    'site_online': True,
                }
            )

            sku = product_details.get('skuId')
            if sku:
                yield MergeRequest(
                    url=TurntoReviews.compile_url(
                        sku=sku,
                        site_id='I1h5bZdr3OSP4fMsite',
                    ),
                    item=product,
                    callback=self._on_reviews_response
                )
            if product['variants']:
                for variant in product.get('variants', []):
                    if not variant['selected'] and variant['sku_id']:
                        yield Request(
                            url=self.PRODUCT_URL.format(sku=variant['sku_id']),
                            callback=self.parse_extra_variants,
                            meta={'product': product},
                            dont_filter=True,
                        )

        else:
            product['not_found'] = True
            product['no_longer_available'] = True
        yield product

    # Extract json block
    @catch_json_exceptions
    def _parse_main_json(self, response):
        return json.loads(response.body_as_unicode())

    # Parse fields from json block above
    @catch_dictionary_exception
    def _parse_title(self, data):
        return data.get('lgDisplayName')

    @catch_dictionary_exception
    def _parse_brand(self, data):
        brand_dict = data.get('brand', {})
        if brand_dict:
            brand_info_dict = brand_dict.get('brand', {})
            if brand_info_dict:
                return brand_info_dict.get('brandDisplayName')

    @catch_dictionary_exception
    def _parse_departments(self, data):
        breadcrumb_list = data.get('breadCrumbs', [])
        if breadcrumb_list:
            return [breadcrumb.get('label') for breadcrumb in breadcrumb_list
                if breadcrumb.get('label')]

    @catch_dictionary_exception
    def _parse_variants(self, data):
        variant_list = data.get('varintSkus', {}).get('varintSkussku', [])
        variants = []
        variant = {
            'price': self._parse_price_amount(data),
            'properties': {
                'option_name': data.get('variantSkuDisplayName'),
            },
            'image_url': self._parse_image_url(data),
            'sku_id': self._parse_sku(data),
            'in_stock': True,
            'selected': True
        }
        variants.append(variant)
        if variant_list:
            for _variant in variant_list:
                sku_id = _variant.get('variantSkuId')
                if sku_id:
                    variant = {
                        'properties': {
                            'option_name': _variant.get('variantSkuDisplayName'),
                        },
                        'sku_id': sku_id,
                        'in_stock': True,
                        'selected': False
                    }
                    variants.append(variant)
        return variants if variants else None

    @catch_dictionary_exception
    def parse_extra_variants(self, response):
        product = response.meta.get('product')

        data = self._parse_main_json(response)
        product_details = data.get('sku', {})

        price = self._parse_price_amount(product_details)
        image_url = self._parse_image_url(product_details)
        sku = self._parse_sku(product_details)

        for variant in product['variants']:
            if not variant['selected'] and variant['sku_id'] == sku:
                variant['price'] = price
                variant['image_url'] = image_url

        # return product

    @catch_dictionary_exception
    def _parse_nutrition_fact_count(self, data):
        nutrition_dict = data.get('skuSupplmnts') or {}
        nutrition_list = nutrition_dict.get('supplementFacts', [])
        return len(nutrition_list)

    @catch_dictionary_exception
    def _parse_ingredients(self, data):
        ingredients_str = data.get('othIngredients', '')
        if ingredients_str:
            return [i.strip() for i in ingredients_str.split(',') if i.strip()]

    @catch_dictionary_exception
    def _parse_specs(self, data):
        return {
            'Form': data['form'],
            'Strength': data['skuStrength'],
            'Serving Size': data['servngSize'],
            'Number of Servings': data['numOfServings'],
            'Price per Serving:': data['prcPerServing'],
            'Product Weight': '{weight} {type}'.format(weight=data['weight'], type=data['weightType'])
        }

    @catch_dictionary_exception
    def _parse_description(self, data):
        return data['description']

    @catch_dictionary_exception
    def _parse_long_description(self, data):
        return data['labelDescrption']

    @catch_dictionary_exception
    def _parse_directions(self, data):
        return data['direction']

    @catch_dictionary_exception
    def _parse_warnings(self, data):
        return data['warningMsg']

    @catch_dictionary_exception
    def _parse_image_url(self, data):
        return data['thumbnailImage']

    @catch_dictionary_exception
    def _parse_sku(self, data):
        return data['skuId']

    @catch_dictionary_exception
    def _parse_reseller_id(self, data):
        return data['jdaSkId']

    @catch_dictionary_exception
    def _parse_price_amount(self, data):
        return float(data.get('skuActivePrice', '0'))

    # ############################
    # Requests generator methods #
    # ############################

    def make_single_product_requests(self, url, *args, **kwargs):
        request = next(super(VitaminshoppeProductsSpider, self).make_single_product_requests(url, *args, **kwargs))
        sku = urllib.parse.urlparse(url).path.split('/')[-1]
        yield request.replace(url=self.PRODUCT_URL.format(sku=sku.upper()))

    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(VitaminshoppeProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        yield request.replace(callback=self._search_url_help)

    def make_shelf_page_requests(self, url, *args, **kwargs):
        request = next(super(VitaminshoppeProductsSpider, self).make_shelf_page_requests(url, *args, **kwargs))
        yield request.replace(url=self.SHELF_URL.format(path=url, page_size=self.PRODS_PER_PAGE, current_page=1))

    ################################
    # Search term abstract methods #
    ################################

    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term, page_size=self.PRODS_PER_PAGE, current_page=1)

    @catch_json_exceptions
    def parse_search_term_items(self, response):
        json_response = json.loads(response.body)
        data = json_response.get('contents', [])[0].get('contentList', [])[7]
        if data:
            products = data.get('PLPProductsList')
            for product in products:
                prod_url = product.get('pdpUrl')
                if not prod_url:
                    continue
                url = response.urljoin(prod_url)
                yield url

    def _search_url_help(self, response):
        data = self._parse_main_json(response)
        search_redirect_url = data.get('endeca:redirect', {}).get('link', {}).get('url')
        if search_redirect_url:
            search_redirect_url = self.SHELF_URL.format(
                path=response.urljoin(urllib.parse.urlparse(search_redirect_url).path),
                page_size=self.PRODS_PER_PAGE,
                current_page=1
            )
            return response.request.replace(
                url=search_redirect_url,
                dont_filter=True,
                callback=self.process_search_terms_response
            )
        return self.process_search_terms_response(response)

    def get_search_term_next_page(self, response):
        current_page = response.meta.get('current_page', 1)
        total_matches = self.parse_search_term_total_matches(response)
        if current_page * self.PRODS_PER_PAGE < total_matches:
            current_page += 1
            if not '/search?search=' in response.url:
                return self.get_shelf_page_next_page(response)

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

    @catch_json_exceptions
    def parse_search_term_total_matches(self, response):
        json_response = json.loads(response.body)
        total_match = json_response.get('seoParams', {}).get('totalNumRecords')

        return total_match if total_match else None

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
            url = self.SHELF_URL.format(
                path=response.urljoin(urllib.parse.urlparse(response.url).path),
                page_size=self.PRODS_PER_PAGE,
                current_page=current_page)
            meta = response.meta
            meta['current_page'] = current_page
            return Request(
                url,
                meta=meta,
                callback=self.process_search_terms_response,
            )

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)

    def _on_reviews_response(self, response):
        product = response.meta.get('item')
        cond_set_value(product, 'buyer_reviews', TurntoReviews.parse_reviews(response))
