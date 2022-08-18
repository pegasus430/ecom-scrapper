import re
import json

from content_analytics.data_parsing.buyer_reviews.bazaarvoice_reviews import BazaarvoiceReviews
from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.middlewares.proxy import ProxyContext
from content_analytics.utils import catch_json_exceptions, replace_http_with_https, catch_dictionary_exception


class DockerscaProductsSpider(BaseProductsSpider):
    name = 'dockers_ca_products'
    allowed_domains = ['www.dockers.com', 'api.bazaarvoice.com']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 '
                      'Safari/537.36 (Content Analytics)'
    }

    SEARCH_URL = "https://www.dockers.com/CA/en_CA/search/{search_term}"

    REVIEWS_URL = "https://api.bazaarvoice.com/data/batch.json?"\
                  "passkey=casXO49OnnLONGhfxN6TSfvEmsGWbyrfjtFtLGZWnBUeE"\
                  "&apiversion=5.5"\
                  "&displaycode=18029-en_ca"\
                  "&resource.q0=products"\
                  "&filter.q0=id%3Aeq%3A{product_id}"\
                  "&stats.q0=reviews"\
                  "&filteredstats.q0=reviews"\
                  "&filter_reviews.q0=contentlocale%3Aeq%3Aen*%2Cen_CA"\
                  "&filter_reviewcomments.q0=contentlocale%3Aeq%3Aen*%2Cen_CA"\
                  "&resource.q1=reviews"

    VARIANTS_DATA_URL = "https://www.dockers.com/CA/en_CA/p/{product_id}/data"
    
    def parse_product(self, response):
        for r in self._parse_product(response):
            yield r

    # ############################
    # Requests generator methods #
    # ############################

    def make_single_product_requests(self, url, *args, **kwargs):
        request = next(super(DockerscaProductsSpider, self).make_single_product_requests(url, *args, **kwargs))
        request = request.replace(url=replace_http_with_https(request.url))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(DockerscaProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    def make_shelf_page_requests(self, url, *args, **kwargs):
        request = next(super(DockerscaProductsSpider, self).make_shelf_page_requests(url, *args, **kwargs))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    ################################
    # Search term abstract methods #
    ################################
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(
            search_term=search_term
        )

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//ul[@class="pagination"]'
            '//li[@class="pagination-next"]'
            '//a[@rel="next"]/@href'
        ).extract_first()

        if next_page:
            return response.urljoin(next_page)

    def parse_search_term_items(self, response):
        links = response.xpath(
            '//div[contains(@class, "product-item")]'
            '//a[contains(@class, "thumb-link")]/@href'
        ).extract()
        return [response.urljoin(link) for link in links]

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath(
            '//div[@class="pagination-bar-results"]/text()'
        ).re(r'\d+')
        return int(total_matches[0]) if total_matches else None

    def parse_search_term_results_per_page(self, response):
        return self.parse_search_term_total_matches(response)

    ################################
    # Shelf pages abstract methods #
    ################################
    def get_shelf_page_next_page(self, response):
        return None

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)

    ###################
    # Parsing methods #
    ###################
    @staticmethod
    @catch_json_exceptions
    def _get_product_json(response):
        """ Get product json
        :param response (HtmlResponse): general url response
        :return (dict) product json data
        """
        product_json = None
        raw_data = re.search(
            r'LSCO.dtos = (.*?)LSCO',
            response.body,
            re.DOTALL
        )
        if raw_data:
            product_json = json.loads(raw_data.group(1))
        return product_json

    @staticmethod
    @catch_json_exceptions
    def _get_json_from_response(response):
        """ Get json data from response
        :param response (HtmlResponse): general url response
        :return (dict) json data
        """
        data = json.loads(response.body_as_unicode())
        return data

    def _parse_product(self, response):
        product = response.meta.get('item')

        product_json = self._get_product_json(response)
        if not product_json:
            yield product

        product_id = self._parse_product_id(product['url'], product_json)
        departments = self._parse_departments(response)
        image_urls, image_alts = self._parse_images(response)

        product.update({
            # Main data
            'title': self._parse_title(response),
            'brand': 'Dockers',

            # CH data
            'features': self._parse_features(response),
            'image_urls': image_urls,
            'short_description': self._parse_short_description(product_json),
            'departments': departments,

            # SC data
            'image_url': image_urls[0] if image_urls else None,
            'image_alts': image_alts,
            'department': departments[-1] if departments else None,

            # Availability
            'in_stores': False,
            'site_online': True,
            'site_online_out_of_stock': self._parse_site_online_out_of_stock(response),

            # Price
            'price_amount': self._parse_price_amount(product_json),
            'price_currency': self._parse_price_currency(product_json),

            # Codes parsing
            'product_id': product_id,
            'reseller_id': product_id,
            'site_product_id': product_id
        })

        # variants
        variants_data = self._parse_variant_ids(product_json, product.get('price_amount'))
        chosen_variant = [x for x in variants_data if x['id'] == product['reseller_id']]
        if variants_data:
            yield MergeRequest(
                url=self.VARIANTS_DATA_URL.format(
                    product_id=chosen_variant[0]['id'] if chosen_variant else variants_data[0]['id']
                ),
                item=product,
                meta={
                    'variants_data': variants_data
                },
                callback=self._parse_variants
            )

        # buyer_reviews
        yield MergeRequest(
            url=BazaarvoiceReviews.compile_url(
                product_id=product_id,
                passkey='casXO49OnnLONGhfxN6TSfvEmsGWbyrfjtFtLGZWnBUeE',
                displaycode='18029-en_ca'
            ),
            item=product,
            callback=self._on_reviews_response
        )

        yield product

    @staticmethod
    @catch_dictionary_exception
    def _parse_short_description(product_json):
        """ Get short_description
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `short_description`
        """
        return product_json['product']['description']

    @staticmethod
    def _parse_product_id(product_url, product_json):
        """ Get product_id
        :param product_url (str): product url
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `product_id`
        """
        product_id = re.search(r'/p/(\d+)', product_url)
        return product_id.group(1) if product_id else product_json.get('code')

    @staticmethod
    def _parse_departments(response):
        """ Get departments
        :param response (HtmlResponse): general url response
        :return (list) product `departments`
        """
        departments = response.xpath(
            '//ol[@class="breadcrumb"]//li/a/text()'
        ).extract()
        return departments if departments else None

    @staticmethod
    def _parse_title(response):
        title = response.xpath(
            '//div[contains(@class, "-title")]/*[@itemprop="name"]/text()').extract_first()
        return title

    @staticmethod
    @catch_dictionary_exception
    def _parse_price_amount(product_json):
        """ Get price amount
        :param price (str): price from method `_parse_price`
        :return (float) product `price amount`
        """
        price_amount = product_json['product']['price']['regularPrice']
        return float(price_amount) if price_amount else None

    @staticmethod
    @catch_dictionary_exception
    def _parse_price_currency(product_json):
        """ Get price currency
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `price currency`
        """
        return product_json['product']['price']['currencyIso']

    @staticmethod
    def _parse_images(response):
        """ Get image urls and alts
        :param response (HtmlResponse): general url response
        :return (tuple) product `image_urls` and `image_alts`
        """
        urls = []
        alts = []
        images = response.xpath(
            '//picture//img'
        )
        for image in images:
            url = image.xpath('./@data-src').extract_first()
            if url and url.split('?'):
                urls.append(url.split('?')[0])
                alts.append(image.xpath('./@alt').extract_first())
        return urls, alts

    @staticmethod
    def _parse_features(response):
        """ Get features
        :param response (HtmlResponse): general url response
        :return (list) product `features`
        """
        features = response.xpath(
            '//div[contains(@class, "pdp-spec-feature-list")]//li/text()'
        ).extract()
        return features if features else None

    @staticmethod
    def _parse_site_online_out_of_stock(response):
        """ Get site online outt of stock
        :param response (HtmlResponse): general url response
        :return (bool) product `site_online_out_of_stock`
        """
        return bool(response.xpath(
            '//button[contains(text(), "Out")]'
        ))

    @staticmethod
    def _parse_variant_ids(product_json, price_amount):
        """Get product top-level variants 
        :param variant_json (dict): product json from method `_get_product_json`
        :return (dict): dictionary of top-level variants data
        """
        return [{
            'colorName': x.get('colorName'),
            'id': x.get('code'),
            'url': x.get('url'),
            'active': x.get('active'),
            'price': price_amount
        } for x in product_json.get('swatches', [])]

    def _parse_variants(self, response):
        """Get product variants (full data)
        :param response (HtmlResponse): general url response
        :return (product): product `variants`
        """
        meta = response.meta.copy()
        product = response.meta.get('item')
        size_data = self._get_json_from_response(response)
        variants_data = meta.get('variants_data')
        variant_data = None
        variants = meta.get('variants', [])
        for i, x in enumerate(variants_data):
            if x['id'] == size_data.get('code'):
                variant_data = variants_data.pop(i)
        if variant_data:
            for data in size_data.get('variantOptions', []):
                variants.append({
                    'in_stock': bool(data.get('stock', {}).get('stockLevel')),
                    'colorid': variant_data.get('id'),
                    'price': variant_data.get('price'),
                    'properties': {
                        'color': variant_data.get('colorName'),
                        'size': data.get('displaySizeDescription')
                    },
                    'selected': variant_data.get('active'),
                    'url': response.urljoin('/CA/en_CA' + variant_data.get('url'))
                })
            product.update({'variants': variants})

        return product

    @staticmethod
    def _on_reviews_response(response):
        product = response.meta.get('item')
        product.update({'buyer_reviews': BazaarvoiceReviews.parse_reviews(response.body)})
