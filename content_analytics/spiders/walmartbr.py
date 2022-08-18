# -*- coding: utf-8 -*-
import re
import json
import datetime

from scrapy.utils.markup import remove_tags, replace_entities

from content_analytics.items import BuyerReviews
from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.utils import catch_json_exceptions, cond_set_value

class WalmartBrProductsSpider(BaseProductsSpider):
    name = 'walmart_br_products'

    allowed_domains = ['walmart.com.br']

    SEARCH_URL = "https://www.walmart.com.br/busca/_search?ft={search_term}" \
                 "&PS={results_count}&target=Search&PageNumber={page}"

    REVIEWS_URL = "https://www.walmart.com.br/xhr/reviews/{site_product_id}/?pageNumber={page_num}"

    MARKETPLACE_URL = "https://www2.walmart.com.br/checkout/services/simulation?" \
                      "postalCode={zip_code}&sku={sku}&_=1522879177357"

    DEFAULT_PER_PAGE = 20

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
    }

    def __init__(self, *args, **kwargs):
        super(WalmartBrProductsSpider, self).__init__(*args, **kwargs)
        self.zip_code = kwargs.get('zip_code', '70000000')

    ############################
    # General abstract methods #
    ############################
    def parse_product(self, response):
        product = response.meta.get('item')

        product_json = self._get_product_json(response)
        if not product_json:
            self.logger.warning('Can\'t find product json data for url {}'.format(self.product_url))
            yield product
        else:
            departments = self._parse_departments(response)
            image_urls, image_alts, image_dimensions, zoom_image_dimensions = self._parse_images_data(response)
            sku = self._parse_sku(product_json)

            product.update({
                # Main data
                'brand': self._parse_brand(product_json),
                'title': self._parse_title(product_json),

                # CH data
                'departments': departments if departments else None,
                'department': departments[0] if departments else None,
                'specs': self._parse_specs(response),
                'image_urls': image_urls,
                'image_alts': image_alts,
                'image_dimensions': image_dimensions,
                'zoom_image_dimensions': zoom_image_dimensions,
                'short_description': self._parse_short_description(product_json),
                'video_urls': self._parse_video_urls(response),

                # SC data
                'image_url': image_urls[0] if image_urls else None,

                # Availability
                'site_online': True,
                'is_out_of_stock': self._parse_is_out_of_stock(product_json),
                'site_online_out_of_stock': self._parse_is_out_of_stock(product_json),

                # Price
                'price_amount': self._parse_price_amount(product_json),
                'price_currency': 'BRL',
                'special_pricing': self._parse_special_pricing(product_json, response),
                'was_price': self._parse_special_pricing(product_json, response),
                'now_price': self._parse_price_amount(product_json),

                # Codes parsing
                'sku': sku,
                'primary_seller': self._parse_primary_seller(product_json),
                'site_product_id': self._parse_site_product_id(product['url'])
            })

            # marketplaces
            if sku:
                yield MergeRequest(
                    url=self.MARKETPLACE_URL.format(
                        zip_code=self.zip_code,
                        sku=sku
                    ),
                    item=product,
                    callback=self._parse_marketplaces
                )

            # buyer_reviews
            if product.get('site_product_id'):
                yield MergeRequest(
                    url=self.REVIEWS_URL.format(
                        site_product_id=product.get('site_product_id'),
                        page_num=1
                    ),
                    meta={
                        'reviews_page': 1,
                        'site_product_id': product.get('site_product_id')
                    },
                    item=product,
                    callback=self._get_reviews_data,
                )

            yield product

    ################################
    # Search term abstract methods #
    ################################
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(
            search_term=search_term,
            results_count=self.DEFAULT_PER_PAGE,
            page=1
        )

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//a[contains(@class, "shelf-view-more next")]/@href'
        ).re(r'(?<=PageNumber=)\d+')
        if next_page:
            return self.SEARCH_URL.format(
                    search_term=response.meta['search_term'],
                    results_count=self.DEFAULT_PER_PAGE,
                    page=next_page[0]
                )

    def parse_search_term_items(self, response):
        links = response.xpath(
            '//li[contains(@class, "shelf-product-item")]//a[2]/@href'
        ).extract()
        for link in links:
            yield response.urljoin(link)

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath(
            '//ul[@data-quantity]/@data-quantity'
        ).re(r'(\d+)')
        return int(total_matches[0]) if total_matches else 0

    def parse_search_term_results_per_page(self, response):
        return self.DEFAULT_PER_PAGE

    ################################
    # Shelf pages abstract methods #
    ################################
    def get_shelf_page_next_page(self, response):
        next_page = response.xpath(
            '//a[contains(@class, "shelf-view-more next")]/@href'
        ).extract_first()
        if next_page:
            return response.urljoin(next_page)

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def parse_shelf_page_total_matches(self, response):
        total_matches = response.xpath(
            '//span[@class="result-items"]/text()'
        ).re(r'(\d+)')
        return int(total_matches[0]) if total_matches else 0

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)

    ###################
    # Parsing methods #
    ###################
    @catch_json_exceptions
    def _get_product_json(self, response):
        data = None
        json_data = re.search(
            r'var dataLayer\s=\s(.*?}]);',
            response.body
        )
        if json_data:
            data = json.loads(
                json_data.group(1).replace('\\', '\\\\').replace('\t', ' ')
            )[0]
        return data

    @staticmethod
    def _parse_brand(product_json):
        return product_json.get('product', [{}])[0].get('productBrandName')

    @staticmethod
    def _parse_departments(response):
        departments = response.xpath(
            '//span[@itemprop="title"]/text()'
        ).extract()
        return departments[1:] if departments else None

    @staticmethod
    def _parse_specs(response):
        specs = {}
        specs_data = response.xpath(
            '//table[contains(@class,"characteristics") '
            'and contains(@class,"table-striped")]/tbody//tr'
        )
        if specs_data:
            for spec in specs_data:
                key = spec.xpath('.//th/text()').extract_first()
                data = spec.xpath('.//td/text()').extract_first()
                if key and data:
                    specs[key] = data
        return specs if specs else None

    @staticmethod
    def _parse_sku(product_json):
        sku = product_json.get('product', [{}])[0].get('productSku')
        if not sku:
            sku = product_json.get('product', [{}])[0].get('productSk')
        return sku

    @staticmethod
    def _parse_images_data(response):
        """ Get image info
        :param response (HtmlResponse): general url response
        :return (tuple(image_urls, image_alts, image_dimensions, zoom_image_dimensions))
        """
        data = {
            'urls': [],
            'alts': [],
            'dimensions': [],
            'zoom': []
        }
        images = response.xpath(
            '//div[@id="wm-pictures-carousel"]'
            '//a[contains(@class,"item")]'
        )
        for image in images:
            # image url
            data['urls'].append(
                 response.urljoin(image.xpath('./@data-normal').extract_first())
            )
            # image alt
            data['alts'].append(
                image.xpath('./img/@alt').extract_first()
            )
            # image dimension
            size = image.xpath('./@data-normal').re(r'(?:\d+)-(\d+)-(\d+)')
            if size and int(size[0]) > 500 and int(size[1] > 500):
                data['dimensions'].append(1)
            else:
                data['dimensions'].append(0)
            # zoom image avaliable
            data['zoom'].append(
                int(bool(
                    image.xpath('./@data-zoom')
                ))
            )
        return data['urls'], data['alts'], data['dimensions'], data['zoom']

    @staticmethod
    def _parse_is_out_of_stock(product_json):
        return not product_json.get('product', [{}])[0].get('productAvailable', False)

    @staticmethod
    def _parse_short_description(product_json):
        return remove_tags(
            replace_entities(
                product_json.get('product', [{}])[0].get('productDescription')
            )
        )
    
    @staticmethod
    def _parse_price_amount(product_json):
        price_amount = product_json.get('product', [{}])[0].get('productPrice')
        return float(price_amount) if re.match(r'^\d+?\.?\d+?$', price_amount) else None

    @staticmethod
    def _parse_primary_seller(product_json):
        return product_json.get('product', [{}])[0].get('productSeller', [None])[0]

    @staticmethod
    def _parse_site_product_id(product_url):
        product_id = re.search(r'/(\d+)/', product_url)
        return product_id.group(1) if product_id else None

    @staticmethod
    def _parse_special_pricing(product_json, response):
        seller = product_json.get('product', [{}])[0].get('productSeller')
        if seller:
            price = response.xpath(
                '//li[@data-seller-name="%s"]'
                '//p[contains(@class,"product-price")]/@data-price-old' % seller[0]
            ).extract()
            if price:
                return ''.join(price[0].split('.')[:-1]) + '.' + price[0].split('.')[-1]
        return None
    
    @staticmethod
    def _parse_title(product_json):
        return product_json.get('pageTitle')

    @staticmethod
    def _parse_video_urls(response):
        video_urls = response.xpath(
            '//div[contains(@class,"product-video")]/a/@href'
        ).extract()
        return video_urls

    @staticmethod
    def _parse_reviews(response, stars):
        """ Parse reviews page
        :param response (HtmlResponse): general urk response
        :param stars (list): current `stars` for BuyerReviews
        :return (list): `stars` for BuyerReviews
        """
        stars_values = response.xpath(
            '//div[@class="star-rating-content"]/div[contains(@class, "value")]/@class'
        ).extract()
        for star in stars_values:
            rating_value = re.search(r'star-rating-value-(\d)', star)
            if rating_value:
                if int(rating_value.group(1)) not in stars.keys():
                    stars[int(rating_value.group(1))] = 0
                stars[int(rating_value.group(1))] += 1
        return stars

    @catch_json_exceptions
    def _get_merketplace_json(self, response):
        return json.loads(response.body)

    def _parse_marketplaces(self, response):
        item = response.meta.get('item')
        marketplace_data = self._get_merketplace_json(response)
        if marketplace_data:
            ch_marketplaces = []
            marketplaces = []
            for market in marketplace_data:
                price = 0.0
                in_stock = False
                is_shippable = False
                if market.get('items'):
                    price = '{0:.2f}'.format(float(market.get('items')[0].get('price')) // 100)
                    in_stock = not market.get('items')[0].get('unavailableProduct')
                    is_shippable = bool(market.get('items')[0].get('deliveryTypes'))
                ch_marketplaces.append({
                    'name': market.get('sellerName'),
                    'currency': 'BRL',
                    'price': price,
                    'in_stock': in_stock,
                    'seller_id': market.get('sellerId')
                })
                marketplaces.append({
                    'name': market.get('sellerName'),
                    'currency': 'BRL',
                    'price': price,
                    'in_stock': in_stock,
                    'seller_type': 'seller_type' if market.get('walmart') else 'marketplace',
                    'shippable': is_shippable
                })
            item.update({
                '_ch_marketplace': ch_marketplaces,
                'marketplace': marketplaces,
                'in_stores': bool(ch_marketplaces)
            })
        return item

    def _get_reviews_data(self, response):
        meta = response.meta.copy()
        product = meta.get('item')
        reviews_div = response.xpath('//article[@itemprop="review"]').extract()
        stars = meta.get('stars', {})
        if reviews_div:
            if not meta.get('last_review_date'):
                last_review_timestamp = response.xpath('//meta[@itemprop="datePublished"]/@content').extract_first()
                if last_review_timestamp:
                    meta['last_review_date'] = datetime.datetime.fromtimestamp(int(last_review_timestamp[:-3]))
            stars = self._parse_reviews(response, stars)
            meta['reviews_page'] += 1
            meta['stars'] = stars
            yield MergeRequest(
                url=self.REVIEWS_URL.format(
                    site_product_id=meta['site_product_id'],
                    page_num=meta['reviews_page']
                ),
                meta=meta,
                item=product,
                callback=self._get_reviews_data,
            )
        else:
            if meta.get('last_review_date'):
                product['buyer_reviews'] = BuyerReviews(
                    stars=stars,
                    last_review_date=meta.get('last_review_date')
                )
            else:
                product['buyer_reviews'] = BuyerReviews(stars=stars, average=0.0)
            yield product
