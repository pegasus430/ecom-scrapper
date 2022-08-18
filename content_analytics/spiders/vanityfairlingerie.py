# -*- coding: utf-8 -*-
import re
import traceback

from urlparse import urljoin
from scrapy.item import Field
from content_analytics.items import SiteProductItem
from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.data_parsing.buyer_reviews.bazaarvoice_reviews import BazaarvoiceReviews


class VanityfairLingerieProductItem(SiteProductItem):
    buy_for = Field()
    save_percent = Field()
    save_amount = Field()


class VanityfairLingerieProductsSpider(BaseProductsSpider):
    name = 'vanityfairlingerie_products'
    allowed_domains = ['vanityfairlingerie.com', BazaarvoiceReviews.DOMAIN]

    def get_default_item(self, *args, **kwargs):
        return VanityfairLingerieProductItem()

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
    }

    SEARCH_URL = 'https://www.vanityfairlingerie.com/search?q={search_term}&lang=default'

    RESULTS_PER_PAGE = 12

    def parse_product(self, response):
        product = response.meta.get('item')

        price = self._parse_price_amount(response)
        product_id = self._parse_reseller_id(response)

        product.update(
            {
                'title': self._parse_title(response),
                'brand': self._parse_brand(response),
                'departments': self._parse_departments(response),
                'long_description': self._parse_long_desc(response),
                'image_urls': self._parse_image_urls(response),
                'bullets': self._parse_bullets(response),
                'variants': self._parse_variants(response),

                # Codes parsing
                'product_id': product_id,
                'reseller_id': product_id,
                'site_product_id': product_id,

                # Price
                'price_amount': price,
                'now_price': price,
                'price_currency': 'USD',
                'save_percent': self._parse_save_percent(response),
                'was_price': self._parse_was_price(response),

                # Availability
                'is_out_of_stock': False,
                'in_stores': False
            }
        )

        # buyer_reviews
        if product_id:
            yield MergeRequest(
                url=BazaarvoiceReviews.compile_url(
                    product_id=product_id,
                    passkey='cawBnVLuwjHOJRBlOKx5tkP5BZ8M9GdljbifcE10tCFus',
                    displaycode='19614-en_us'
                ),
                item=product,
                callback=self._on_reviews_response
            )

        yield product

    @staticmethod
    def _parse_title(response):
        return response.xpath('//h1[@class="product-name"]/text()').extract_first()

    @staticmethod
    def _parse_brand(response):
        return response.xpath('//meta[@property="og:brand"]/@content').extract_first()

    @staticmethod
    def _parse_departments(response):
        departments = response.xpath('//*[@class="breadcrumb-element"]/text()').extract()
        return departments if departments else None

    @staticmethod
    def _parse_is_out_of_stock(response):
        in_stock = response.xpath('//meta[@property="og:availability"]/@content').re(r'instock')
        return not bool(in_stock)

    @staticmethod
    def _parse_long_desc(response):
        return response.xpath('//div[@itemprop="description"]/text()').extract_first()

    @staticmethod
    def _parse_bullets(response):
        return response.xpath('//div[contains(@class, "product-tabs")]'
                              '//div[@id="tab2"]//ul').extract_first()

    @staticmethod
    def _parse_warnings(response):
        return response.xpath('//div[h3/strong[contains(text(), "Warnings")]]'
                              '/div[contains(@class, "prod")]'
                              '/p/text()').extract_first()

    @staticmethod
    def _parse_variants(response):
        variant_list = []
        attrs = response.xpath('//div[@class="product-grouping-row"]')
        for attr in attrs:
            attr_name = attr.xpath('./label/text()').extract_first()
            if not attr_name:
                continue
            for variant in attr.xpath('.//div[contains(@class, "thumbnail-tile")]'):
                variant_list.append({
                    'sku_id': variant.xpath('./@data-pid').extract_first(),
                    'image_url': variant.xpath('./img/@src').extract_first(),
                    'properties': {
                        attr_name.strip().lower(): variant.xpath('./@data-val').extract_first()
                    },
                    'selected': 'combo-selected' in variant.xpath('./@class').extract_first()
                })
        return variant_list

    @staticmethod
    def _parse_image_urls(response):
        return response.xpath('//div[@class="product-primary-image"]//img/@src').extract()

    @staticmethod
    def _parse_reseller_id(response):
        reseller_id = re.search(r'/(\d+)\.html', response.url)
        return reseller_id.group(1) if reseller_id else None

    @staticmethod
    def _parse_save_percent(response):
        save_percent = response.xpath('//div[@class="promotion-callout"]'
                                      '//div[@class="tooltip-content"]').re(r'\d+')
        return save_percent[0] if save_percent else None

    def _parse_was_price(self, response):
        was_price = response.xpath('//div[@class="product-price"]'
                                   '//span[@class="price-standard"]'
                                   '/text()').extract_first()
        try:
            return float(was_price.replace('$', '')) if was_price else None
        except:
            self.logger.warning('Could not parse standard price {}'.format(traceback.format_exc()))

    def _parse_price_amount(self, response):
        price = response.xpath('//span[contains(@class, "price-sales")]/text()').extract_first()
        try:
            return float(price.replace('$', '')) if price else None
        except:
            self.logger.warning('Could not parse price amount {}'.format(traceback.format_exc()))

    @staticmethod
    def _on_reviews_response(response):
        product = response.meta.get('item')
        product.update({'buyer_reviews': BazaarvoiceReviews.parse_reviews(response.body)})

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        links = response.xpath('//ul[@id="search-result-items"]'
                               '//li//div[@class="product-tile"]'
                               '//div[@class="product-name"]/a/@href').extract()
        for link in links:
            yield urljoin(response.url, link)

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//div[@class="pagination"]/ul/li[@class="current-page"]/following-sibling::li/a/@href'
        ).extract_first()
        return next_page

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//span[@class="pagination-info"]').re(r'(\d+) Results')
        if total_matches:
            return int(total_matches[0])

    def parse_search_term_results_per_page(self, response):
        return self.RESULTS_PER_PAGE

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
