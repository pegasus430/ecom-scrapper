# -*- coding: utf-8 -*-
import re
import traceback

from datetime import datetime
from urlparse import urljoin, urlparse

from scrapy.item import Field
from scrapy.log import WARNING

from content_analytics.items import SiteProductItem, BuyerReviews
from content_analytics.spiders import BaseProductsSpider, MergeRequest


class IHerbProductItem(SiteProductItem):
    buy_for = Field()
    save_percent = Field()
    save_amount = Field()


class IHerbsProductsSpider(BaseProductsSpider):
    name = 'iherb_products'
    allowed_domains = ['iherb.com']
    SEARCH_URL = 'https://www.iherb.com/search?kw={search_term}'

    def get_default_item(self, *args, **kwargs):
        return IHerbProductItem()

    def parse_product(self, response):
        product = response.meta.get('item')

        if product.get('redirect') == True:
            product.update(
                {
                    'total_matches': 1,
                    'url': response.url
                }
            )
            yield product
        else:
            price = self._parse_price_amount(response)

            gtin = self._parse_gtin(response)
            product.update(
                {
                    # Main data
                    'title': self._parse_title(response),
                    'brand': self._parse_brand(response),
                    'departments': self._parse_departments(response),
                    'long_description': self._parse_long_desc(response),
                    'image_urls': self._parse_image_urls(response),
                    'warnings': self._parse_warnings(response),
                    'specs': self._parse_specs(response),
                    'bullets': self._parse_bullets(response),
                    'nutrition_fact_count': self._parse_nutrition_fact_count(response),
                    'variants': self._parse_variants(response),
                    'ingredients': self._parse_ingredients(response),

                    # Codes
                    'sku': self._parse_sku(response),
                    'reseller_id': self._parse_reseller_id(response),
                    'gtin': gtin,
                    'upc': gtin,

                    # Price
                    'price_amount': price,
                    'now_price': price,
                    'price_currency': self._parse_price_currency(response),
                    'save_amount': self._parse_save_amount(response),
                    'save_percent': self._parse_save_percent(response),
                    'was_price': self._parse_was_price(response),

                    'is_out_of_stock': self._parse_is_out_of_stock(response),
                }
            )

            review_url = response.xpath('//div[@itemprop="aggregateRating"]//a[@class="stars"]/@href').extract_first()
            if review_url:
                yield MergeRequest(
                        url=review_url,
                        item=product,
                        callback=self._parse_buyer_reviews,
                    )
            yield product

    # Parsing json data
    @staticmethod
    def _parse_title(response):
        return response.xpath('//meta[@property="og:title"]/@content').extract_first()

    @staticmethod
    def _parse_brand(response):
        return response.xpath('//meta[@property="og:brand"]/@content').extract_first()

    @staticmethod
    def _parse_departments(response):
        departments = response.xpath('//div[@id="breadCrumbs"]//a[@href]/text()').extract()
        return departments if departments else None

    def _parse_buyer_reviews(self, response):
        product = response.meta.get('item')
        average = response.xpath('//span[@class="rating-average"]/text()').re(r'\d\.?\d*')
        count = response.xpath('//span[@class="customer-rating"]/text()').re(r'\d+')
        stars = response.xpath('//figure[@class="ratings-graph-container"]'
                               '//div[@class="right-container"]'
                               '//a[not(span)]/text()').re(r'\d+')

        latest_review = [
            i.strip()
            for i in response.xpath('//div[@class="posted-by"]/p/text()').re(r'on (.*)')
            if i.strip()
            ]

        if all([average, count, stars, latest_review]):
            try:
                latest_review = datetime.strptime(latest_review[0], '%b %d, %Y')
            except:
                self.log('Error Parsing Latest review time: {}'.format(traceback.format_exc()))
                latest_review = None

            product['buyer_reviews'] = BuyerReviews(
                stars={5-idx: int(star) for idx, star in enumerate(stars)},
                average=float(average[0]),
                count=int(count[0]),
                last_review_date=latest_review
            )

    @staticmethod
    def _parse_is_out_of_stock(response):
        in_stock = response.xpath('//meta[@property="og:availability"]/@content').re(r'instock')
        return not bool(in_stock)

    @staticmethod
    def _parse_long_desc(response):
        long_desc = response.xpath('//div[@itemprop="description"]'
                                   '//*[not(self::ul[position()=1])]//text()').extract()
        return ' '.join(long_desc) if long_desc else None

    @staticmethod
    def _parse_specs(response):
        specs = {}
        spec_list = response.xpath('//ul[@id="product-specs-list"]/li')
        for spec in spec_list:
            key = [i.strip() for i in spec.xpath('./text()').extract() if i.strip()]
            value = [i.strip() for i in spec.xpath('.//span/text()').extract() if i.strip()]
            if not value and key:
                value = [key[-1]]
            if all([key, value]):
                specs[key[0]] = ''.join(value)
        return specs

    @staticmethod
    def _parse_bullets(response):
        bullets = response.xpath('//div[@itemprop="description"]//ul').extract_first()
        return bullets

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
    def _parse_nutrition_fact_count(response):
        nutrition_facts = response.xpath(
            '//table[contains(//strong/text(), "Facts")]'
            '//td/text()'
        ).extract()
        return len([
            i
            for i in nutrition_facts
            if i.strip()
        ])

    @staticmethod
    def _parse_ingredients(response):
        ingredients = response.xpath('//div[@class="prodOverviewIngred"]//p/text()').extract_first()
        return ingredients.split(',') if ingredients else None

    @staticmethod
    def _parse_image_urls(response):
        return response.xpath(
            '//div[@class="img-slider-container"]//img/@data-lazyload'
        ).extract()

    # Codes parsing
    @staticmethod
    def _parse_sku(response):
        return response.xpath('//span[@itemprop="sku"]/text()').extract_first()

    @staticmethod
    def _parse_gtin(response):
        return response.xpath('//span[@itemprop="gtin12"]/text()').extract_first()

    @staticmethod
    def _parse_reseller_id(response):
        return urlparse(response.url).path.split('/')[-1]

    @staticmethod
    def _parse_was_price(response):
        was_price = response.xpath('//section[@id="product-msrp"]'
                                   '//div[contains(@class, "price")]'
                                   '//s/text()').re(r'\d{1,3}[,\d{3}]*\.?\d*')
        return was_price[0] if was_price else None

    @staticmethod
    def _parse_save_amount(response):
        save_amount = response.xpath('//section[@id="product-discount"]'
                                     '//div[contains(@class, "discount")]'
                                     '/text()').re(r'\d{1,3}[,\d{3}]*\.?\d*')
        return save_amount[0] if save_amount else None

    @staticmethod
    def _parse_save_percent(response):
        save_percent = response.xpath('//section[@id="product-discount"]'
                                      '//div[contains(@class, "discount")]'
                                      '//span[@class="discount-text"]/text()').re(r'\d{1,3}[,\d{3}]*\.?\d*')

        return save_percent[0] if save_percent else None

    def _parse_price_amount(self, response):
        price = response.xpath('//meta[@property="og:price:amount"]/@content').extract()
        try:
            return float(price[0])
        except:
            self.log('Error Parsing the Price Amount: {}'.format(traceback.format_exc()), WARNING)

    @staticmethod
    def _parse_price_currency(response):
        price_currency = response.xpath('//meta[@property="og:price:currency"]/@content').extract_first()
        return price_currency if price_currency else 'USD'

    @staticmethod
    def _parse_buy_for(response):
        products_num = response.xpath('//div[@volume-discount-item" and button[@add-to-cart-quantity]]'
                                 '//button[@add-to-cart-quantity]'
                                 '/@add-to-cart-quantity').extract()
        price = response.xpath('//div[@volume-discount-item" and button[@add-to-cart-quantity]]'
                               '//strong[contains(@class, "discounted-price")]'
                               '/text()').re(r'\d{1,3}[,\d{3}]*\.?\d*')
        if all([products_num, price]):
            return ','.join([products_num[0], price[0]])

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        links = response.xpath('//a[@itemprop="url" and @data-ga-event-action="productClick"]/@href').extract()

        # Checks search result page is redirected into single product or not.
        if not links and self.valid_single_product_url(response):
            item = IHerbProductItem()
            item['redirect'] = True
            response.meta.update({
                'item': item
            })
            yield response.url, item
        for link in links:
            yield link

    @staticmethod
    def valid_single_product_url(response):
        """Checks whether response url is like single product or not.
        Returns:
            True if valid, False otherwise
        """
        m = re.match(r"^https://www.iherb.com/pr/.*?$", response.url)
        return bool(m)

    def get_search_term_next_page(self, response):
        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract_first()
        if next_page:
            return urljoin(response.url, next_page)

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//span[@class="sub-header-title display-items"]').re(r'(\d+) Results')
        if total_matches:
            return int(total_matches[0])

    def parse_search_term_results_per_page(self, response):
        results_per_page = response.xpath('//div[@class="items-per-page"]'
                                          '//select[@class="dropdown-sort"]'
                                          '/option[@selected]/@value').re(r'\d+')
        return int(results_per_page[0]) if results_per_page else 24

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
