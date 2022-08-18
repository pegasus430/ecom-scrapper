# -*- coding: utf-8 -*-
import traceback
import re
import itertools
from datetime import datetime

from content_analytics.items import BuyerReviews, SiteProductItem
from content_analytics.spiders import BaseProductsSpider
from scrapy import Field


class GncProductItem(SiteProductItem):
    questions_total = Field()


class GncSpider(BaseProductsSpider):
    name = 'gnc_products'
    allowed_domains = ['gnc.com']
    SEARCH_URL = 'http://www.gnc.com/search?q={search_term}&sz=64'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }

    def parse_product(self, response):
        product = response.meta.get('item')

        price = self._parse_price_amount(response)
        sku = self._parse_sku(response)
        product.update(
            {
                # Main data
                'title': self._parse_title(response),
                'brand': self._parse_brand(response),
                'short_description': self._parse_short_desc(response),
                'departments': self._parse_departments(response),
                'image_urls': self._parse_image_urls(response),
                'pdf_urls': self._parse_pdf_urls(response),
                'warnings': self._parse_warnings(response),
                'bullets': self._parse_bullets(response),
                'nutrition_fact_count': self._parse_nutrition_fact_count(response),
                'ingredients': self._parse_ingredients(response),
                'variants': self._parse_variants(response),
                'sku': sku,
                'reseller_id': sku,
                'price_amount': price,
                'now_price': price,
                'was_price': self._parse_was_price(response),
                'price_currency': self._parse_price_currency(response),
                'buyer_reviews': self._parse_buyer_reviews(response),
                'is_out_of_stock': self._parse_is_out_of_stock(response),
                'site_online': True,
                'site_online_out_of_stock': self._parse_is_out_of_stock(response),
                'questions_total': self._parse_questions_total(response)
            }
        )
        yield product

    @staticmethod
    def _parse_title(response):
        return response.xpath('//h1[@class="product-name"]/text()').extract_first()

    @staticmethod
    def _parse_brand(response):
        brand = re.search(r'TurnToCatItemBrand = "(.*?)";', response.body)
        return brand.group(1) if brand else None

    def get_default_item(self, *args, **kwargs):
        return GncProductItem()

    def _parse_buyer_reviews(self, response):
        average = response.xpath(
            '//div[@class="TTreviewSummary"]'
            '//meta[@itemprop="ratingValue"]'
            '/@content'
        ).re_first(r'\d\.?\d*')
        count = response.xpath(
            '//div[@class="TTreviewSummary"]'
            '//meta[@itemprop="reviewCount"]'
            '/@content'
        ).re_first(r'\d{1,3}[,\d{3}]*')
        stars = response.xpath(
            '//div[@class="TTreviewSummary"]'
            '//div[contains(@id, "TTreviewSummaryBreakdown")]'
            '/text()'
        ).re(r'\d{1,3}[,\d{3}]*')

        latest_review = response.xpath(
            '//div[@class="TTreview"]'
            '//div[@itemprop="dateCreated"]'
            '/@datetime'
        ).extract_first()

        if all([average, count, stars, latest_review]):
            try:
                latest_review = datetime.strptime(latest_review, '%Y-%m-%d')
            except:
                self.log('Error Parsing Latest review time: {}'.format(traceback.format_exc()))
                latest_review = None

            return BuyerReviews(
                stars={5-idx: int(star.replace(',', '')) for idx, star in enumerate(stars) if idx < 5},
                average=float(average),
                count=int(count.replace(',', '')),
                last_review_date=latest_review
            )

    @staticmethod
    def _parse_questions_total(response):
        return response.xpath(
            '//span[@class="TTrespDesktopLandscapeDisp"]/text()'
        ).re_first(r'Browse (\d{1,3}[,\d+{3}]*) questions')

    @staticmethod
    def _parse_is_out_of_stock(response):
        in_stock = response.xpath('//div[@id="prod-oos-msg" and @hidden]')
        return not bool(in_stock)

    @staticmethod
    def _parse_short_desc(response):
        return response.xpath(
            '//meta[@property="og:description"]/@content'
        ).extract_first()

    @staticmethod
    def _parse_departments(response):
        return response.xpath(
            '//a[@class="breadcrumb-element"]/text()'
        ).extract()

    @staticmethod
    def _parse_bullets(response):
        bullets_list = response.xpath(
            '//div[@class="product-info-container"]'
            '//div[@class="product-information"]'
            '//ul/li/text()'
        ).extract()
        return '\n'.join(bullets_list) if bullets_list else None

    @staticmethod
    def _parse_warnings(response):
        return response.xpath(
            '//div[contains(h4/text(), "Warnings") and @class="output"]/text()[normalize-space(.)]'
        ).extract_first()

    @staticmethod
    def _parse_nutrition_fact_count(response):
        return len(response.xpath(
            '//div[@class="supplement-information"]'
            '//table//tr[td[@valign="top"] and td[@align="right"]]'
        ).extract())

    @staticmethod
    def _parse_ingredients(response):
        ingredients = response.xpath(
            '//div[@class="output" and contains(h4/text(), "Ingredients")]/text()[normalize-space(.)]'
        ).extract_first()
        if ingredients:
            ingredients = ingredients.replace('(', ',').replace(')', ',').split(',')
            return [
                i.strip()
                for i in ingredients
                if i.strip()
            ]

    @staticmethod
    def _parse_image_urls(response):
        image_urls = response.xpath(
            '//img[@class="productthumbnail"]/@data-lgimg'
        ).re(r'"url":"(.*?)",')
        if not image_urls:
            image_urls = response.xpath(
                '//img[@itemprop="image"]/@src'
            ).extract()
        return image_urls

    @staticmethod
    def _parse_pdf_urls(response):
        return response.xpath('//a[contains(@href, ".pdf")]/@href').extract()

    # Codes parsing
    @staticmethod
    def _parse_sku(response):
        return response.xpath('//span[@itemprop="productID"]/text()').extract_first()

    # Price parsing
    @staticmethod
    def _parse_price_amount(response):
        price = response.xpath(
            '//span[contains(@class, "sale") and @itemprop="price"]/text()'
        ).re_first(r'\d{1,3}[,\.\d{3}]*\.?\d*')
        return float(price)

    @staticmethod
    def _parse_price_currency(response):
        currency = re.search(r'"currency":"(.*?)",', response.body)
        return currency.group(1) if currency else 'USD'

    @staticmethod
    def _parse_was_price(response):
        was_price = response.xpath(
            '//span[@class="price-standard" and @itemprop="highPrice"]/text()'
        ).re_first(r'\d{1,3}[,\.\d{3}]*\.?\d*')
        return was_price.replace(',', '') if was_price else None

    @staticmethod
    def _parse_variants(response):
        elems = response.xpath(
            '//li[contains(@class, "attribute") and contains(@class, "variant-dropdown")]'
        )
        attr_list = []
        value_list = []
        for elem in elems:
            label = elem.xpath('.//span[contains(@class, "label")]/text()').extract_first()
            values = elem.xpath('.//select[@class="variation-select"]/option[position()>1]/text()').extract()
            if not all([label, values]):
                continue
            attr_list.append(label.strip())
            value_list.append(values)
        variant_list = []
        for variant_combination in list(itertools.product(*value_list)):
            variant = {
                'properties': {},
            }
            for idx, attribute in enumerate(attr_list):
                variant['properties'][attribute] = variant_combination[idx].strip()
            variant_list.append(variant)
        if variant_list:
            return variant_list

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        links = response.xpath(
            '//div[@class="product-image"]//a[@class="thumb-link"]/@href'
        ).extract()
        for link in links:
            yield response.urljoin(link)

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//div[@class="pagination"]/ul/li[@class="current-page"]/following-sibling::li/a/@href'
        ).extract_first()
        return next_page

    def parse_search_term_total_matches(self, response):
        total_matches = re.search(r'"searchResultsCount":(\d{1,3}[,\d{3}]*)(\}|,)', response.body)
        return int(total_matches.group(1).replace(',', '')) if total_matches else None

    def parse_search_term_results_per_page(self, response):
        results_per_page = response.xpath(
            '//select[@id="grid-paging-header"]/option[@selected]/text()'
        ).re_first(r'\d+')
        return int(results_per_page) if results_per_page else 64

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
