import json

from content_analytics.items import BuyerReviews
from content_analytics.utils import catch_json_exceptions


class PowerReviews(object):
    REVIEWS_URL = 'https://readservices-b2c.powerreviews.com/m/{group_id}/l/{locale}/product/{prod_id}/reviews'
    DEFAULT_LOCALE = 'en_US'

    @staticmethod
    def compile_url(product_id, group_id, locale=DEFAULT_LOCALE):
        return PowerReviews.REVIEWS_URL.format(
            group_id=group_id,
            prod_id=product_id,
            locale=locale
        )

    @staticmethod
    @catch_json_exceptions
    def parse_reviews(response):
        review_data = json.loads(response.body_as_unicode()).get('results')[0]
        data_rollup = review_data.get('rollup', {})
        data_metrics = review_data.get('metrics', data_rollup)

        num_of_reviews = data_metrics.get('review_count', 0)
        average_rating = data_metrics.get('average_rating', 0)

        rating_by_star = {
            int(star): int(value)
            for star, value in enumerate(data_rollup.get('rating_histogram', []), 1)
        }

        buyer_reviews = {
            'stars': rating_by_star,
            'average': float(average_rating),
            'count': int(num_of_reviews)
        }

        return BuyerReviews(**buyer_reviews)
