import logging
from datetime import datetime

import json

from content_analytics.items import BuyerReviews
from content_analytics.utils import catch_json_exceptions


class BazaarvoiceReviews(object):
    logger = logging.getLogger(__name__)
    DOMAIN = 'api.bazaarvoice.com'
    REVIEWS_URL = "https://api.bazaarvoice.com/data/batch.json?" \
                  "passkey={passkey}" \
                  "&apiversion=5.5" \
                  "&displaycode={displaycode}" \
                  "&resource.q0=products" \
                  "&filter.q0=id%3Aeq%3A{product_id}" \
                  "&stats.q0=reviews"

    @staticmethod
    def compile_url(product_id, passkey, displaycode):
        return BazaarvoiceReviews.REVIEWS_URL.format(passkey=passkey,
                                                     displaycode=displaycode,
                                                     product_id=product_id)

    @staticmethod
    @catch_json_exceptions
    def parse_reviews(response_str):
        reviews_json_obj = json.loads(response_str)
        if not reviews_json_obj:
            return None

        result_reviews = reviews_json_obj.get('BatchedResults', {}).get('q0', {}).get('Results', [])
        if not result_reviews:
            return None  # no reviews

        reviews_stats = result_reviews[0].get('ReviewStatistics', {})
        last_review = reviews_stats.get('LastSubmissionTime')
        if last_review:
            last_review = datetime.strptime(last_review.split('.')[0], '%Y-%m-%dT%H:%M:%S')

        average = reviews_stats.get('AverageOverallRating', 0)
        stars = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for star in reviews_stats.get('RatingDistribution', []):
            stars[star.get('RatingValue')] = star.get('Count', 0)

        return BuyerReviews(
            stars=stars,
            count=reviews_stats.get('TotalReviewCount', 0),
            average=average if average else 0,
            last_review_date=last_review.date() if last_review else None
        )
