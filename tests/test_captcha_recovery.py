"""Recovery-focused automated tests for captcha-aware crawler flows."""

from unittest.mock import Mock, patch

import pytest

from config.settings import settings
from crawler.anti_crawler import AntiCrawler
from crawler.exceptions import (
    CaptchaAutoSlideFailed,
    CaptchaCooldownException,
    CaptchaTimeoutException,
)
from crawler.hotel_list_crawler import HotelListCrawler
from crawler.review_crawler import ReviewCrawler


def _price_range(level: str = "经济型", top_n: int = 5) -> dict:
    return {
        "level": level,
        "min": 0,
        "max": 300,
        "top_n": top_n,
    }


def _business_zone() -> dict:
    return {"name": "测试商圈", "code": "zone-a"}


def test_hotel_list_interruption_propagates_captcha_exception():
    anti = Mock()
    anti.navigate_to = Mock(
        side_effect=CaptchaTimeoutException(
            timeout_seconds=120,
            elapsed_seconds=133,
            retry_count=1,
            max_retries=3,
        )
    )
    crawler = HotelListCrawler(anti_crawler=anti)
    crawler._get_saved_hotel_ids = Mock(return_value=set())

    with pytest.raises(CaptchaTimeoutException):
        crawler.crawl_by_zone_and_price(
            region_type="CBD商务区",
            business_zone=_business_zone(),
            price_range=_price_range(),
            save_to_db=False,
        )


def test_review_interruption_preserves_completed_pool_save():
    crawler = ReviewCrawler(anti_crawler=Mock())
    first_pool = [{"review_id": "r1"}, {"review_id": "r2"}]

    crawler.navigate_to_reviews = Mock(return_value=True)
    crawler.get_total_review_count = Mock(return_value=settings.min_reviews_threshold + 10)
    crawler._crawl_pool = Mock(
        side_effect=[
            first_pool,
            CaptchaTimeoutException(
                timeout_seconds=120,
                elapsed_seconds=125,
                retry_count=1,
                max_retries=3,
            ),
        ]
    )
    crawler.save_reviews = Mock(return_value=len(first_pool))

    with pytest.raises(CaptchaTimeoutException):
        crawler.waterfall_crawl("10019773", max_reviews=10, save_to_db=True)

    crawler.save_reviews.assert_called_once_with(first_pool)


def test_navigate_to_rethrows_captcha_failure_to_caller():
    anti = AntiCrawler()
    page = Mock()
    page.get = Mock()
    page.wait.load_start = Mock()

    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()
    anti.check_captcha = Mock(return_value=True)
    anti.handle_captcha = Mock(
        side_effect=CaptchaAutoSlideFailed(
            reason="auto slide failed",
            retry_count=1,
            max_retries=3,
        )
    )

    with pytest.raises(CaptchaAutoSlideFailed):
        anti.navigate_to("https://example.com")


def test_review_guard_stops_before_captcha_dom_parsing_continues():
    anti = Mock()
    page = Mock()
    page.html = "验证已过期，请刷新"
    anti.get_page = Mock(return_value=page)
    anti.check_captcha = Mock()
    anti.handle_captcha = Mock()

    crawler = ReviewCrawler(anti_crawler=anti)

    with pytest.raises(CaptchaTimeoutException):
        crawler._ensure_review_page_ready(
            hotel_id="10019773",
            source_pool="negative",
            action="extract_reviews",
            progress="1/3",
        )

    anti.check_captcha.assert_not_called()
    anti.handle_captcha.assert_not_called()


def test_handle_captcha_refreshes_expired_verification_page_once():
    anti = AntiCrawler()
    anti.check_captcha = Mock(side_effect=[True, True, False])
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=True)
    anti._refresh_verification_page = Mock()
    anti.random_delay = Mock()

    with patch("crawler.anti_crawler.time.time", side_effect=[0, 1]):
        anti.handle_captcha()

    anti._refresh_verification_page.assert_called_once_with()
    assert anti._captcha_refresh_count == 1


def test_handle_captcha_raises_terminal_cooldown_after_retry_ceiling():
    anti = AntiCrawler()
    anti._captcha_retry_count = settings.captcha_max_retries - 1
    anti.check_captcha = Mock(side_effect=[True, True])
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=False)

    with patch("crawler.anti_crawler.time.time", side_effect=[0, 1]):
        with pytest.raises(CaptchaCooldownException) as exc_info:
            anti.handle_captcha()

    assert exc_info.value.should_retry() is False
