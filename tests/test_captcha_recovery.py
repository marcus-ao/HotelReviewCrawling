"""Recovery-focused automated tests for captcha-aware crawler flows."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from config.settings import settings
from crawler.anti_crawler import AntiCrawler
from crawler.exceptions import (
    RecoverableInterruption,
    CaptchaAutoSlideFailed,
    CaptchaCooldownException,
    CaptchaTimeoutException,
)
from crawler.hotel_list_crawler import HotelListCrawler
from crawler.review_crawler import ReviewCrawler
import main as app_main
from utils.checkpoint_manager import CheckpointManager


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
    crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
    first_pool = [{"review_id": "r1"}, {"review_id": "r2"}]

    crawler.navigate_to_reviews = Mock(return_value=True)
    crawler.get_total_review_count = Mock(return_value=settings.min_reviews_threshold + 10)
    crawler._load_review_checkpoint = Mock(return_value=None)
    crawler._load_existing_reviews_from_db = Mock(return_value=[])
    crawler._crawl_pool = Mock(return_value=first_pool)
    crawler._crawl_positive_pool_manual = Mock(
        side_effect=CaptchaTimeoutException(
            timeout_seconds=120,
            elapsed_seconds=125,
            retry_count=1,
            max_retries=3,
        )
    )
    crawler.save_reviews = Mock(return_value=len(first_pool))

    with pytest.raises(CaptchaTimeoutException):
        crawler.waterfall_crawl("10019773", max_reviews=10, save_to_db=True)

    crawler.save_reviews.assert_called_once_with(first_pool)


def test_waterfall_respects_configured_pool_limits_and_remaining_quota():
    crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=False)
    crawler.navigate_to_reviews = Mock(return_value=True)
    crawler.get_total_review_count = Mock(return_value=500)
    crawler._get_hotel_review_count_from_db = Mock(return_value=500)
    crawler.save_reviews = Mock(return_value=0)

    crawler._crawl_pool = Mock(
        side_effect=lambda **kwargs: [
            {"review_id": f"{kwargs['source_pool']}_{i}"} for i in range(kwargs["max_count"])
        ]
    )

    fake_settings = SimpleNamespace(
        max_reviews_per_hotel=300,
        min_reviews_threshold=200,
        review_negative_pool_limit=80,
        review_total_sample_ratio=0.20,
        review_total_min_per_hotel=80,
        review_total_max_per_hotel=300,
        review_negative_target_ratio=0.25,
        review_negative_min_per_hotel=20,
        review_negative_max_per_hotel=80,
    )

    with patch("crawler.review_crawler.settings", fake_settings):
        reviews = crawler.waterfall_crawl("10019773", max_reviews=100, save_to_db=False)

    assert len(reviews) == 25
    assert crawler._crawl_pool.call_count == 1
    assert crawler._crawl_pool.call_args_list[0].kwargs["source_pool"] == "negative"
    assert crawler._crawl_pool.call_args_list[0].kwargs["max_count"] == 25


def test_waterfall_resume_negative_respects_total_review_cap():
    crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=False)
    crawler.navigate_to_reviews = Mock(return_value=True)
    crawler.get_total_review_count = Mock(return_value=500)
    crawler._get_hotel_review_count_from_db = Mock(return_value=500)
    crawler.save_reviews = Mock(return_value=0)
    crawler._load_existing_reviews_from_db = Mock(
        return_value=[
            {"review_id": f"p{i}", "source_pool": "positive"}
            for i in range(280)
        ]
    )
    crawler._crawl_pool = Mock(return_value=[])

    fake_settings = SimpleNamespace(
        max_reviews_per_hotel=300,
        min_reviews_threshold=200,
        review_negative_pool_limit=100,
        review_total_sample_ratio=0.20,
        review_total_min_per_hotel=80,
        review_total_max_per_hotel=300,
        review_negative_target_ratio=0.25,
        review_negative_min_per_hotel=20,
        review_negative_max_per_hotel=80,
    )

    with patch("crawler.review_crawler.settings", fake_settings):
        reviews = crawler.waterfall_crawl("10019773", max_reviews=300, save_to_db=True)

    assert len(reviews) == 100
    assert crawler._crawl_pool.call_count == 0


def test_crawl_pool_stops_after_stagnant_pages():
    crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=False)
    crawler.filter_reviews = Mock(return_value=True)
    crawler.extract_reviews_from_page = Mock(side_effect=[[], []])
    crawler.load_more_reviews = Mock(return_value=1)

    fake_settings = SimpleNamespace(review_stagnant_page_limit=2)
    with patch("crawler.review_crawler.settings", fake_settings):
        reviews = crawler._crawl_pool(
            hotel_id="10019773",
            source_pool="negative",
            filter_types=[crawler.FILTER_ALL],
            max_count=20,
        )

    assert reviews == []
    assert crawler.load_more_reviews.call_count == 1


def test_main_review_all_processes_multiple_batches_until_empty():
    anti = Mock()
    anti.init_browser = Mock()
    anti.close = Mock()
    anti.random_delay = Mock()

    crawler = Mock()
    crawler.crawl_hotel_reviews = Mock(return_value=[{"review_id": "r1"}])

    hotels = [
        SimpleNamespace(hotel_id="h1", review_count=500),
        SimpleNamespace(hotel_id="h2", review_count=300),
        SimpleNamespace(hotel_id="h3", review_count=200),
    ]
    session = Mock()
    session.query.return_value.filter.return_value.order_by.return_value.all.return_value = hotels

    @contextmanager
    def fake_session_scope():
        yield session

    fake_settings = SimpleNamespace(
        min_reviews_threshold=50,
        review_positive_manual_default_enabled=True,
        log_path=Path("logs"),
    )

    with patch("main.AntiCrawler", return_value=anti), \
         patch("main.ReviewCrawler", return_value=crawler) as crawler_cls, \
         patch("main.session_scope", fake_session_scope), \
         patch("main.settings", fake_settings):
        result = app_main.crawl_reviews(all_hotels=True)

    assert result == app_main.EXIT_SUCCESS
    assert crawler_cls.call_args.kwargs["positive_manual"] is False
    assert crawler.crawl_hotel_reviews.call_count == 3
    crawler.crawl_hotel_reviews.assert_any_call("h1", save_to_db=True)
    crawler.crawl_hotel_reviews.assert_any_call("h2", save_to_db=True)
    crawler.crawl_hotel_reviews.assert_any_call("h3", save_to_db=True)


def test_waterfall_default_max_reviews_uses_dynamic_target_without_none_math_bug():
    crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=False)
    crawler.navigate_to_reviews = Mock(return_value=True)
    crawler.get_total_review_count = Mock(return_value=500)
    crawler._get_hotel_review_count_from_db = Mock(return_value=500)
    crawler._load_existing_reviews_from_db = Mock(return_value=[])
    crawler._crawl_pool = Mock(return_value=[])

    fake_settings = SimpleNamespace(
        max_reviews_per_hotel=300,
        min_reviews_threshold=200,
        review_negative_pool_limit=100,
        review_total_sample_ratio=0.20,
        review_total_min_per_hotel=80,
        review_total_max_per_hotel=300,
        review_negative_target_ratio=0.25,
        review_negative_min_per_hotel=20,
        review_negative_max_per_hotel=80,
    )

    with patch("crawler.review_crawler.settings", fake_settings):
        reviews = crawler.waterfall_crawl("10019773", save_to_db=True)

    assert reviews == []
    assert crawler._crawl_pool.call_count == 1
    assert crawler._crawl_pool.call_args.kwargs["max_count"] == 25


def test_main_review_all_forces_positive_manual_off():
    anti = Mock()
    anti.init_browser = Mock()
    anti.close = Mock()

    crawler = Mock()
    crawler.crawl_hotel_reviews = Mock(return_value=[])

    hotels = [SimpleNamespace(hotel_id="h1", review_count=500)]
    session = Mock()
    session.query.return_value.filter.return_value.order_by.return_value.all.return_value = hotels

    @contextmanager
    def fake_session_scope():
        yield session

    fake_settings = SimpleNamespace(
        min_reviews_threshold=50,
        review_positive_manual_default_enabled=True,
        log_path=Path("logs"),
    )

    with patch("main.AntiCrawler", return_value=anti), \
         patch("main.ReviewCrawler", return_value=crawler) as crawler_cls, \
         patch("main.session_scope", fake_session_scope), \
         patch("main.settings", fake_settings):
        result = app_main.crawl_reviews(all_hotels=True, positive_manual=True)

    assert result == app_main.EXIT_SUCCESS
    assert crawler_cls.call_args.kwargs["positive_manual"] is True


def test_checkpoint_manager_roundtrip():
    temp_dir = Path("logs") / "test_checkpoints"
    temp_dir.mkdir(parents=True, exist_ok=True)
    manager = CheckpointManager(base_dir=temp_dir)
    payload = {"hotel_id": "10001", "stage": "negative", "page_no": 4}

    path = manager.save("reviews", "10001", payload)
    loaded = manager.load("reviews", "10001")

    assert Path(path).exists()
    assert loaded == payload

    manager.clear("reviews", "10001")
    assert manager.load("reviews", "10001") is None


def test_main_review_single_hotel_retries_after_recoverable_interruption():
    anti = Mock()
    anti.init_browser = Mock()
    anti.close = Mock()

    crawler = Mock()
    crawler.crawl_hotel_reviews = Mock(
        side_effect=[
            RecoverableInterruption(
                "network changed",
                action="waterfall_negative",
                checkpoint_path="logs/checkpoints/reviews_10019773.json",
                context={"hotel_id": "10019773", "stage": "negative"},
            ),
            [{"review_id": "r1"}],
        ]
    )

    with patch("main.AntiCrawler", return_value=anti), \
         patch("main.ReviewCrawler", return_value=crawler), \
         patch("builtins.input", side_effect=[""]):
        result = app_main.crawl_reviews(hotel_id="10019773")

    assert result == app_main.EXIT_SUCCESS
    assert anti.init_browser.call_count == 2
    anti.close.assert_called_once_with()


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
    anti.check_captcha = Mock(side_effect=[True, True, True, False])
    anti._detect_captcha_stage = Mock(return_value="slider")
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=True)
    anti._click_verification_refresh = Mock(return_value=False)
    anti._refresh_verification_page = Mock()
    anti.random_delay = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        anti.handle_captcha()

    anti._refresh_verification_page.assert_called_once_with()
    assert anti._captcha_refresh_count == 1


def test_handle_captcha_raises_terminal_cooldown_after_retry_ceiling():
    anti = AntiCrawler()
    anti._captcha_retry_count = settings.captcha_max_retries - 1
    anti.check_captcha = Mock(side_effect=[True, True, True])
    anti._detect_captcha_stage = Mock(return_value="slider")
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=False)

    with patch("crawler.anti_crawler.time.time", side_effect=[0, 1]):
        with pytest.raises(CaptchaCooldownException) as exc_info:
            anti.handle_captcha()

    assert exc_info.value.should_retry() is False


def test_handle_captcha_prefers_click_refresh_zone_before_page_refresh():
    anti = AntiCrawler()
    anti.check_captcha = Mock(side_effect=[True, True, False])
    anti._detect_captcha_stage = Mock(return_value="refresh_click")
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=True)
    anti._click_verification_refresh = Mock(return_value=True)
    anti._refresh_verification_page = Mock()
    anti.random_delay = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        anti.handle_captcha()

    anti._click_verification_refresh.assert_called_once_with()
    anti._refresh_verification_page.assert_not_called()
    assert anti._captcha_refresh_count == 0


def test_handle_captcha_supports_alternating_click_and_slide_stages():
    anti = AntiCrawler()
    anti._captcha_retry_count = 2
    anti.check_captcha = Mock(side_effect=[True, True, True, False])
    anti._detect_captcha_stage = Mock(side_effect=["refresh_click", "slider", "slider", "slider", "slider"])
    anti._click_verification_refresh = Mock(return_value=True)
    anti._auto_slide_captcha = Mock(return_value=True)
    anti._is_verification_expired = Mock(return_value=False)
    anti.random_delay = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        anti.handle_captcha()

    anti._click_verification_refresh.assert_called_once_with()
    anti._auto_slide_captcha.assert_called_once_with()
    assert anti._captcha_retry_count == 0
    assert anti._captcha_refresh_count == 0


def test_handle_captcha_refreshes_when_refresh_click_stage_stalls():
    anti = AntiCrawler()
    anti.check_captcha = Mock(side_effect=[True, True, True, True, False])
    anti._detect_captcha_stage = Mock(return_value="refresh_click")
    anti._click_verification_refresh = Mock(return_value=True)
    anti._refresh_verification_page = Mock()
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=False)
    anti.random_delay = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        anti.handle_captcha()

    anti._refresh_verification_page.assert_called_once_with()
    assert anti._captcha_refresh_count == 1


def test_handle_captcha_refreshes_when_click_slide_loop_repeats():
    anti = AntiCrawler()
    anti.check_captcha = Mock(side_effect=[True, True, True, True, True, True, True, False])
    anti._detect_captcha_stage = Mock(
        side_effect=[
            "slider", "refresh_click",  # loop 1
            "slider", "refresh_click",  # loop 2
            "slider", "refresh_click",  # loop 3 -> force refresh
            "slider",                    # after refresh
        ]
    )
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=False)
    anti._refresh_verification_page = Mock()
    anti.random_delay = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        anti.handle_captcha()

    anti._refresh_verification_page.assert_called_once_with()
    assert anti._captcha_refresh_count == 1


def test_click_verification_refresh_uses_refresh_selector_click():
    anti = AntiCrawler()
    page = Mock()
    page.html = ""
    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()
    anti._click_selector_center = Mock(side_effect=lambda selector: selector == '#nc_1_refresh1')
    anti._did_click_refresh_take_effect = Mock(return_value=True)

    assert anti._click_verification_refresh() is True
    anti._click_selector_center.assert_any_call('#nc_1_refresh1')


def test_click_verification_refresh_clicks_retry_rectangle_when_prompt_detected():
    anti = AntiCrawler()
    page = Mock()
    page.html = "验证失败，点击框体重试(errorspQeLb)"
    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()
    anti._click_selector_center = Mock(side_effect=lambda selector: selector == '#nc_1_n1t')
    anti._did_click_refresh_take_effect = Mock(return_value=True)

    assert anti._click_verification_refresh() is True
    assert any(call.args[0] == '#nc_1_n1t' for call in anti._click_selector_center.call_args_list)


def test_click_verification_refresh_returns_false_when_click_has_no_effect():
    anti = AntiCrawler()
    page = Mock()
    page.html = "验证失败，点击框体重试"
    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()
    anti._click_selector_center = Mock(return_value=True)
    anti._did_click_refresh_take_effect = Mock(return_value=False)

    assert anti._click_verification_refresh() is False


def test_save_captcha_debug_artifacts_writes_html_and_falls_back_screenshot(tmp_path):
    anti = AntiCrawler()
    page = Mock()
    page.get_screenshot = Mock(side_effect=RuntimeError("primary screenshot failed"))
    page.screenshot = Mock(side_effect=lambda path: Path(path).write_bytes(b"png"))
    page.html = "<html>captcha</html>"
    anti.get_page = Mock(return_value=page)

    fake_settings = SimpleNamespace(
        captcha_debug_artifacts_enabled=True,
        captcha_debug_artifacts_path=tmp_path,
    )

    with patch("crawler.anti_crawler.settings", fake_settings):
        anti._save_captcha_debug_artifacts("timeout/cooldown branch")

    png_files = list(tmp_path.glob("*.png"))
    html_files = list(tmp_path.glob("*.html"))

    assert len(png_files) == 1
    assert len(html_files) == 1
    assert "timeout_cooldown_branch" in png_files[0].stem
    assert html_files[0].read_text(encoding="utf-8") == "<html>captcha</html>"
    page.screenshot.assert_called_once_with(path=str(png_files[0]))


def test_save_captcha_debug_artifacts_swallow_errors_when_snapshot_fails(tmp_path):
    anti = AntiCrawler()
    page = Mock()
    page.get_screenshot = Mock(side_effect=RuntimeError("boom"))
    page.screenshot = Mock(side_effect=RuntimeError("boom"))
    page.save = Mock(side_effect=RuntimeError("boom"))
    type(page).html = property(lambda self: (_ for _ in ()).throw(RuntimeError("html boom")))
    anti.get_page = Mock(return_value=page)

    fake_settings = SimpleNamespace(
        captcha_debug_artifacts_enabled=True,
        captcha_debug_artifacts_path=tmp_path,
    )

    with patch("crawler.anti_crawler.settings", fake_settings):
        anti._save_captcha_debug_artifacts("retryable failure")

    assert list(tmp_path.iterdir()) == []


def test_refresh_verification_page_uses_navigation_fallback_when_refresh_no_progress():
    anti = AntiCrawler()
    page = Mock()
    page.url = "https://example.com/search?city=gz"
    page.wait.load_start = Mock()
    page.refresh = Mock()
    page.get = Mock()
    page.run_cdp = Mock()
    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()

    anti._detect_captcha_stage = Mock(
        side_effect=["refresh_click", "refresh_click", "refresh_click", "slider"]
    )
    anti._page_state_signature = Mock(side_effect=["sig_a", "sig_a", "sig_a", "sig_b"])

    with patch("crawler.anti_crawler.time.time", return_value=1234.5):
        result = anti._refresh_verification_page()

    assert result is True
    page.refresh.assert_called_once_with()
    page.run_cdp.assert_any_call("Page.reload", ignoreCache=True)
    page.run_cdp.assert_any_call("Page.stopLoading")
    page.get.assert_called_once_with("https://example.com/search?city=gz&_captcha_ts=1234500")


def test_refresh_verification_page_returns_false_when_refresh_and_fallback_still_stuck():
    anti = AntiCrawler()
    page = Mock()
    page.url = "https://example.com/search"
    page.wait.load_start = Mock()
    page.refresh = Mock()
    page.get = Mock()
    page.run_cdp = Mock()
    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()

    anti._detect_captcha_stage = Mock(
        side_effect=["refresh_click", "refresh_click", "refresh_click", "refresh_click"]
    )
    anti._page_state_signature = Mock(side_effect=["sig_a", "sig_a", "sig_a", "sig_a"])

    with patch("crawler.anti_crawler.time.time", return_value=55.2):
        result = anti._refresh_verification_page()

    assert result is False
    page.refresh.assert_called_once_with()
    page.run_cdp.assert_any_call("Page.reload", ignoreCache=True)
    page.run_cdp.assert_any_call("Page.stopLoading")
    page.get.assert_called_once_with("https://example.com/search?_captcha_ts=55200")


def test_refresh_verification_page_uses_hard_reload_without_url_fallback_when_effective():
    anti = AntiCrawler()
    page = Mock()
    page.url = "https://example.com/search"
    page.wait.load_start = Mock()
    page.refresh = Mock()
    page.get = Mock()
    page.run_cdp = Mock()
    anti.get_page = Mock(return_value=page)
    anti.random_delay = Mock()

    anti._detect_captcha_stage = Mock(side_effect=["refresh_click", "refresh_click", "slider"])
    anti._page_state_signature = Mock(side_effect=["sig_a", "sig_a", "sig_b"])

    with patch("crawler.anti_crawler.time.time", return_value=999.1):
        result = anti._refresh_verification_page()

    assert result is True
    page.refresh.assert_called_once_with()
    page.run_cdp.assert_any_call("Page.reload", ignoreCache=True)
    page.get.assert_not_called()


def test_refresh_effective_returns_false_when_refresh_click_stage_unchanged():
    assert (
        AntiCrawler._is_refresh_effective(
            previous_signature="sig_a",
            current_signature="sig_b",
            previous_stage="refresh_click",
            current_stage="refresh_click",
        )
        is False
    )


def test_detect_captcha_stage_ignores_global_keywords_without_captcha_container():
    anti = AntiCrawler()
    page = Mock()
    page.html = "验证失败，点击框体重试"
    page.ele = Mock(return_value=None)
    anti.get_page = Mock(return_value=page)
    anti.check_captcha = Mock(return_value=False)

    stage = anti._detect_captcha_stage()

    assert stage == "none"


def test_save_captcha_debug_artifacts_does_not_use_page_save_as_screenshot(tmp_path):
    anti = AntiCrawler()
    page = Mock()
    page.get_screenshot = Mock(side_effect=RuntimeError("primary screenshot failed"))
    page.screenshot = Mock(side_effect=RuntimeError("secondary screenshot failed"))
    page.save = Mock(side_effect=lambda path: Path(path).mkdir(parents=True, exist_ok=True))
    page.html = "<html>captcha</html>"
    anti.get_page = Mock(return_value=page)

    fake_settings = SimpleNamespace(
        captcha_debug_artifacts_enabled=True,
        captcha_debug_artifacts_path=tmp_path,
    )

    with patch("crawler.anti_crawler.settings", fake_settings):
        anti._save_captcha_debug_artifacts("screenshot_save_guard")

    assert list(tmp_path.glob("*.png")) == []
    assert len(list(tmp_path.glob("*.html"))) == 1
    page.save.assert_not_called()


def test_handle_captcha_raises_when_same_error_code_repeats_in_refresh_stage():
    anti = AntiCrawler()
    anti.check_captcha = Mock(side_effect=[True, True, True, True])
    anti._detect_captcha_stage = Mock(return_value="refresh_click")
    anti._extract_captcha_error_code = Mock(return_value="VP8Ddc")
    anti._click_verification_refresh = Mock(return_value=True)
    anti._refresh_verification_page = Mock(return_value=False)
    anti._captcha_attempt_interval_delay = Mock()
    anti.random_delay = Mock()
    anti._save_captcha_debug_artifacts = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        with pytest.raises(CaptchaTimeoutException):
            anti.handle_captcha()

    assert anti._extract_captcha_error_code.call_count == 3
    assert anti._captcha_error_code_streak == 3


def test_access_denied_detection_hits_modal_keywords():
    anti = AntiCrawler()
    modal = Mock()
    modal.text = "亲，访问被拒绝"
    modal.html = "<div>亲，访问被拒绝</div>"

    page = Mock()
    page.url = "https://hotel.fliggy.com/hotel_detail2.htm?shid=1"
    page.html = "<div>亲，访问被拒绝</div>"
    page.ele = Mock(side_effect=lambda selector, timeout=1: modal if selector == '#baxia-dialog-content' else None)
    anti.get_page = Mock(return_value=page)

    assert anti.is_access_denied_blocked(log_detected=False) is True


def test_detect_captcha_stage_returns_access_denied_before_generic():
    anti = AntiCrawler()
    anti.is_access_denied_blocked = Mock(return_value=True)
    anti.get_page = Mock(return_value=Mock())

    assert anti._detect_captcha_stage() == "access_denied"


def test_handle_captcha_uses_access_denied_recovery_branch():
    anti = AntiCrawler()
    anti.check_captcha = Mock(side_effect=[True, True, False])
    anti._detect_captcha_stage = Mock(return_value="access_denied")
    anti._recover_access_denied = Mock(return_value=True)
    anti._captcha_attempt_interval_delay = Mock()

    with patch("crawler.anti_crawler.time.time", return_value=0):
        anti.handle_captcha()

    anti._recover_access_denied.assert_called_once_with()
