"""爬虫模块测试"""
import pytest
from unittest.mock import Mock, patch
from contextlib import contextmanager
import json
from pathlib import Path

from utils.cleaner import clean_text, extract_tags, parse_star_score, parse_date, extract_price
from utils.validator import HotelModel, ReviewModel
from config.regions import GUANGZHOU_REGIONS, calculate_expected_hotels, get_all_business_zones


class TestCleaner:
    """数据清洗工具测试"""

    def test_clean_text_basic(self):
        """测试基本文本清洗"""
        text = "  这是一段测试文本  "
        result = clean_text(text)
        assert result == "这是一段测试文本"

    def test_clean_text_html(self):
        """测试HTML标签清洗"""
        text = "<p>这是<b>HTML</b>文本</p>"
        result = clean_text(text)
        assert "HTML" in result
        assert "<" not in result

    def test_clean_text_empty(self):
        """测试空文本"""
        assert clean_text("") == ""

    def test_extract_tags(self):
        """测试标签提取"""
        text = "#交通便利 #服务热情 这是一条评论"
        tags = extract_tags(text)
        assert "交通便利" in tags
        assert "服务热情" in tags

    def test_extract_tags_common(self):
        """测试常见标签提取"""
        text = "位置好，服务热情，干净卫生"
        tags = extract_tags(text)
        assert "位置好" in tags
        assert "服务热情" in tags
        assert "干净卫生" in tags

    def test_parse_star_score(self):
        """测试星级评分解析"""
        assert parse_star_score("width:100%") == 5.0
        assert parse_star_score("width:80%") == 4.0
        assert parse_star_score("width:60%") == 3.0
        assert parse_star_score("width:40%") == 2.0
        assert parse_star_score("width:20%") == 1.0
        assert parse_star_score("") == 0.0

    def test_parse_date(self):
        """测试日期解析"""
        assert parse_date("[2026-01-11 20:34]") == "2026-01-11 20:34:00"
        assert parse_date("2026-01-11") == "2026-01-11 00:00:00"
        assert parse_date("") is None

    def test_extract_price(self):
        """测试价格提取"""
        assert extract_price("¥857") == 857
        assert extract_price("857元") == 857
        assert extract_price("价格：1200") == 1200
        assert extract_price("") is None


class TestValidator:
    """数据验证模型测试"""

    def test_hotel_model_valid(self):
        """测试有效的酒店数据"""
        data = {
            "hotel_id": "10019773",
            "name": "广州海航威斯汀酒店",
            "address": "林和中路6号",
            "rating_score": 4.7,
            "review_count": 8155,
        }
        hotel = HotelModel.model_validate(data)
        assert hotel.hotel_id == "10019773"
        assert hotel.name == "广州海航威斯汀酒店"

    def test_hotel_model_invalid_empty_id(self):
        """测试空酒店ID"""
        data = {
            "hotel_id": "",
            "name": "测试酒店",
        }
        with pytest.raises(ValueError):
            HotelModel.model_validate(data)

    def test_review_model_valid(self):
        """测试有效的评论数据"""
        data = {
            "hotel_id": "10019773",
            "content": "这是一条测试评论",
            "score_clean": 4.0,
            "score_location": 5.0,
            "score_service": 4.0,
            "score_value": 3.0,
        }
        review = ReviewModel.model_validate(data)
        assert review.hotel_id == "10019773"
        assert review.content == "这是一条测试评论"

    def test_review_model_calculate_overall(self):
        """测试综合评分计算"""
        data = {
            "hotel_id": "10019773",
            "content": "测试评论",
            "score_clean": 4.0,
            "score_location": 5.0,
            "score_service": 4.0,
            "score_value": 3.0,
        }
        review = ReviewModel.model_validate(data)
        overall = review.calculate_overall_score()
        assert overall == 4.0  # (4+5+4+3)/4 = 4.0


class TestRegions:
    """功能区配置测试"""

    def test_regions_count(self):
        """测试功能区数量"""
        assert len(GUANGZHOU_REGIONS) == 6

    def test_regions_structure(self):
        """测试功能区结构"""
        for region_name, region_data in GUANGZHOU_REGIONS.items():
            assert "business_zones" in region_data
            assert "price_ranges" in region_data
            assert len(region_data["business_zones"]) >= 1
            assert len(region_data["price_ranges"]) == 4

    def test_business_zones_have_code(self):
        """测试商圈都有代码"""
        zones = get_all_business_zones()
        for zone in zones:
            assert "zone_code" in zone
            assert zone["zone_code"] is not None

    def test_calculate_expected_hotels(self):
        """测试预期酒店数量计算"""
        expected = calculate_expected_hotels()
        assert "total" in expected
        assert "breakdown" in expected
        # 6个功能区 × 3个商圈 × 15家酒店 = 270家
        assert expected["total"] == 270


class TestAntiCrawler:
    """反爬虫模块测试（模拟测试）"""

    def test_random_delay_range(self):
        """测试随机延迟范围"""
        from crawler.anti_crawler import AntiCrawler
        import time

        anti_crawler = AntiCrawler()

        # 使用mock避免实际延迟
        with patch('time.sleep') as mock_sleep:
            anti_crawler.random_delay(1, 2)
            mock_sleep.assert_called_once()
            delay = mock_sleep.call_args[0][0]
            assert 1 <= delay <= 2


class TestHotelListCrawler:
    """酒店列表爬虫测试（模拟测试）"""

    def test_build_search_url(self):
        """测试搜索URL构建"""
        from crawler.hotel_list_crawler import HotelListCrawler

        crawler = HotelListCrawler(anti_crawler=Mock())
        url = crawler.build_search_url(
            city_code="440100",
            business_zone_code="39584",
            price_min=300,
            price_max=600,
        )

        assert "city=440100" in url
        assert "businessAreaId=39584" in url
        assert "priceRange=300-600" in url


class TestReviewCrawler:
    """评论爬虫测试（模拟测试）"""

    def test_get_hotel_detail_url(self):
        """测试酒店详情URL生成"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        url = crawler.get_hotel_detail_url("10019773")

        assert "10019773" in url
        assert "hotel_detail2.htm" in url

    def test_click_with_fallback_uses_js_mode(self):
        """当原生点击失败时，回退到by_js点击。"""
        from crawler.review_crawler import ReviewCrawler

        class DummyElement:
            def __init__(self):
                self.calls = []

            def click(self, **kwargs):
                self.calls.append(kwargs)
                if not kwargs:
                    raise Exception("native click failed")

        class DummyPage:
            def run_js(self, _script):
                return True

        crawler = ReviewCrawler(anti_crawler=Mock())
        clicked = crawler._click_with_fallback(
            page=DummyPage(),
            element=DummyElement(),
            selector="#review-addreply",
            action="unit_test_click",
            hotel_id="10001",
            source_pool="positive",
            progress="0/10",
        )

        assert clicked is True

    def test_parse_review_payload_from_json_reviews_key(self):
        """网络响应中包含reviews数组时应成功映射评论。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        payload = {
            "reviews": [
                {
                    "reviewId": "r1",
                    "content": "房间干净，服务很好",
                    "userNick": "Alice",
                    "createTime": "2026-03-01",
                    "picList": ["https://img.example.com/1.jpg"],
                }
            ]
        }

        reviews = crawler._parse_review_payload(payload, "10001", "positive")

        assert len(reviews) == 1
        assert reviews[0]["hotel_id"] == "10001"
        assert reviews[0]["source_pool"] == "positive"
        assert reviews[0]["content"] == "房间干净，服务很好"
        assert "effective_length" in reviews[0]
        assert "quality_tier" in reviews[0]

    def test_parse_review_payload_from_jsonp_string(self):
        """JSONP字符串应被解包并解析评论。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        payload = 'callback({"data": {"list": [{"id": "x2", "rateContent": "位置不错", "nick": "Bob"}]}})'

        reviews = crawler._parse_review_payload(payload, "20002", "negative")

        assert len(reviews) == 1
        assert reviews[0]["content"] == "位置不错"
        assert reviews[0]["hotel_id"] == "20002"
        assert "effective_length" in reviews[0]
        assert "quality_tier" in reviews[0]

    def test_extract_reviews_from_network_deduplicates_existing_ids(self):
        """网络回退提取应复用crawled_review_ids进行去重。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        crawler._capture_review_packets = Mock(
            return_value=[
                {
                    "body": {
                        "reviews": [
                            {"reviewId": "r1", "content": "a", "userNick": "u1"},
                            {"reviewId": "r2", "content": "b", "userNick": "u2"},
                        ]
                    }
                }
            ]
        )
        crawler.crawled_review_ids.add("10001_r1")

        reviews = crawler._extract_reviews_from_network("10001", "positive", "0/10")

        assert len(reviews) == 1
        assert reviews[0]["review_id"] == "10001_r2"

    def test_reactivate_review_runtime_calls_focus_presence_and_wait(self):
        """评论运行时重激活应串联输入层、页面层和ready等待。"""
        from crawler.review_crawler import ReviewCrawler

        anti = Mock()
        anti.ensure_page_foreground = Mock()
        anti.simulate_human_presence = Mock()

        crawler = ReviewCrawler(anti_crawler=anti)
        crawler._trigger_review_lazy_load = Mock()
        crawler._wait_review_module_ready = Mock(return_value=True)

        result = crawler._reactivate_review_runtime("10001", "positive", "0/10", "unit_test")

        assert result is True
        anti.ensure_page_foreground.assert_called_once()
        anti.simulate_human_presence.assert_called_once()
        crawler._trigger_review_lazy_load.assert_called_once()
        crawler._wait_review_module_ready.assert_called_once()

    def test_capture_review_packets_retries_with_rewarm_after_empty_first_attempt(self):
        """首轮未抓到包时，应进行会话再暖并重试。"""
        from crawler.review_crawler import ReviewCrawler

        class DummyListen:
            def __init__(self):
                self.wait_calls = 0
                self.starts = []

            def start(self, **kwargs):
                self.starts.append(kwargs)

            def stop(self):
                return None

            def wait(self, timeout=0, fit_count=False):
                self.wait_calls += 1
                if self.wait_calls <= 2:
                    return None
                packet = Mock()
                packet.url = "https://hotel.fliggy.com/ajax/getReviews.do"
                packet.method = "GET"
                packet.response = Mock(status=200, body={"reviews": []})
                return packet

        page = Mock()
        page.listen = DummyListen()

        anti = Mock()
        anti.get_page.return_value = page
        anti.warm_session = Mock()
        anti.post_captcha_stabilize = Mock()

        crawler = ReviewCrawler(anti_crawler=anti)
        crawler._reactivate_review_runtime = Mock(return_value=True)
        crawler._force_open_review_detail_page = Mock(return_value=True)

        with patch("crawler.review_crawler.settings", Mock(review_request_capture_attempts=2)):
            packets = crawler._capture_review_packets("10001", "positive", "0/10", max_packets=2, wait_per_packet=0.01)

        assert len(packets) == 2
        anti.warm_session.assert_called_once()
        anti.post_captcha_stabilize.assert_called_once()
        crawler._force_open_review_detail_page.assert_called_once()

    def test_crawl_positive_pool_manual_stops_after_manual_quit_on_failed_advance(self):
        """自动翻页失败后若人工选择退出，应保留已抓取页面并结束。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        crawler.navigate_to_reviews = Mock(return_value=True)
        crawler._ensure_positive_manual_entry = Mock()
        crawler._ensure_review_page_ready = Mock()
        crawler._get_manual_page_state = Mock(return_value={"signature": ("p1",), "filter_text": "好评", "js_count": 15, "preview_items": []})
        crawler._log_manual_positive_page_state = Mock()
        crawler._save_manual_positive_snapshot = Mock(return_value=None)
        crawler._log_manual_positive_page_result = Mock()
        crawler.extract_reviews_from_page = Mock(
            return_value=[{"review_id": "r1", "content": "房间干净，位置方便，早餐不错", "overall_score": 5.0}]
        )
        crawler._auto_advance_positive_page = Mock(
            return_value=(False, {"signature": ("p1",), "filter_text": "好评", "js_count": 15, "preview_items": []}, "page_unchanged")
        )
        crawler._request_manual_positive_takeover = Mock(return_value="quit")

        reviews = crawler._crawl_positive_pool_manual("10001", max_count=10, save_to_db=False)

        assert len(reviews) == 1
        assert reviews[0]["review_id"] == "r1"
        assert reviews[0]["overall_score"] == 5.0
        assert "quality_tier" in reviews[0]
        crawler._request_manual_positive_takeover.assert_called_once()

    def test_crawl_positive_pool_manual_filters_non_positive_reviews(self):
        """人工正向采集只保留综合评分>=4的评论。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        crawler.navigate_to_reviews = Mock(return_value=True)
        crawler._ensure_positive_manual_entry = Mock()
        crawler._ensure_review_page_ready = Mock()
        crawler._get_manual_page_state = Mock(
            side_effect=[
                {"signature": ("p1",), "filter_text": "好评", "js_count": 15, "preview_items": []},
                {"signature": ("p2",), "filter_text": "好评", "js_count": 15, "preview_items": []},
                {"signature": ("p2",), "filter_text": "好评", "js_count": 15, "preview_items": []},
            ]
        )
        crawler._log_manual_positive_page_state = Mock()
        crawler._save_manual_positive_snapshot = Mock(return_value=None)
        crawler._log_manual_positive_page_result = Mock()
        crawler.extract_reviews_from_page = Mock(
            side_effect=[
                [
                    {"review_id": "r1", "content": "房间干净，服务很好，早餐不错", "overall_score": 4.5},
                    {"review_id": "r2", "content": "一般", "overall_score": 3.0},
                ],
            ]
        )
        crawler._auto_advance_positive_page = Mock(return_value=(True, {"signature": ("p2",), "filter_text": "好评", "js_count": 15, "preview_items": []}, "ok"))

        reviews = crawler._crawl_positive_pool_manual("10001", max_count=1, save_to_db=False)

        assert len(reviews) == 1
        assert reviews[0]["review_id"] == "r1"

    def test_crawl_positive_pool_manual_advances_even_when_current_page_has_no_new_reviews(self):
        """当前页无新增评论时也应继续自动翻页，避免页码前进但页面停在原地。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        crawler.navigate_to_reviews = Mock(return_value=True)
        crawler._ensure_positive_manual_entry = Mock(return_value=False)
        crawler._ensure_review_page_ready = Mock()
        crawler._get_manual_page_state = Mock(
            side_effect=[
                {"signature": ("p1",), "filter_text": "全部", "js_count": 15, "preview_items": []},
                {"signature": ("p1",), "filter_text": "全部", "js_count": 15, "preview_items": []},
                {"signature": ("p2",), "filter_text": "全部", "js_count": 15, "preview_items": []},
                {"signature": ("p2",), "filter_text": "全部", "js_count": 15, "preview_items": []},
            ]
        )
        crawler._log_manual_positive_page_state = Mock()
        crawler._save_manual_positive_snapshot = Mock(return_value=None)
        crawler._log_manual_positive_page_result = Mock()
        crawler.extract_reviews_from_page = Mock(side_effect=[[], []])
        crawler._auto_advance_positive_page = Mock(
            return_value=(True, {"signature": ("p2",), "filter_text": "全部", "js_count": 15, "preview_items": []}, "ok")
        )
        crawler._request_manual_positive_takeover = Mock(return_value="quit")

        reviews = crawler._crawl_positive_pool_manual("10001", max_count=10, save_to_db=False)

        assert reviews == []
        assert crawler._auto_advance_positive_page.call_count >= 1
        first_call = crawler._auto_advance_positive_page.call_args_list[0]
        assert first_call.kwargs["page_no"] == 1

    def test_request_manual_positive_takeover_info_reprints_state(self):
        """人工接管提示输入 info 时应重新输出当前页状态。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        crawler._get_manual_page_state = Mock(
            side_effect=[
                {"signature": ("p1",), "filter_text": "好评", "js_count": 15, "preview_items": []},
                {"signature": ("p1",), "filter_text": "好评", "js_count": 15, "preview_items": []},
            ]
        )
        crawler._log_manual_positive_page_state = Mock()

        with patch("builtins.input", side_effect=["i", "q"]):
            action = crawler._request_manual_positive_takeover(
                hotel_id="10001",
                page_no=2,
                current_count=15,
                target_count=50,
                current_state={"signature": ("p1",), "filter_text": "好评", "js_count": 15, "preview_items": []},
            )

        assert action == "quit"
        assert crawler._log_manual_positive_page_state.call_count >= 1

    def test_describe_manual_page_transition_contains_before_after(self):
        """人工翻页差异描述应包含前后摘要。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        before = {
            "preview_items": [
                {"review_date": "2026-03-01 00:00:00", "overall_score": 5.0, "preview": "before text"}
            ]
        }
        after = {
            "preview_items": [
                {"review_date": "2026-03-02 00:00:00", "overall_score": 4.0, "preview": "after text"}
            ]
        }

        result = crawler._describe_manual_page_transition(before, after)

        assert "before=" in result
        assert "after=" in result
        assert "before text" in result
        assert "after text" in result

    def test_build_manual_positive_status_line_contains_core_fields(self):
        """人工正向采集状态栏应包含核心字段。"""
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        state = {"filter_text": "好评", "js_count": 15}

        result = crawler._build_manual_positive_status_line(
            page_no=3,
            current_state=state,
            raw_count=15,
            kept_count=6,
            total_count=28,
        )

        assert "page=3" in result
        assert "filter=好评" in result
        assert "js=15" in result
        assert "raw=15" in result
        assert "kept=6" in result
        assert "total=28" in result

    def test_find_positive_next_button_supports_quickbuy_selector(self):
        """正向自动翻页应能识别飞猪点评页的 next-page J_quickbuyPage 按钮。"""
        from crawler.review_crawler import ReviewCrawler

        page = Mock()
        button = Mock()
        page.ele.side_effect = lambda selector, timeout=1: button if selector == 'a.next-page.J_quickbuyPage' else None
        page.run_js = Mock(return_value="")

        crawler = ReviewCrawler(anti_crawler=Mock(), positive_manual=True)
        found, selector = crawler._find_positive_next_button(page)

        assert found is button
        assert selector == 'a.next-page.J_quickbuyPage'

    def test_save_reviews_persists_master_and_positive_subtable(self):
        """保存正向评论时应写入 reviews 与 reviews_positive。"""
        from crawler.review_crawler import ReviewCrawler
        from database.models import Review, ReviewPositive

        session = Mock()
        session.query.return_value.filter.return_value.all.return_value = []
        session.flush.return_value = None
        added_items = []

        def _capture_add(item):
            added_items.append(item)

        session.add.side_effect = _capture_add

        @contextmanager
        def fake_session_scope():
            yield session

        crawler = ReviewCrawler(anti_crawler=Mock())
        review = {
            "review_id": "r1",
            "hotel_id": "10019773",
            "content": "整体不错，入住体验很好",
            "source_pool": "positive",
        }

        with patch("crawler.review_crawler.session_scope", fake_session_scope):
            saved = crawler.save_reviews([review])

        assert saved == 1
        assert any(isinstance(item, Review) for item in added_items)
        assert any(isinstance(item, ReviewPositive) for item in added_items)
        assert len(added_items) == 2

    def test_effective_length_counts_cjk_as_double_and_ignores_punctuation(self):
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())

        assert crawler._effective_length("位置方便!!!") == 8
        assert crawler._effective_length("WiFi快") == 6
        assert crawler._effective_length("  ") == 0

    def test_build_review_quality_metadata_marks_generic_short_comment(self):
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        metadata = crawler._build_review_quality_metadata(
            content="不错",
            summary=None,
            source_pool="positive",
        )

        assert metadata["is_generic_short"] is True
        assert metadata["quality_tier"] == "D"
        assert metadata["effective_length"] == 4

    def test_build_review_quality_metadata_keeps_specific_negative_short_comment(self):
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        metadata = crawler._build_review_quality_metadata(
            content="隔音很差",
            summary=None,
            source_pool="negative",
        )

        assert metadata["effective_length"] == 8
        assert metadata["aspect_hit_count"] >= 1
        assert metadata["quality_tier"] == "C"
        assert metadata["is_specific_short"] is True

    def test_apply_quality_selection_relaxes_low_volume_hotel_and_rejects_generic_short(self):
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        reviews = []
        for review_id, content in [
            ("r1", "房间干净，位置方便，早餐不错，入住体验很好"),
            ("r2", "位置方便"),
            ("r3", "不错"),
        ]:
            review = {
                "review_id": review_id,
                "hotel_id": "10001",
                "content": content,
                "summary": None,
                "source_pool": "positive",
            }
            review.update(
                crawler._build_review_quality_metadata(
                    content=content,
                    summary=None,
                    source_pool="positive",
                )
            )
            reviews.append(review)

        accepted, stats = crawler._apply_quality_selection(
            reviews,
            hotel_review_count=220,
            source_pool="positive",
            target_count=3,
        )

        assert {item["review_id"] for item in accepted} == {"r1", "r2"}
        assert stats["quality_relaxed_used"] is True
        assert stats["filtered_quality_total"] == 1

    def test_record_last_crawl_summary_merges_quality_stats(self):
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())
        crawler._quality_pool_stats = {
            "negative": {
                "candidate_total": 20,
                "accepted_total": 12,
                "filtered_quality_total": 8,
                "high_quality_count": 8,
                "short_comment_count": 3,
                "specific_short_count": 2,
                "short_comment_ratio": 0.25,
                "high_quality_ratio": 0.67,
                "quality_relaxed_used": True,
                "quality_fallback_used": False,
            },
            "positive": {
                "candidate_total": 30,
                "accepted_total": 18,
                "filtered_quality_total": 12,
                "high_quality_count": 15,
                "short_comment_count": 2,
                "specific_short_count": 0,
                "short_comment_ratio": 0.11,
                "high_quality_ratio": 0.83,
                "quality_relaxed_used": False,
                "quality_fallback_used": True,
            },
        }

        summary = crawler._record_last_crawl_summary(
            "10001",
            review_count=500,
            target_total=30,
            target_negative=8,
            target_positive=22,
            reviews=[
                {"review_id": "n1", "source_pool": "negative"},
                {"review_id": "p1", "source_pool": "positive"},
            ],
        )

        assert summary["candidate_total"] == 50
        assert summary["accepted_total"] == 30
        assert summary["filtered_quality_total"] == 20
        assert summary["short_comment_count"] == 5
        assert summary["specific_short_count"] == 2
        assert summary["quality_relaxed_used"] is True
        assert summary["quality_fallback_used"] is True

    def test_calculate_review_targets_scales_with_review_count(self):
        from crawler.review_crawler import ReviewCrawler

        crawler = ReviewCrawler(anti_crawler=Mock())

        with patch("crawler.review_crawler.settings") as fake_settings:
            fake_settings.review_total_sample_ratio = 0.20
            fake_settings.review_total_min_per_hotel = 80
            fake_settings.review_total_max_per_hotel = 300
            fake_settings.max_reviews_per_hotel = 300
            fake_settings.review_negative_target_ratio = 0.25
            fake_settings.review_negative_min_per_hotel = 20
            fake_settings.review_negative_max_per_hotel = 80
            fake_settings.review_negative_pool_limit = 100

            assert crawler._calculate_review_targets(220) == {
                "review_count": 220,
                "target_total": 80,
                "target_negative": 20,
                "target_positive": 60,
            }
            assert crawler._calculate_review_targets(500) == {
                "review_count": 500,
                "target_total": 100,
                "target_negative": 25,
                "target_positive": 75,
            }
            assert crawler._calculate_review_targets(1200) == {
                "review_count": 1200,
                "target_total": 240,
                "target_negative": 60,
                "target_positive": 180,
            }
            assert crawler._calculate_review_targets(4000) == {
                "review_count": 4000,
                "target_total": 300,
                "target_negative": 75,
                "target_positive": 225,
            }

    def test_write_review_batch_report_persists_expected_summary(self):
        import main as app_main

        report_dir = Path("logs") / "test_review_reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "generated_at": "2026-03-18T12:00:00",
            "totals": {"actual_negative": 10, "actual_positive": 30},
            "hotels": [{"hotel_id": "1001", "actual_total": 40}],
        }

        fake_settings = Mock()
        fake_settings.log_path = report_dir

        with patch("main.settings", fake_settings):
            report_path = app_main._write_review_batch_report(report)

        assert report_path is not None
        assert report_path.exists()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["totals"]["actual_negative"] == 10
        assert saved["hotels"][0]["hotel_id"] == "1001"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
