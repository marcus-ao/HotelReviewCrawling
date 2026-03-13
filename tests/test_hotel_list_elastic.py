"""Tests for hotel-list elastic backfill accounting and callback behavior."""

from unittest.mock import Mock, patch

from crawler.hotel_list_crawler import HotelListCrawler


def test_extract_hotels_max_limit_triggers_page_callback_before_early_return():
    anti = Mock()
    page = Mock()
    page.wait = Mock()
    page.wait.ele_displayed = Mock()
    page.url = "https://hotel.fliggy.com/hotel_list3.htm"
    page.html = "<html></html>"

    anti.get_page.return_value = page
    anti.scroll_to_bottom = Mock()
    anti.check_captcha = Mock(return_value=False)

    crawler = HotelListCrawler(anti_crawler=anti)
    crawler._get_query_page_info = Mock(return_value={"currentPage": 1, "totalPage": 10})
    crawler._extract_hotels_from_query_data = Mock(
        return_value=[
            {"hotel_id": "h-1", "review_count": 10, "base_price": 320},
            {"hotel_id": "h-2", "review_count": 12, "base_price": 340},
        ]
    )

    callback_calls = []

    def page_callback(page_hotels, page_no):
        callback_calls.append((list(page_hotels), page_no))

    with patch("crawler.hotel_list_crawler.time.sleep", return_value=None):
        hotels = crawler.extract_hotels_from_page(
            max_hotels=1,
            min_review_count=0,
            price_range={"level": "舒适型", "min": 300, "max": 600},
            page_callback=page_callback,
        )

    assert len(hotels) == 1
    assert len(callback_calls) == 1
    assert callback_calls[0][1] == 1
    assert len(callback_calls[0][0]) == 1
    assert callback_calls[0][0][0]["hotel_id"] == hotels[0]["hotel_id"]


def test_elastic_backfill_uses_existing_progress_for_carry_over():
    anti = Mock()
    anti.random_delay = Mock()
    crawler = HotelListCrawler(anti_crawler=anti)

    target_calls = []

    def fake_crawl_by_zone_and_price(**kwargs):
        target_calls.append((kwargs["price_range"]["level"], kwargs["target_count"]))
        return []

    crawler.crawl_by_zone_and_price = Mock(side_effect=fake_crawl_by_zone_and_price)

    # Simulate DB progress by price level:
    # - 经济型: before=0, after=0 (still缺口4)
    # - 舒适型: before=8, after=10 (补齐了 carry-over 4 + base 6 => target 10)
    # - 高档型: before=0, after=0
    call_count = {"经济型": 0, "舒适型": 0, "高档型": 0}

    def fake_get_saved_hotel_ids(region_type, zone_code, price_level):
        call_count[price_level] += 1
        if price_level == "舒适型":
            if call_count[price_level] == 1:
                return {f"comfort-{i}" for i in range(8)}
            return {f"comfort-{i}" for i in range(10)}
        return set()

    crawler._get_saved_hotel_ids = Mock(side_effect=fake_get_saved_hotel_ids)

    business_zone = {"name": "琶洲国际会展中心商圈", "code": "39589"}
    price_ranges = [
        {"level": "经济型", "top_n": 4, "min": 0, "max": 300},
        {"level": "舒适型", "top_n": 6, "min": 300, "max": 600},
        {"level": "高档型", "top_n": 3, "min": 600, "max": 900},
    ]

    zone_hotels = crawler._crawl_business_zone_elastic(
        region_type="会展活动区",
        business_zone=business_zone,
        price_ranges=price_ranges,
        exclude_ids=set(),
        save_to_db=True,
    )

    assert zone_hotels == []
    assert target_calls[:3] == [
        ("经济型", 4),
        ("舒适型", 10),
        ("高档型", 3),
    ]


def test_elastic_backfill_skips_reverse_retry_for_exhausted_tiers():
    anti = Mock()
    anti.random_delay = Mock()
    crawler = HotelListCrawler(anti_crawler=anti)

    call_levels = []

    def fake_crawl_by_zone_and_price(**kwargs):
        call_levels.append(kwargs["price_range"]["level"])
        return []

    crawler.crawl_by_zone_and_price = Mock(side_effect=fake_crawl_by_zone_and_price)
    crawler._get_saved_hotel_ids = Mock(return_value=set())

    business_zone = {"name": "江南西/国际轻纺城/珠江南附近商圈", "code": "39588"}
    price_ranges = [
        {"level": "经济型", "top_n": 4, "min": 0, "max": 300},
        {"level": "舒适型", "top_n": 6, "min": 300, "max": 600},
        {"level": "高档型", "top_n": 3, "min": 600, "max": 900},
        {"level": "奢华型", "top_n": 2, "min": 900, "max": 99999},
    ]

    crawler._crawl_business_zone_elastic(
        region_type="会展活动区",
        business_zone=business_zone,
        price_ranges=price_ranges,
        exclude_ids=set(),
        save_to_db=True,
    )

    # All forward tiers are attempted once; reverse phase should skip re-crawling exhausted tiers.
    assert call_levels == ["经济型", "舒适型", "高档型", "奢华型"]
