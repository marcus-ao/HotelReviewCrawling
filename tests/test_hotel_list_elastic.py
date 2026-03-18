"""Tests for hotel-list two-phase compensation behavior and callback handling."""

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


def test_two_phase_compensation_does_not_swing_back_to_origin_tier():
    anti = Mock()
    anti.random_delay = Mock()
    crawler = HotelListCrawler(anti_crawler=anti)

    target_calls = []

    def fake_crawl_by_zone_and_price(**kwargs):
        target_calls.append((kwargs["price_range"]["level"], kwargs["target_count"]))
        return []

    crawler.crawl_by_zone_and_price = Mock(side_effect=fake_crawl_by_zone_and_price)
    saved_state = {
        "经济型": {f"eco-{i}" for i in range(4)},
        "舒适型": {f"comfort-{i}" for i in range(6)},
        "高档型": {"high-0"},
        "奢华型": {"lux-0", "lux-1"},
    }

    def fake_get_saved_hotel_ids(region_type, zone_code, price_level):
        return set(saved_state.get(price_level, set()))

    crawler._get_saved_hotel_ids = Mock(side_effect=fake_get_saved_hotel_ids)

    business_zone = {"name": "珠江新城/五羊新城商圈", "code": "39584"}
    price_ranges = [
        {"level": "经济型", "top_n": 4, "min": 0, "max": 300},
        {"level": "舒适型", "top_n": 6, "min": 300, "max": 600},
        {"level": "高档型", "top_n": 4, "min": 600, "max": 900},
        {"level": "奢华型", "top_n": 2, "min": 900, "max": 99999},
    ]

    zone_hotels = crawler._crawl_business_zone_elastic(
        region_type="CBD商务区",
        business_zone=business_zone,
        price_ranges=price_ranges,
        exclude_ids=set(),
        save_to_db=True,
    )

    assert zone_hotels == []
    assert target_calls[:4] == [
        ("经济型", 4),
        ("舒适型", 6),
        ("高档型", 4),
        ("奢华型", 2),
    ]
    assert target_calls[4:] == [("奢华型", 1), ("舒适型", 2), ("经济型", 2)]
    assert all(level != "高档型" for level, _ in target_calls[4:])

def test_two_phase_compensation_attempts_each_candidate_at_most_once():
    anti = Mock()
    anti.random_delay = Mock()
    crawler = HotelListCrawler(anti_crawler=anti)

    call_levels = []

    def fake_crawl_by_zone_and_price(**kwargs):
        call_levels.append(kwargs["price_range"]["level"])
        return []

    crawler.crawl_by_zone_and_price = Mock(side_effect=fake_crawl_by_zone_and_price)
    saved_state = {
        "经济型": {f"eco-{i}" for i in range(4)},
        "舒适型": {f"comfort-{i}" for i in range(6)},
        "高档型": set(),
        "奢华型": {f"lux-{i}" for i in range(2)},
    }
    crawler._get_saved_hotel_ids = Mock(side_effect=lambda region_type, zone_code, price_level: set(saved_state.get(price_level, set())))

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

    assert call_levels == ["经济型", "舒适型", "高档型", "奢华型", "奢华型", "舒适型", "经济型"]


def test_two_phase_compensation_respects_borrow_cap():
    anti = Mock()
    anti.random_delay = Mock()
    crawler = HotelListCrawler(anti_crawler=anti)

    target_calls = []
    saved_state = {
        "经济型": {f"eco-{i}" for i in range(4)},
        "舒适型": {f"comfort-{i}" for i in range(6)},
        "高档型": {"high-0"},
        "奢华型": {"lux-0", "lux-1"},
    }

    def fake_crawl_by_zone_and_price(**kwargs):
        level = kwargs["price_range"]["level"]
        target_calls.append((level, kwargs["target_count"]))
        if level == "舒适型" and 0 < kwargs["target_count"] < 6:
            saved_state[level].add("comfort-extra")
            return [{"hotel_id": "comfort-extra"}]
        return []

    crawler.crawl_by_zone_and_price = Mock(side_effect=fake_crawl_by_zone_and_price)
    crawler._get_saved_hotel_ids = Mock(side_effect=lambda region_type, zone_code, price_level: set(saved_state.get(price_level, set())))

    with patch("crawler.hotel_list_crawler.settings.sampling_borrow_cap_ratio", 0.30):
        crawler._crawl_business_zone_elastic(
            region_type="CBD商务区",
            business_zone={"name": "珠江新城/五羊新城商圈", "code": "39584"},
            price_ranges=[
                {"level": "经济型", "top_n": 4, "min": 0, "max": 300},
                {"level": "舒适型", "top_n": 6, "min": 300, "max": 600},
                {"level": "高档型", "top_n": 4, "min": 600, "max": 900},
                {"level": "奢华型", "top_n": 2, "min": 900, "max": 99999},
            ],
            exclude_ids=set(),
            save_to_db=True,
        )

    # 仅高档型存在缺口时，候选请求不会超过高档型可借位上限 2。
    compensation_calls = target_calls[4:]
    assert compensation_calls == [("奢华型", 1), ("舒适型", 2), ("经济型", 1)]
    assert max(target for _, target in compensation_calls) <= 2


def test_two_phase_compensation_skips_exhausted_candidates():
    anti = Mock()
    anti.random_delay = Mock()
    crawler = HotelListCrawler(anti_crawler=anti)

    call_levels = []

    def fake_crawl_by_zone_and_price(**kwargs):
        level = kwargs["price_range"]["level"]
        call_levels.append(level)
        return []

    crawler.crawl_by_zone_and_price = Mock(side_effect=fake_crawl_by_zone_and_price)
    crawler._get_saved_hotel_ids = Mock(return_value=set())

    crawler._crawl_business_zone_elastic(
        region_type="会展活动区",
        business_zone={"name": "江南西/国际轻纺城/珠江南附近商圈", "code": "39588"},
        price_ranges=[
            {"level": "经济型", "top_n": 4, "min": 0, "max": 300},
            {"level": "舒适型", "top_n": 6, "min": 300, "max": 600},
            {"level": "高档型", "top_n": 3, "min": 600, "max": 900},
            {"level": "奢华型", "top_n": 2, "min": 900, "max": 99999},
        ],
        exclude_ids=set(),
        save_to_db=True,
    )

    # All tiers exhausted during main pass; compensation should skip candidates instead of retrying them.
    assert call_levels == ["经济型", "舒适型", "高档型", "奢华型"]
