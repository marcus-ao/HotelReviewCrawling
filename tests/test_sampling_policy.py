"""Sampling policy tests for sparse strata compensation."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

from crawler.hotel_list_crawler import HotelListCrawler


def test_apply_region_floor_policy_uses_relaxed_threshold_when_needed():
    crawler = HotelListCrawler(anti_crawler=Mock())

    fake_settings = SimpleNamespace(
        sampling_policy_enabled=True,
        sampling_sparse_tier_levels=["高档型"],
        sampling_threshold_steps=[200, 150, 120],
        min_reviews_threshold=200,
    )

    crawl_calls = []

    def fake_incremental(*, region_type, price_level, needed, min_review_count, sample_source, borrow_for_region=None):
        crawl_calls.append((region_type, price_level, needed, min_review_count, sample_source))
        if min_review_count == 200:
            return [{"hotel_id": "h1"}]
        return [{"hotel_id": "h2"}, {"hotel_id": "h3"}]

    with patch("crawler.hotel_list_crawler.settings", fake_settings):
        crawler._get_region_tier_target = Mock(return_value=9)
        crawler._get_region_tier_floor = Mock(return_value=6)
        crawler._count_region_tier_hotels = Mock(side_effect=[3, 4, 6])
        crawler._crawl_region_tier_incremental = Mock(side_effect=fake_incremental)

        hotels = crawler._apply_region_floor_policy("会展活动区")

    assert len(hotels) == 3
    assert crawl_calls[0][3] == 200
    assert crawl_calls[0][4] == "in_region"
    assert crawl_calls[1][3] == 150
    assert crawl_calls[1][4] == "relaxed_threshold"


def test_apply_city_compensation_policy_respects_borrow_cap_and_donor_guard():
    crawler = HotelListCrawler(anti_crawler=Mock())

    fake_settings = SimpleNamespace(
        sampling_policy_enabled=True,
        sampling_city_compensation_enabled=True,
        sampling_sparse_tier_levels=["高档型"],
        sampling_borrow_cap_ratio=0.30,
        sampling_donor_guard_ratio=0.10,
        sampling_threshold_steps=[200],
    )

    mock_regions = {
        "R1": {"business_zones": [], "price_ranges": []},
        "R2": {"business_zones": [], "price_ranges": []},
    }

    def fake_target(region_type, price_level):
        return 10

    def fake_floor(region_type, price_level):
        return 7

    def fake_actual(region_type, price_level):
        return {"R1": 6, "R2": 12}[region_type]

    crawl_calls = []

    def fake_incremental(*, region_type, price_level, needed, min_review_count, sample_source, borrow_for_region=None):
        crawl_calls.append((region_type, price_level, needed, min_review_count, sample_source, borrow_for_region))
        return [{"hotel_id": f"{region_type}-{i}"} for i in range(needed)]

    with patch("crawler.hotel_list_crawler.settings", fake_settings), patch(
        "crawler.hotel_list_crawler.GUANGZHOU_REGIONS",
        mock_regions,
    ):
        crawler._get_region_tier_target = Mock(side_effect=fake_target)
        crawler._get_region_tier_floor = Mock(side_effect=fake_floor)
        crawler._count_region_tier_hotels = Mock(side_effect=fake_actual)
        crawler._crawl_region_tier_incremental = Mock(side_effect=fake_incremental)

        hotels = crawler._apply_city_compensation_policy()

    # R1 deficit=4, borrow_cap=3 => max借调3; R2 donor_surplus=12-7-1=4
    assert len(hotels) == 3
    assert len(crawl_calls) == 1
    assert crawl_calls[0][0] == "R2"
    assert crawl_calls[0][2] == 3
    assert crawl_calls[0][4] == "cross_region_borrow"
    assert crawl_calls[0][5] == "R1"
