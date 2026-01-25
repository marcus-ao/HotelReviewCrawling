"""爬虫模块测试"""
import pytest
from unittest.mock import Mock, patch

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
        assert clean_text(None) == ""

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
        hotel = HotelModel(**data)
        assert hotel.hotel_id == "10019773"
        assert hotel.name == "广州海航威斯汀酒店"

    def test_hotel_model_invalid_empty_id(self):
        """测试空酒店ID"""
        data = {
            "hotel_id": "",
            "name": "测试酒店",
        }
        with pytest.raises(ValueError):
            HotelModel(**data)

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
        review = ReviewModel(**data)
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
        review = ReviewModel(**data)
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
        assert "businessZone=39584" in url
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
