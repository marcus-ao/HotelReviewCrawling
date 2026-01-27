"""酒店列表爬虫模块

负责按功能区和价格档次爬取酒店列表，实现分层抽样策略。
"""
import re
from typing import Optional, Generator
from datetime import datetime, timedelta

from config.settings import settings
from config.regions import GUANGZHOU_REGIONS, PRICE_RANGES
from utils.logger import get_logger
from utils.cleaner import clean_text, extract_price, normalize_hotel_name
from utils.validator import HotelModel
from database.connection import session_scope
from database.models import Hotel, CrawlTask
from .anti_crawler import AntiCrawler

logger = get_logger("hotel_list_crawler")


class HotelListCrawler:
    """酒店列表爬虫类"""

    # 飞猪酒店列表URL模板
    BASE_URL = "https://hotel.fliggy.com/hotel_list3.htm"

    def __init__(self, anti_crawler: AntiCrawler = None):
        """初始化爬虫

        Args:
            anti_crawler: 反爬虫实例，如果不提供则创建新实例
        """
        self.anti_crawler = anti_crawler or AntiCrawler()
        self.page = None

    def build_search_url(
        self,
        city_code: str = "440100",
        business_zone_code: str = None,
        price_min: int = None,
        price_max: int = None,
        sort_by: str = "default",
        check_in: str = None,
        check_out: str = None,
    ) -> str:
        """构建搜索URL

        Args:
            city_code: 城市代码（广州: 440100）
            business_zone_code: 商圈代码
            price_min: 最低价格
            price_max: 最高价格
            sort_by: 排序方式 (default/sales/score/price)
            check_in: 入住日期
            check_out: 离店日期

        Returns:
            完整的搜索URL
        """
        # 默认日期为明天和后天
        if not check_in:
            check_in = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        if not check_out:
            check_out = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

        params = [
            f"city={city_code}",
            f"checkIn={check_in}",
            f"checkOut={check_out}",
        ]

        # 商圈筛选
        if business_zone_code:
            params.append(f"businessZone={business_zone_code}")

        # 价格筛选
        if price_min is not None and price_max is not None:
            params.append(f"priceRange={price_min}-{price_max}")

        # 排序方式
        sort_map = {
            "default": "",
            "sales": "sortType=1",
            "score": "sortType=2",
            "price": "sortType=3",
        }
        if sort_by in sort_map and sort_map[sort_by]:
            params.append(sort_map[sort_by])

        url = f"{self.BASE_URL}?{'&'.join(params)}"
        return url

    def extract_hotels_from_page(self) -> list[dict]:
        """从当前页面提取酒店信息

        Returns:
            酒店信息列表
        """
        page = self.anti_crawler.get_page()
        hotels = []

        # 等待酒店列表加载
        page.wait.ele_displayed('.list-row.J_ListRow', timeout=10)

        # 滚动页面以加载所有酒店
        self.anti_crawler.scroll_to_bottom(step=500, max_scrolls=10)

        # 查找所有酒店列表项
        hotel_elements = page.eles('.list-row.J_ListRow')
        logger.info(f"找到 {len(hotel_elements)} 家酒店")

        for elem in hotel_elements:
            try:
                hotel_data = self._parse_hotel_element(elem)
                if hotel_data:
                    hotels.append(hotel_data)
            except Exception as e:
                logger.warning(f"解析酒店元素失败: {e}")
                continue

        return hotels

    def _parse_hotel_element(self, elem) -> Optional[dict]:
        """解析单个酒店元素

        Args:
            elem: 酒店DOM元素

        Returns:
            酒店数据字典
        """
        try:
            # 从data属性提取基本信息
            hotel_id = elem.attr('data-shid')
            name = elem.attr('data-name')
            latitude = elem.attr('data-lat')
            longitude = elem.attr('data-lng')

            if not hotel_id or not name:
                return None

            # 清洗名称
            name = normalize_hotel_name(name)

            # 提取评分
            rating_score = None
            score_elem = elem.ele('.comment-score .score', timeout=1)
            if score_elem:
                try:
                    rating_score = float(score_elem.text.strip())
                except (ValueError, AttributeError):
                    pass

            # 提取评论数
            review_count = None
            review_elem = elem.ele('.comment-score .count', timeout=1)
            if review_elem:
                count_text = review_elem.text
                match = re.search(r'(\d+)', count_text)
                if match:
                    review_count = int(match.group(1))

            # 提取价格
            base_price = None
            price_elem = elem.ele('.pi-price', timeout=1)
            if price_elem:
                base_price = extract_price(price_elem.text)

            # 提取地址
            address = None
            addr_elem = elem.ele('.row-address', timeout=1)
            if addr_elem:
                address = clean_text(addr_elem.text)

            # 提取星级
            star_level = None
            star_elem = elem.ele('.row-subtitle', timeout=1)
            if star_elem:
                star_level = star_elem.attr('title') or star_elem.text

            return {
                'hotel_id': hotel_id,
                'name': name,
                'address': address,
                'latitude': float(latitude) if latitude else None,
                'longitude': float(longitude) if longitude else None,
                'star_level': star_level,
                'rating_score': rating_score,
                'review_count': review_count,
                'base_price': base_price,
            }

        except Exception as e:
            logger.debug(f"解析酒店元素异常: {e}")
            return None

    def crawl_by_zone_and_price(
        self,
        region_type: str,
        business_zone: dict,
        price_range: dict,
        save_to_db: bool = True,
    ) -> list[dict]:
        """按商圈和价格档次爬取酒店

        Args:
            region_type: 功能区类型
            business_zone: 商圈配置 {"name": "xxx", "code": "xxx"}
            price_range: 价格档次配置
            save_to_db: 是否保存到数据库

        Returns:
            爬取到的酒店列表
        """
        zone_name = business_zone['name']
        zone_code = business_zone['code']
        price_level = price_range['level']
        top_n = price_range['top_n']
        sort_by = price_range.get('sort', 'default')

        logger.info(f"开始爬取: {region_type} - {zone_name} - {price_level}")

        # 构建URL
        url = self.build_search_url(
            business_zone_code=zone_code,
            price_min=price_range['min'],
            price_max=price_range['max'],
            sort_by=sort_by,
        )

        # 导航到页面
        if not self.anti_crawler.navigate_to(url):
            logger.error(f"导航失败: {url}")
            return []

        # 等待页面加载
        self.anti_crawler.random_delay(2, 4)

        # 提取酒店
        hotels = self.extract_hotels_from_page()

        # 取Top N
        hotels = hotels[:top_n]

        # 添加分层信息
        for hotel in hotels:
            hotel['region_type'] = region_type
            hotel['business_zone'] = zone_name
            hotel['business_zone_code'] = zone_code
            hotel['price_level'] = price_level
            hotel['city_code'] = '440100'

        logger.info(f"爬取完成: {len(hotels)} 家酒店")

        # 保存到数据库
        if save_to_db and hotels:
            self._save_hotels(hotels)

        return hotels

    def crawl_region(self, region_type: str, save_to_db: bool = True) -> list[dict]:
        """爬取整个功能区的酒店

        Args:
            region_type: 功能区类型
            save_to_db: 是否保存到数据库

        Returns:
            爬取到的所有酒店
        """
        if region_type not in GUANGZHOU_REGIONS:
            logger.error(f"未知的功能区: {region_type}")
            return []

        region_config = GUANGZHOU_REGIONS[region_type]
        all_hotels = []

        for zone in region_config['business_zones']:
            for price_range in region_config['price_ranges']:
                hotels = self.crawl_by_zone_and_price(
                    region_type=region_type,
                    business_zone=zone,
                    price_range=price_range,
                    save_to_db=save_to_db,
                )
                all_hotels.extend(hotels)

                # 每次爬取后随机延迟
                self.anti_crawler.random_delay(3, 6)

        logger.info(f"功能区 {region_type} 爬取完成，共 {len(all_hotels)} 家酒店")
        return all_hotels

    def crawl_all_regions(self, save_to_db: bool = True) -> list[dict]:
        """爬取所有功能区的酒店

        Args:
            save_to_db: 是否保存到数据库

        Returns:
            爬取到的所有酒店
        """
        all_hotels = []

        for region_type in GUANGZHOU_REGIONS.keys():
            hotels = self.crawl_region(region_type, save_to_db)
            all_hotels.extend(hotels)

            # 功能区之间增加较长延迟
            self.anti_crawler.random_delay(5, 10)

        logger.info(f"全部爬取完成，共 {len(all_hotels)} 家酒店")
        return all_hotels

    def _save_hotels(self, hotels: list[dict]) -> int:
        """保存酒店到数据库（批量去重优化）

        Args:
            hotels: 酒店数据列表

        Returns:
            成功保存的数量
        """
        if not hotels:
            return 0

        saved_count = 0
        updated_count = 0

        with session_scope() as session:
            # 批量查询已存在的酒店ID（减少数据库查询次数）
            hotel_ids = [h['hotel_id'] for h in hotels if 'hotel_id' in h]
            existing_hotels = session.query(Hotel).filter(
                Hotel.hotel_id.in_(hotel_ids)
            ).all()
            
            # 构建已存在酒店的字典，方便快速查找
            existing_dict = {h.hotel_id: h for h in existing_hotels}
            
            logger.debug(f"批量查询: {len(hotel_ids)}个酒店ID, 已存在{len(existing_dict)}个")

            for hotel_data in hotels:
                try:
                    # 验证数据
                    validated = HotelModel(**hotel_data)

                    if validated.hotel_id in existing_dict:
                        # 更新现有记录
                        existing = existing_dict[validated.hotel_id]
                        for key, value in validated.model_dump().items():
                            if value is not None:
                                setattr(existing, key, value)
                        updated_count += 1
                        logger.debug(f"更新酒店: {validated.name}")
                    else:
                        # 创建新记录
                        hotel = Hotel(**validated.model_dump())
                        session.add(hotel)
                        saved_count += 1
                        logger.debug(f"新增酒店: {validated.name}")

                except Exception as e:
                    logger.warning(f"保存酒店失败: {e}")
                    continue

        total = saved_count + updated_count
        logger.info(f"保存完成: 新增{saved_count}家, 更新{updated_count}家, 共{total}家")
        return total

    def get_hotels_for_review_crawl(self) -> Generator[Hotel, None, None]:
        """获取需要爬取评论的酒店

        Yields:
            Hotel对象
        """
        with session_scope() as session:
            hotels = session.query(Hotel).filter(
                Hotel.review_count >= settings.min_reviews_threshold
            ).all()

            for hotel in hotels:
                yield hotel
