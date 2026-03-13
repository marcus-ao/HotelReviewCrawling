"""酒店列表爬虫模块"""
import math
import importlib
import re
import time
from collections import defaultdict
from typing import Callable, Optional, Generator
from datetime import datetime, timedelta

from config.settings import settings
from config.regions import GUANGZHOU_REGIONS, PRICE_RANGES
from utils.logger import get_logger
from utils.cleaner import clean_text, normalize_hotel_name
from database.connection import session_scope
from database.models import Hotel, CrawlTask
from .anti_crawler import AntiCrawler
from .exceptions import CaptchaCooldownException, CaptchaException

logger = get_logger("hotel_list_crawler")


def _context_helpers():
    return importlib.import_module("utils.hotel_list_context")


def _pagination_helpers():
    return importlib.import_module("utils.hotel_list_pagination")


def _query_data_helpers():
    return importlib.import_module("utils.hotel_list_query_data")


def _persistence_helpers():
    return importlib.import_module("utils.hotel_list_persistence")


class HotelListCrawler:
    """酒店列表爬虫类"""

    # 飞猪酒店列表URL模板
    BASE_URL = "https://hotel.fliggy.com/hotel_list3.htm"

    def __init__(self, anti_crawler: Optional[AntiCrawler] = None):
        """初始化爬虫

        Args:
            anti_crawler: 反爬虫实例，如果不提供则创建新实例
        """
        self.anti_crawler = anti_crawler or AntiCrawler()
        self.page = None
        self._position_context: dict[str, object] = {}

    def build_search_url(
        self,
        city_code: str = "440100",
        business_zone_code: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
        check_in: Optional[str] = None,
        check_out: Optional[str] = None,
    ) -> str:
        """构建搜索URL

        Args:
            city_code: 城市代码（广州: 440100）
            business_zone_code: 商圈代码
            price_min: 最低价格
            price_max: 最高价格
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

        # 商圈筛选（飞猪使用 businessAreaId 参数）
        if business_zone_code:
            params.append(f"businessAreaId={business_zone_code}")

        # 价格筛选
        if price_min is not None and price_max is not None:
            params.append(f"priceRange={price_min}-{price_max}")
            params.append(f"lowPrice={price_min}")
            params.append(f"highPrice={price_max}")
        url = f"{self.BASE_URL}?{'&'.join(params)}"
        return url

    def _update_position_context(self, **kwargs) -> None:
        """Update crawl position context for logging/resume hints."""
        _context_helpers().update_position_context(self._position_context, **kwargs)

    def _clear_position_context(self) -> None:
        """Clear crawl position context."""
        _context_helpers().clear_position_context(self._position_context)

    def _format_position_context(self) -> str:
        """Format crawl position context for logs."""
        return _context_helpers().format_position_context(self._position_context)

    def _log_captcha_failure(self, action: str, exc: Exception) -> None:
        """Log captcha failure with current crawl context."""
        _context_helpers().log_captcha_failure(logger, self._position_context, action, exc)

    def _get_saved_hotel_ids(
        self,
        region_type: str,
        business_zone_code: str,
        price_level: str,
    ) -> set[str]:
        """Load already saved hotel ids for resume-safe crawling."""
        return _persistence_helpers().get_saved_hotel_ids(region_type, business_zone_code, price_level)

    def _prepare_hotels_for_price_range(
        self,
        hotels: list[dict],
        region_type: str,
        zone_name: str,
        zone_code: str,
        price_range: dict,
    ) -> list[dict]:
        """Attach zone metadata and filter invalid price-range records."""
        prepared_hotels = []
        skipped_no_price = 0
        skipped_price_mismatch = 0

        for hotel in hotels:
            base_price = hotel.get('base_price')
            mapped_level = self._map_price_level(base_price)
            if mapped_level is None:
                skipped_no_price += 1
                continue
            if not self._price_in_range(base_price, price_range):
                skipped_price_mismatch += 1
                continue

            hotel['region_type'] = region_type
            hotel['business_zone'] = zone_name
            hotel['business_zone_code'] = zone_code
            hotel['price_level'] = mapped_level
            hotel['city_code'] = '440100'
            prepared_hotels.append(hotel)

        if skipped_no_price or skipped_price_mismatch:
            logger.info(
                f"{zone_name} - {price_range['level']} 过滤: "
                f"无价格{skipped_no_price}家, 价格不匹配{skipped_price_mismatch}家"
            )

        return prepared_hotels

    def _get_region_saved_hotel_ids(self, region_type: str) -> set[str]:
        """Load saved hotel ids for a full functional region."""
        return _persistence_helpers().get_region_saved_hotel_ids(region_type)

    def _count_region_tier_hotels(self, region_type: str, price_level: str) -> int:
        """Count saved hotels for one region-tier bucket."""
        return _persistence_helpers().count_region_tier_hotels(region_type, price_level)

    def _get_price_range_by_level(self, region_type: str, price_level: str) -> Optional[dict]:
        """Find configured price range record by level in a region config."""
        region_config = GUANGZHOU_REGIONS.get(region_type)
        if not region_config:
            return None
        for price_range in region_config.get("price_ranges", []):
            if price_range.get("level") == price_level:
                return price_range
        return None

    def _get_region_tier_target(self, region_type: str, price_level: str) -> int:
        """Compute target hotel count for region-tier bucket."""
        region_config = GUANGZHOU_REGIONS.get(region_type)
        if not region_config:
            return 0
        zone_count = len(region_config.get("business_zones", []))
        price_range = self._get_price_range_by_level(region_type, price_level)
        if not price_range:
            return 0
        return int(price_range.get("top_n", 0)) * zone_count

    def _get_tier_floor_ratio(self, price_level: str) -> float:
        """Return floor ratio per price level."""
        mapping = {
            "经济型": settings.sampling_floor_ratio_economy,
            "舒适型": settings.sampling_floor_ratio_comfort,
            "高档型": settings.sampling_floor_ratio_high,
            "奢华型": settings.sampling_floor_ratio_luxury,
        }
        ratio = float(mapping.get(price_level, settings.sampling_floor_ratio_high))
        return max(0.0, min(ratio, 1.0))

    def _get_region_tier_floor(self, region_type: str, price_level: str) -> int:
        """Compute hard-floor count for region-tier bucket."""
        target = self._get_region_tier_target(region_type, price_level)
        if target <= 0:
            return 0
        ratio = self._get_tier_floor_ratio(price_level)
        floor_count = int(math.ceil(target * ratio))
        return max(0, min(floor_count, target))

    def _record_sampling_audit(
        self,
        session,
        hotel_data: dict,
        sample_source: str,
        min_review_count: int,
        borrow_for_region: Optional[str] = None,
    ) -> None:
        """Persist per-hotel sampling source for auditability."""
        _persistence_helpers().record_sampling_audit(
            session=session,
            hotel_data=hotel_data,
            sample_source=sample_source,
            min_review_count=min_review_count,
            borrow_for_region=borrow_for_region,
        )

    def _crawl_region_tier_incremental(
        self,
        region_type: str,
        price_level: str,
        needed: int,
        min_review_count: int,
        sample_source: str,
        borrow_for_region: Optional[str] = None,
    ) -> list[dict]:
        """Incrementally crawl additional hotels for one region-tier bucket."""
        if needed <= 0:
            return []

        region_config = GUANGZHOU_REGIONS.get(region_type)
        if not region_config:
            return []

        price_range = self._get_price_range_by_level(region_type, price_level)
        if not price_range:
            logger.warning(f"未找到价格档位配置: {region_type} - {price_level}")
            return []

        region_seen_ids = self._get_region_saved_hotel_ids(region_type)
        collected: list[dict] = []
        remaining = needed

        for zone in region_config.get("business_zones", []):
            if remaining <= 0:
                break

            zone_hotels = self.crawl_by_zone_and_price(
                region_type=region_type,
                business_zone=zone,
                price_range=price_range,
                target_count=remaining,
                exclude_ids=region_seen_ids,
                save_to_db=True,
                min_review_count_override=min_review_count,
                incremental_target=True,
                sample_source=sample_source,
                borrow_for_region=borrow_for_region,
            )

            for hotel in zone_hotels:
                hotel_id = hotel.get("hotel_id")
                if hotel_id:
                    region_seen_ids.add(hotel_id)

            collected.extend(zone_hotels)
            remaining = max(remaining - len(zone_hotels), 0)

        return collected

    def _apply_region_floor_policy(self, region_type: str) -> list[dict]:
        """Apply hard-floor and threshold relaxation within a region first."""
        if not settings.sampling_policy_enabled:
            return []

        collected: list[dict] = []
        threshold_steps = settings.sampling_threshold_steps

        for price_level in settings.sampling_sparse_tier_levels:
            target = self._get_region_tier_target(region_type, price_level)
            floor_count = self._get_region_tier_floor(region_type, price_level)
            if target <= 0 or floor_count <= 0:
                continue

            actual = self._count_region_tier_hotels(region_type, price_level)
            if actual >= floor_count:
                continue

            deficit = floor_count - actual
            logger.warning(
                f"{region_type} - {price_level} 低于硬下限: "
                f"当前{actual}/{floor_count}, 需要补齐{deficit}家"
            )

            for threshold in threshold_steps:
                if deficit <= 0:
                    break

                sample_source = (
                    "in_region"
                    if threshold >= settings.min_reviews_threshold
                    else "relaxed_threshold"
                )
                new_hotels = self._crawl_region_tier_incremental(
                    region_type=region_type,
                    price_level=price_level,
                    needed=deficit,
                    min_review_count=threshold,
                    sample_source=sample_source,
                )
                collected.extend(new_hotels)

                actual = self._count_region_tier_hotels(region_type, price_level)
                deficit = max(floor_count - actual, 0)

        return collected

    def _apply_city_compensation_policy(self) -> list[dict]:
        """Compensate sparse tier deficits with guarded cross-region borrowing."""
        if not settings.sampling_policy_enabled or not settings.sampling_city_compensation_enabled:
            return []

        all_regions = list(GUANGZHOU_REGIONS.keys())
        collected: list[dict] = []

        for price_level in settings.sampling_sparse_tier_levels:
            stats: dict[str, dict[str, int]] = {}
            for region_type in all_regions:
                target = self._get_region_tier_target(region_type, price_level)
                floor_count = self._get_region_tier_floor(region_type, price_level)
                actual = self._count_region_tier_hotels(region_type, price_level)
                borrow_cap = int(math.floor(target * settings.sampling_borrow_cap_ratio))
                donor_guard = int(math.ceil(target * settings.sampling_donor_guard_ratio))
                deficit = max(target - actual, 0)
                borrow_need = min(deficit, max(borrow_cap, 0))
                donor_surplus = max(actual - floor_count - donor_guard, 0)

                stats[region_type] = {
                    "target": target,
                    "actual": actual,
                    "floor": floor_count,
                    "borrow_need": borrow_need,
                    "donor_surplus": donor_surplus,
                }

            total_need = sum(item["borrow_need"] for item in stats.values())
            total_surplus = sum(item["donor_surplus"] for item in stats.values())
            if total_need <= 0 or total_surplus <= 0:
                continue

            donor_out: dict[str, int] = defaultdict(int)
            donor_receivers: dict[str, set[str]] = defaultdict(set)

            receiver_order = sorted(
                all_regions,
                key=lambda region: stats[region]["borrow_need"],
                reverse=True,
            )

            donor_order = sorted(
                all_regions,
                key=lambda region: stats[region]["donor_surplus"],
                reverse=True,
            )

            for receiver in receiver_order:
                need = stats[receiver]["borrow_need"]
                if need <= 0:
                    continue

                for donor in donor_order:
                    if donor == receiver:
                        continue
                    surplus = stats[donor]["donor_surplus"]
                    if surplus <= 0 or need <= 0:
                        continue

                    take = min(need, surplus)
                    stats[donor]["donor_surplus"] -= take
                    need -= take
                    donor_out[donor] += take
                    donor_receivers[donor].add(receiver)

            for donor_region, needed in donor_out.items():
                remaining = needed
                borrow_for_region = ",".join(sorted(donor_receivers[donor_region]))

                for threshold in settings.sampling_threshold_steps:
                    if remaining <= 0:
                        break

                    new_hotels = self._crawl_region_tier_incremental(
                        region_type=donor_region,
                        price_level=price_level,
                        needed=remaining,
                        min_review_count=threshold,
                        sample_source="cross_region_borrow",
                        borrow_for_region=borrow_for_region,
                    )
                    collected.extend(new_hotels)
                    remaining = max(remaining - len(new_hotels), 0)

                if remaining > 0:
                    logger.warning(
                        f"跨分区补偿不足: tier={price_level}, donor={donor_region}, "
                        f"未完成{remaining}/{needed}家"
                    )

        return collected

    def extract_hotels_from_page(
        self,
        max_hotels: Optional[int] = None,
        exclude_ids: Optional[set[str]] = None,
        min_review_count: Optional[int] = None,
        sort_strategy: Optional[str] = None,
        price_range: Optional[dict] = None,
        page_callback: Optional[Callable[[list[dict], int], None]] = None,
    ) -> list[dict]:
        """从当前页面提取酒店信息（支持翻页）

        Args:
            max_hotels: 最大提取数量，如果为None则提取所有
            exclude_ids: 需要排除的酒店ID集合（用于跨价格档去重）

        Returns:
            酒店信息列表
        """
        page = self.anti_crawler.get_page()
        all_hotels = []
        seen_hotel_ids = set()
        exclude_ids = set(exclude_ids) if exclude_ids else set()
        if min_review_count is None:
            min_review_count = settings.min_reviews_threshold
        current_page = 1
        max_pages = 20  # 自定义最大连续搜索页数深度
        
        while True:
            # 等待页面完全加载
            time.sleep(5)  # 等待JavaScript执行
            
            # 尝试等待地图容器加载（说明页面已渲染）
            try:
                page.wait.ele_displayed('#J_Map', timeout=15)
                logger.debug("地图容器已加载")
            except Exception as e:
                logger.warning(f"等待地图容器超时: {e}")
            
            # 再等待一下确保数据加载完成
            time.sleep(3)

            try:
                self.anti_crawler.scroll_to_bottom(step=500, max_scrolls=5)
                if self.anti_crawler.check_captcha():
                    self.anti_crawler.handle_captcha()
            except (CaptchaException, CaptchaCooldownException) as exc:
                self._log_captcha_failure("页面滚动", exc)
                raise
            time.sleep(2)

            # 直接从HTML源码提取（优先解析页面内置的查询数据）
            html = page.html
            page_info = self._get_query_page_info(html)
            page_no = current_page
            total_page = None

            if page_info:
                try:
                    current_page_raw = page_info.get("currentPage")
                    if current_page_raw is not None:
                        page_no = int(str(current_page_raw))
                except Exception:
                    page_no = current_page
                try:
                    total_page_raw = page_info.get("totalPage")
                    if total_page_raw is not None:
                        total_page = int(str(total_page_raw))
                except Exception:
                    total_page = None

            self._update_position_context(
                current_page=page_no,
                current_url=getattr(page, "url", "") or None,
            )


            logger.info(f"正在提取第 {page_no} 页...")

            # 统计本页新增的酒店数
            new_count = 0
            skipped_low_review = 0
            page_hotels = []

            hotels_on_page = self._extract_hotels_from_query_data(html)
            if hotels_on_page:
                def _sort_key(item: dict) -> tuple:
                    review_count = item.get("review_count") or 0
                    try:
                        review_count = int(review_count)
                    except Exception:
                        review_count = 0

                    price = item.get("base_price")
                    try:
                        price = int(price) if price is not None else None
                    except Exception:
                        price = None

                    score = item.get("rating_score")
                    try:
                        score = float(score) if score is not None else None
                    except Exception:
                        score = None

                    if sort_strategy == "price_desc":
                        price_key = -(price if price is not None else -1)
                        score_key = -(score if score is not None else 0)
                        return (-review_count, price_key, score_key)
                    if sort_strategy in ("price", "price_asc"):
                        price_key = price if price is not None else 10**9
                        score_key = -(score if score is not None else 0)
                        return (-review_count, price_key, score_key)
                    if sort_strategy == "score":
                        score_key = -(score if score is not None else 0)
                        price_key = price if price is not None else 10**9
                        return (-review_count, score_key, price_key)

                    price_key = price if price is not None else 10**9
                    score_key = -(score if score is not None else 0)
                    return (-review_count, price_key, score_key)

                hotels_on_page.sort(key=_sort_key)
                logger.info(f"page {page_no}: {len(hotels_on_page)} hotels from query data")

                for hotel_data in hotels_on_page:
                    hotel_id = hotel_data.get('hotel_id')
                    if not hotel_id:
                        continue
                    review_count = hotel_data.get('review_count') or 0
                    try:
                        review_count = int(review_count)
                    except Exception:
                        review_count = 0
                    if min_review_count and review_count <= min_review_count:
                        skipped_low_review += 1
                        continue
                    if hotel_id in seen_hotel_ids or hotel_id in exclude_ids:
                        continue
                    if price_range and not self._price_in_range(hotel_data.get('base_price'), price_range):
                        continue

                    all_hotels.append(hotel_data)
                    page_hotels.append(hotel_data)
                    seen_hotel_ids.add(hotel_id)
                    new_count += 1

                    # 如果达到最大数量，提前返回
                    if max_hotels and len(all_hotels) >= max_hotels:
                        logger.info(f"已达到目标数量 {max_hotels}，停止提取")
                        if page_callback and page_hotels:
                            page_callback(page_hotels, page_no)
                        return all_hotels
            else:
                # 回退方案：从HTML中提取酒店ID，再逐个解析
                hotel_ids = re.findall(r'data-shid="(\d+)"', html)
                hotel_ids = list(dict.fromkeys(hotel_ids))  # 去重但保持顺序
                logger.info(f"page {page_no}: {len(hotel_ids)} unique hotel IDs from html")

                # 为每个ID提取详细信息
                for hotel_id in hotel_ids:
                    # 检查是否已经提取过
                    if hotel_id in seen_hotel_ids or hotel_id in exclude_ids:
                        continue

                    try:
                        hotel_data = self._extract_hotel_from_html(html, hotel_id)
                        if hotel_data:
                            if price_range and not self._price_in_range(hotel_data.get('base_price'), price_range):
                                continue
                            review_count = hotel_data.get('review_count') or 0
                            try:
                                review_count = int(review_count)
                            except Exception:
                                review_count = 0
                            if min_review_count and review_count <= min_review_count:
                                skipped_low_review += 1
                                continue
                            all_hotels.append(hotel_data)
                            page_hotels.append(hotel_data)
                            seen_hotel_ids.add(hotel_id)
                            new_count += 1

                            # 如果达到最大数量，提前返回
                            if max_hotels and len(all_hotels) >= max_hotels:
                                logger.info(f"已达到目标数量 {max_hotels}，停止提取")
                                if page_callback and page_hotels:
                                    page_callback(page_hotels, page_no)
                                return all_hotels
                    except Exception as e:
                        logger.debug(f"提取酒店 {hotel_id} 失败: {e}")
                        continue
            
            logger.info(
                f"第 {page_no} 页新增 {new_count} 家酒店，跳过低评论 {skipped_low_review}，累计 {len(all_hotels)} 家"
            )

            if page_callback and page_hotels:
                page_callback(page_hotels, page_no)
            
            # 检查是否需要翻页
            if total_page and page_no >= total_page:
                logger.info("Reached last page, stop pagination")
                break


            
            if max_pages and current_page >= max_pages:
                logger.info(f"已达到最大页数 {max_pages}，停止翻页")
                break
            
            # 尝试翻页
            try:
                if not self._go_to_next_page(page_info):
                    logger.info("没有下一页或翻页失败，停止提取")
                    break
            except (CaptchaException, CaptchaCooldownException) as exc:
                self._log_captcha_failure("列表翻页", exc)
                raise
            
            current_page += 1
            
            # 翻页后随机延迟
            self.anti_crawler.random_delay(3, 5)
        
        logger.info(f"共提取 {len(all_hotels)} 家酒店（{current_page} 页）")
        return all_hotels

    def _extract_json_blob(self, html: str, var_name: str) -> Optional[str]:
        """Extract a JSON object assigned to a JS variable from HTML."""
        return _query_data_helpers().extract_json_blob(html, var_name)

    def _map_price_level(self, base_price: Optional[int]) -> Optional[str]:
        """Map base price to configured price level."""
        if base_price is None:
            return None
        try:
            price = int(base_price)
        except Exception:
            return None

        for pr in PRICE_RANGES:
            min_price = pr.get("min")
            max_price = pr.get("max")
            if min_price is None or max_price is None:
                continue

            if price >= min_price and (price < max_price or max_price >= 99999):
                return pr.get("level")
        return None

    def _price_in_range(self, base_price: Optional[int], price_range: dict) -> bool:
        """Check if base price falls into the given range."""
        if base_price is None:
            return False
        try:
            price = int(base_price)
        except Exception:
            return False

        min_price = price_range.get("min")
        max_price = price_range.get("max")
        if min_price is None or max_price is None:
            return False

        if max_price >= 99999:
            return price >= min_price
        return min_price <= price < max_price

    def _extract_hotels_from_query_data(self, html: str) -> list[dict]:
        """Extract hotel list from __QUERY_RESULT_DATA__ JSON in the page."""
        return _query_data_helpers().extract_hotels_from_query_data(html, logger=logger)

    def _get_query_page_info(self, html: str) -> Optional[dict]:
        """Get paging info from __QUERY_RESULT_DATA__."""
        return _query_data_helpers().extract_query_page_info(html)

    def _update_url_param(self, url: str, key: str, value: int) -> str:
        """Update/add a query param for pagination."""
        return _pagination_helpers().update_url_param(url, key, value)

    def _extract_hotel_from_html(self, html: str, hotel_id: str) -> Optional[dict]:
        """从HTML源码中提取单个酒店的基本信息

        Args:
            html: 页面HTML源码
            hotel_id: 酒店ID

        Returns:
            酒店数据字典（仅包含基本信息）
        """
        try:
            # 优先从列表项中解析（list-row 包含 data-name/data-shid）
            list_pattern = rf'<div[^>]*class="list-row[^"]*"[^>]*data-shid="{hotel_id}"[^>]*>'
            list_match = re.search(list_pattern, html)
            if list_match:
                row_tag = list_match.group(0)
                name_match = re.search(r'data-name="([^"]+)"', row_tag)
                if name_match:
                    title = name_match.group(1)
                    name = normalize_hotel_name(clean_text(title))
                    if not name:
                        return None

                    latitude = None
                    longitude = None
                    lat_match = re.search(r'data-lat="([^"]+)"', row_tag)
                    lng_match = re.search(r'data-lng="([^"]+)"', row_tag)
                    try:
                        if lat_match:
                            latitude = float(lat_match.group(1))
                        if lng_match:
                            longitude = float(lng_match.group(1))
                    except Exception:
                        latitude = None
                        longitude = None

                    return {
                        'hotel_id': hotel_id,
                        'name': name,
                        'address': None,
                        'latitude': latitude,
                        'longitude': longitude,
                        'star_level': None,
                        'rating_score': None,
                        'review_count': 0,
                        'base_price': None,
                    }

            # 查找包含该酒店ID的HTML片段
            # 地图标记格式: <div class="hotel-marker" title="广州xxx酒店" ... data-shid="10019773">
            marker_pattern = rf'<div[^>]*class="hotel-marker"[^>]*title="([^"]*)"[^>]*data-shid="{hotel_id}"'
            marker_match = re.search(marker_pattern, html)
            
            if not marker_match:
                # 尝试反向匹配
                marker_pattern = rf'<div[^>]*data-shid="{hotel_id}"[^>]*title="([^"]*)"[^>]*class="hotel-marker"'
                marker_match = re.search(marker_pattern, html)
            
            if not marker_match:
                logger.debug(f"未找到酒店 {hotel_id} 的标记信息")
                return None
            
            # 提取酒店名称（保留完整名称，不去掉"广州"前缀）
            title = marker_match.group(1)
            
            # 清洗名称
            name = normalize_hotel_name(clean_text(title))
            
            if not name:
                logger.debug(f"酒店 {hotel_id} 名称为空")
                return None
            
            # 返回基本信息，详细信息将通过fetch_hotel_details获取
            return {
                'hotel_id': hotel_id,
                'name': name,
                'address': None,
                'latitude': None,
                'longitude': None,
                'star_level': None,
                'rating_score': None,
                'review_count': 0,
                'base_price': None,
            }

        except Exception as e:
            logger.debug(f"从HTML提取酒店 {hotel_id} 异常: {e}")
            return None




    def _go_to_next_page(self, page_info: Optional[dict] = None) -> bool:
        """Go to next page (URL-first, click as fallback)."""
        page = self.anti_crawler.get_page()

        if page_info is None:
            page_info = self._get_query_page_info(page.html)

        current_page = None
        total_page = None
        page_size = None

        if page_info:
            try:
                current_page_raw = page_info.get("currentPage")
                if current_page_raw is not None:
                    current_page = int(str(current_page_raw))
            except Exception:
                current_page = None
            try:
                total_page_raw = page_info.get("totalPage")
                if total_page_raw is not None:
                    total_page = int(str(total_page_raw))
            except Exception:
                total_page = None
            try:
                page_size_raw = page_info.get("pageSize")
                if page_size_raw is not None:
                    page_size = int(str(page_size_raw))
            except Exception:
                page_size = None

        if current_page:
            next_page = current_page + 1
            if total_page and next_page > total_page:
                logger.debug("Reached last page")
                return False

            current_url = getattr(page, "url", "") or ""
            if current_url:
                candidate_urls = []
                for key in ("currentPage", "pageNo", "page", "pageNum"):
                    url = self._update_url_param(current_url, key, next_page)
                    if page_size:
                        url = self._update_url_param(url, "offset", (next_page - 1) * page_size)
                    candidate_urls.append(url)

                seen = set()
                for url in candidate_urls:
                    if url in seen or url == current_url:
                        continue
                    seen.add(url)
                    logger.debug(f"Try page URL: {url}")
                    self._update_position_context(current_url=url)
                    try:
                        if not self.anti_crawler.navigate_to(url):
                            continue
                    except (CaptchaException, CaptchaCooldownException) as exc:
                        self._log_captcha_failure("URL翻页导航", exc)
                        raise
                    time.sleep(2)
                    new_info = self._get_query_page_info(page.html)
                    if new_info:
                        try:
                            new_page_raw = new_info.get("currentPage")
                            if new_page_raw is not None:
                                new_page = int(str(new_page_raw))
                            else:
                                new_page = None
                        except Exception:
                            new_page = None
                        if new_page and new_page != current_page:
                            return True

        return self._go_to_next_page_by_click()

    def _go_to_next_page_by_click(self) -> bool:
        """Fallback: click next page button."""
        page = self.anti_crawler.get_page()

        try:
            next_button_selectors = [
                'a.page-next',           # next link
                '.pagination .next',     # pagination next
                'a[title="Next"]',      # title next
                '.page-link.next',       # Bootstrap style
                'li.next a',             # list item next
            ]

            next_button = None
            for selector in next_button_selectors:
                next_button = page.ele(selector, timeout=2)
                if next_button:
                    logger.debug(f"Found next button: {selector}")
                    break

            if not next_button:
                logger.debug("Next button not found")
                return False

            if next_button.attr('class') and 'disabled' in next_button.attr('class'):
                logger.debug("Next button disabled")
                return False

            next_button.scroll.to_see()
            time.sleep(0.5)

            logger.info("Click next page...")
            next_button.click()

            time.sleep(2)
            if self.anti_crawler.check_captcha():
                self.anti_crawler.handle_captcha()
            return True

        except (CaptchaException, CaptchaCooldownException) as exc:
            self._log_captcha_failure("点击翻页", exc)
            raise

        except Exception as e:
            logger.debug(f"Pagination failed: {e}")
            return False

    def fetch_hotel_details(self, hotel_id: str) -> Optional[dict]:
        """访问酒店详情页获取完整信息
        
        Args:
            hotel_id: 酒店ID
            
        Returns:
            包含详细信息的字典，如果失败返回None
        """
        try:
            logger.debug(f"访问酒店详情页: {hotel_id}")
            
            # 构建详情页URL - 使用hotel_detail2.htm格式
            # 参考实际URL: https://hotel.fliggy.com/hotel_detail2.htm?shid=10019773&city=440100&checkIn=2026-01-31&checkOut=2026-02-01
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            day_after = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
            
            detail_url = (
                f"https://hotel.fliggy.com/hotel_detail2.htm?"
                f"shid={hotel_id}&"
                f"city=440100&"
                f"checkIn={tomorrow}&"
                f"checkOut={day_after}&"
                f"searchBy=&"
                f"market=0&"
                f"previousChannel=&"
                f"roomNum=1&"
                f"aNum_1=2&"
                f"cNum_1=0"
            )
            
            # 导航到详情页
            if not self.anti_crawler.navigate_to(detail_url):
                logger.warning(f"无法访问酒店 {hotel_id} 的详情页")
                return None
            
            # 等待页面加载
            self.anti_crawler.random_delay(2, 4)
            page = self.anti_crawler.get_page()
            
            # 等待关键元素加载
            try:
                # 等待酒店基本信息区域加载
                page.wait.ele_displayed('.hotel-baseinfo', timeout=15)
                logger.debug(f"酒店 {hotel_id} 基本信息区域已加载")
            except Exception as e:
                logger.warning(f"等待页面加载超时: {e}")
            
            # 额外等待确保数据加载完成
            time.sleep(2)
            
            # 提取详细信息
            details = {}
            html = page.html
            
            # 提取酒店名称 - 从<h2>标签中提取（保留完整名称）
            try:
                # 格式: <h2>广州海航威斯汀酒店  <!--<em>豪华型</em>-->
                name_match = re.search(r'<h2>([^<]+?)(?:\s*<!--|\s*<)', html)
                if name_match:
                    name = clean_text(name_match.group(1))
                    if name:
                        details['name'] = normalize_hotel_name(name)
                        logger.debug(f"提取到酒店名称: {details['name']}")
            except Exception as e:
                logger.debug(f"提取名称失败: {e}")
            
            # 提取评分 - 从评价区域提取
            try:
                # 格式: <a href="#hotel-review" target="_self">4.7</a>
                score_match = re.search(r'<li class="rate">\s*<a[^>]*>(\d+\.?\d*)</a>', html)
                if score_match:
                    details['rating_score'] = float(score_match.group(1))
                    logger.debug(f"提取到评分: {details['rating_score']}")
            except Exception as e:
                logger.debug(f"提取评分失败: {e}")
            
            # 提取评论数 - 从评价区域提取
            try:
                # 格式: <li class="comments"><a href="#hotel-review" target="_self">8155</a>
                review_match = re.search(r'<li class="comments">\s*<a[^>]*>(\d+)</a>', html)
                if review_match:
                    details['review_count'] = int(review_match.group(1))
                    logger.debug(f"提取到评论数: {details['review_count']}")
            except Exception as e:
                logger.debug(f"提取评论数失败: {e}")
            
            # 提取地址 - 从<p class="address">提取
            try:
                # 格式: <p class="address">林和中路6号(近中信广场火车东站) , 广州 </p>
                address_match = re.search(r'<p class="address">([^<]+)</p>', html)
                if address_match:
                    address = clean_text(address_match.group(1))
                    # 去掉末尾的", 广州"
                    address = re.sub(r'\s*,\s*广州\s*$', '', address)
                    if address:
                        details['address'] = address
                        logger.debug(f"提取到地址: {details['address']}")
            except Exception as e:
                logger.debug(f"提取地址失败: {e}")
            
            # 提取价格 - 从价格区域提取
            try:
                # 格式1: <span class="pi-price" id="J_HotelPrice"><i>&yen;</i>857</span>
                price_match = re.search(r'<span class="pi-price"[^>]*id="J_HotelPrice"[^>]*><i>&yen;</i>(\d+)</span>', html)
                if not price_match:
                    # 格式2: <i>&yen;</i>857<span class="zhijian-container">
                    price_match = re.search(r'id="J_HotelPrice"[^>]*><i>&yen;</i>(\d+)', html)
                if not price_match:
                    # 格式3: 从JavaScript数据中提取
                    price_match = re.search(r'"hotelPrice"\s*:\s*"(\d+)"', html)
                
                if price_match:
                    details['base_price'] = float(price_match.group(1))
                    logger.debug(f"提取到价格: {details['base_price']}")
                else:
                    logger.debug("未找到价格信息")
            except Exception as e:
                logger.debug(f"提取价格失败: {e}")
            
            # 提取星级 - 从meta-level属性提取
            try:
                # 格式: <span class="row-subtitle" title="飞猪旅行用户评定为5钻豪华型" meta-level="5-豪华型">
                star_match = re.search(r'meta-level="(\d+)-([^"]+)"', html)
                if star_match:
                    details['star_level'] = star_match.group(2)
                    logger.debug(f"提取到星级: {details['star_level']}")
            except Exception as e:
                logger.debug(f"提取星级失败: {e}")
            
            # 提取经纬度 - 从地图配置中提取
            try:
                # 格式: lat: 23.143612, lng: 113.325935
                lat_match = re.search(r'lat:\s*([\d.]+)', html)
                lng_match = re.search(r'lng:\s*([\d.]+)', html)
                if lat_match and lng_match:
                    details['latitude'] = float(lat_match.group(1))
                    details['longitude'] = float(lng_match.group(1))
                    logger.debug(f"提取到坐标: ({details['latitude']}, {details['longitude']})")
            except Exception as e:
                logger.debug(f"提取坐标失败: {e}")
            
            if details:
                logger.debug(f"成功提取酒店 {hotel_id} 的详细信息: {len(details)} 个字段")
                return details
            else:
                logger.warning(f"未能提取到酒店 {hotel_id} 的任何详细信息")
                return None
            
        except Exception as e:
            logger.error(f"获取酒店 {hotel_id} 详情失败: {e}")
            return None

    def crawl_by_zone_and_price(
        self,
        region_type: str,
        business_zone: dict,
        price_range: dict,
        target_count: Optional[int] = None,
        exclude_ids: Optional[set[str]] = None,
        save_to_db: bool = True,
        min_review_count_override: Optional[int] = None,
        incremental_target: bool = False,
        sample_source: str = "in_region",
        borrow_for_region: Optional[str] = None,
    ) -> list[dict]:
        """按商圈和价格档次爬取酒店（支持翻页采集）

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
        top_n = target_count if target_count is not None else price_range['top_n']
        effective_min_review_count = (
            min_review_count_override
            if min_review_count_override is not None
            else settings.min_reviews_threshold
        )

        mode_label = "增采" if incremental_target else "爬取"
        logger.info(f"开始{mode_label}: {region_type} - {zone_name} - {price_level} (目标: {top_n}家)")

        saved_hotel_ids = self._get_saved_hotel_ids(region_type, zone_code, price_level) if save_to_db else set()
        if incremental_target:
            remaining_target = top_n
        else:
            remaining_target = max(top_n - len(saved_hotel_ids), 0)
        if saved_hotel_ids:
            if incremental_target:
                logger.info(
                    f"检测到已保存样本: {region_type} - {zone_name} - {price_level} "
                    f"已有{len(saved_hotel_ids)}家, 本次增采{remaining_target}家"
                )
            else:
                logger.info(
                    f"检测到已保存进度: {region_type} - {zone_name} - {price_level} "
                    f"已有{len(saved_hotel_ids)}家, 本次补采{remaining_target}家"
                )
        if remaining_target <= 0:
            logger.info(f"{region_type} - {zone_name} - {price_level} 已达到目标数量，跳过重爬")
            return []

        # 构建URL
        url = self.build_search_url(
            business_zone_code=zone_code,
            price_min=price_range['min'],
            price_max=price_range['max'],
        )

        self._update_position_context(
            region_type=region_type,
            business_zone=zone_name,
            business_zone_code=zone_code,
            price_level=price_level,
            target_count=remaining_target,
            saved_count=len(saved_hotel_ids),
            current_page=1,
            current_url=url,
        )

        # 导航到页面
        try:
            if not self.anti_crawler.navigate_to(url):
                logger.error(f"导航失败: {url}")
                self._clear_position_context()
                return []
        except (CaptchaException, CaptchaCooldownException) as exc:
            self._log_captcha_failure("酒店列表导航", exc)
            raise

        # 等待页面加载 - 增加等待时间
        logger.info("等待页面完全加载...")
        self.anti_crawler.random_delay(5, 8)



        # 提取酒店（支持翻页，直到达到目标数量）
        hotels = []
        effective_exclude_ids = set(exclude_ids) if exclude_ids else set()
        effective_exclude_ids.update(saved_hotel_ids)

        def persist_page_hotels(page_hotels: list[dict], page_no: int) -> None:
            prepared_hotels = self._prepare_hotels_for_price_range(
                hotels=page_hotels,
                region_type=region_type,
                zone_name=zone_name,
                zone_code=zone_code,
                price_range=price_range,
            )
            if not prepared_hotels:
                return

            self._save_hotels(
                prepared_hotels,
                min_review_count_override=effective_min_review_count,
                sample_source=sample_source,
                borrow_for_region=borrow_for_region,
            )
            hotels.extend(prepared_hotels)
            self._update_position_context(saved_count=len(saved_hotel_ids) + len(hotels), current_page=page_no)

        raw_hotels = self.extract_hotels_from_page(
            max_hotels=remaining_target,
            exclude_ids=effective_exclude_ids,
            min_review_count=effective_min_review_count,
            price_range=price_range,
            page_callback=persist_page_hotels if save_to_db else None,
        )

        if not save_to_db:
            hotels = self._prepare_hotels_for_price_range(
                hotels=raw_hotels,
                region_type=region_type,
                zone_name=zone_name,
                zone_code=zone_code,
                price_range=price_range,
            )

        if incremental_target:
            logger.info(
                f"增采完成: {region_type} - {zone_name} - {price_level} "
                f"新增{len(hotels)}/{top_n}家 (阈值>={effective_min_review_count})"
            )
        else:
            actual_total = len(saved_hotel_ids) + len(hotels)
            logger.info(f"爬取完成: {actual_total}/{top_n} 家酒店 (新增{len(hotels)}家)")
        self._clear_position_context()
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
        region_seen_ids: set[str] = set()

        for zone in region_config['business_zones']:
            zone_hotels = self._crawl_business_zone_elastic(
                region_type=region_type,
                business_zone=zone,
                price_ranges=region_config['price_ranges'],
                exclude_ids=region_seen_ids,
                save_to_db=save_to_db,
            )
            for hotel in zone_hotels:
                hotel_id = hotel.get('hotel_id')
                if hotel_id:
                    region_seen_ids.add(hotel_id)
            all_hotels.extend(zone_hotels)

        if save_to_db and settings.sampling_policy_enabled:
            adaptive_hotels = self._apply_region_floor_policy(region_type)
            if adaptive_hotels:
                logger.info(
                    f"功能区 {region_type} 采样补偿新增 {len(adaptive_hotels)} 家酒店"
                )
                for hotel in adaptive_hotels:
                    hotel_id = hotel.get('hotel_id')
                    if hotel_id:
                        region_seen_ids.add(hotel_id)
                all_hotels.extend(adaptive_hotels)

        logger.info(f"功能区 {region_type} 爬取完成，共 {len(all_hotels)} 家酒店")
        return all_hotels

    def _crawl_business_zone_elastic(
        self,
        region_type: str,
        business_zone: dict,
        price_ranges: list[dict],
        exclude_ids: Optional[set[str]] = None,
        save_to_db: bool = True,
    ) -> list[dict]:
        """对单个商圈执行“弹性补位”爬取，保证尽量达到目标数量。"""
        zone_name = business_zone['name']
        zone_code = business_zone['code']
        zone_target_total = sum(pr['top_n'] for pr in price_ranges)

        logger.info(f"开始弹性补位: {region_type} - {zone_name} (目标: {zone_target_total}家)")

        zone_hotels: list[dict] = []
        zone_seen_ids: set[str] = set(exclude_ids) if exclude_ids else set()
        remaining = zone_target_total
        carry_over = 0
        exhausted_price_levels: set[str] = set()

        # 正向遍历价格档次，将缺口顺延到相邻更高价位
        for price_range in price_ranges:
            if remaining <= 0:
                break

            base_target = price_range['top_n']
            target = base_target + carry_over
            if target > remaining:
                target = remaining
            if target <= 0:
                carry_over = 0
                continue

            logger.info(
                f"弹性目标: {zone_name} - {price_range['level']} "
                f"(基础{base_target} + 追加{carry_over} = {target})"
            )

            existing_before_ids: set[str] = set()
            if save_to_db:
                existing_before_ids = self._get_saved_hotel_ids(region_type, zone_code, price_range['level'])
                if existing_before_ids:
                    zone_seen_ids.update(existing_before_ids)
            existing_before_count = len(existing_before_ids)

            hotels = self.crawl_by_zone_and_price(
                region_type=region_type,
                business_zone=business_zone,
                price_range=price_range,
                target_count=target,
                exclude_ids=zone_seen_ids,
                save_to_db=save_to_db,
            )

            # 更新商圈内已采集的酒店集合
            for h in hotels:
                if h.get('hotel_id'):
                    zone_seen_ids.add(h['hotel_id'])

            zone_hotels.extend(hotels)

            if save_to_db:
                existing_after_count = len(self._get_saved_hotel_ids(region_type, zone_code, price_range['level']))
                actual = existing_after_count
                new_added = max(existing_after_count - existing_before_count, 0)
            else:
                actual = len(hotels)
                new_added = len(hotels)

            achieved_for_target = min(target, actual)
            remaining = max(remaining - achieved_for_target, 0)
            carry_over = max(target - achieved_for_target, 0)

            logger.info(
                f"{zone_name} - {price_range['level']} 完成统计: "
                f"已完成 {actual}/{target} 家, 本轮新增 {new_added} 家"
            )

            if save_to_db and target > 0 and new_added == 0:
                exhausted_price_levels.add(price_range['level'])
                logger.info(
                    f"{zone_name} - {price_range['level']} 本轮无增量，标记为耗尽档位"
                )
            elif save_to_db and new_added > 0:
                exhausted_price_levels.discard(price_range['level'])

            if carry_over > 0:
                logger.warning(
                    f"{zone_name} - {price_range['level']} 数量不足，"
                    f"缺口 {carry_over} 家将转移到相邻价位"
                )

            # 每次爬取后随机延迟
            self.anti_crawler.random_delay(3, 6)

        # 如果最后仍有缺口，反向回补到相邻更低价位
        if remaining > 0:
            logger.warning(
                f"{zone_name} 仍缺少 {remaining} 家酒店，开始反向补位"
            )
            for idx, price_range in enumerate(reversed(price_ranges)):
                if remaining <= 0:
                    break
                # Skip highest tier in reverse fill (already attempted)
                if idx == 0:
                    continue

                target = remaining
                logger.info(
                    f"反向补位: {zone_name} - {price_range['level']} (目标: {target})"
                )

                if save_to_db and price_range['level'] in exhausted_price_levels:
                    logger.info(
                        f"反向补位跳过: {zone_name} - {price_range['level']} "
                        f"(本轮已验证无增量，避免重复空转)"
                    )
                    continue

                existing_before_ids: set[str] = set()
                if save_to_db:
                    existing_before_ids = self._get_saved_hotel_ids(region_type, zone_code, price_range['level'])
                    if existing_before_ids:
                        zone_seen_ids.update(existing_before_ids)
                existing_before_count = len(existing_before_ids)

                hotels = self.crawl_by_zone_and_price(
                    region_type=region_type,
                    business_zone=business_zone,
                    price_range=price_range,
                    target_count=target,
                    exclude_ids=zone_seen_ids,
                    save_to_db=save_to_db,
                )

                for h in hotels:
                    if h.get('hotel_id'):
                        zone_seen_ids.add(h['hotel_id'])

                zone_hotels.extend(hotels)

                if save_to_db:
                    existing_after_count = len(self._get_saved_hotel_ids(region_type, zone_code, price_range['level']))
                    actual = existing_after_count
                    new_added = max(existing_after_count - existing_before_count, 0)
                else:
                    actual = len(hotels)
                    new_added = len(hotels)

                # 反向补位是“补缺口”动作，remaining 仅按本轮新增贡献递减。
                achieved_for_target = min(target, new_added)
                remaining = max(remaining - achieved_for_target, 0)

                logger.info(
                    f"反向补位统计: {zone_name} - {price_range['level']} "
                    f"已完成 {actual}/{target} 家, 本轮新增 {new_added} 家"
                )

                if save_to_db and target > 0 and new_added == 0:
                    exhausted_price_levels.add(price_range['level'])

                # 每次爬取后随机延迟
                self.anti_crawler.random_delay(3, 6)

            if remaining > 0:
                logger.warning(
                    f"{zone_name} 最终仍缺少 {remaining} 家，"
                    f"可能需要调整筛选或增加页数"
                )

        if save_to_db:
            zone_real_ids: set[str] = set()
            for price_range in price_ranges:
                zone_real_ids.update(
                    self._get_saved_hotel_ids(region_type, zone_code, price_range['level'])
                )
            achieved_total = len(zone_real_ids)
        else:
            achieved_total = len(zone_hotels)
        logger.info(
            f"商圈 {zone_name} 弹性补位完成，实际 {achieved_total}/{zone_target_total} 家"
        )
        return zone_hotels

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

        if save_to_db and settings.sampling_policy_enabled and settings.sampling_city_compensation_enabled:
            city_compensated_hotels = self._apply_city_compensation_policy()
            if city_compensated_hotels:
                all_hotels.extend(city_compensated_hotels)
                logger.info(f"城市级跨分区补偿新增 {len(city_compensated_hotels)} 家酒店")

        logger.info(f"全部爬取完成，共 {len(all_hotels)} 家酒店")
        return all_hotels

    def _save_hotels(
        self,
        hotels: list[dict],
        fetch_details: bool = False,
        min_review_count_override: Optional[int] = None,
        sample_source: str = "in_region",
        borrow_for_region: Optional[str] = None,
    ) -> int:
        """保存酒店到数据库（优化去重逻辑，避免跨商圈/价格档次的重复）

        Args:
            hotels: 酒店数据列表
            fetch_details: 是否获取详细信息（默认False，因为详情页不可用）

        Returns:
            成功保存的数量
        """
        return _persistence_helpers().save_hotels(
            hotels=hotels,
            fetch_details=fetch_details,
            min_review_count_override=min_review_count_override,
            sample_source=sample_source,
            borrow_for_region=borrow_for_region,
            logger=logger,
            fetch_hotel_details=lambda hotel_id: self.fetch_hotel_details(hotel_id),
            random_delay=lambda min_delay, max_delay: self.anti_crawler.random_delay(min_delay, max_delay),
            map_price_level=lambda base_price: self._map_price_level(base_price),
        )

    def enrich_hotel_details(self, hotel_ids: Optional[list[str]] = None) -> int:
        """补充酒店的详细信息
        
        Args:
            hotel_ids: 要补充信息的酒店ID列表，如果为None则补充所有缺少详细信息的酒店
            
        Returns:
            成功补充的酒店数量
        """
        enriched_count = 0
        
        with session_scope() as session:
            # 查询需要补充信息的酒店
            query = session.query(Hotel)
            
            if hotel_ids:
                query = query.filter(Hotel.hotel_id.in_(hotel_ids))
            else:
                # 查找缺少关键信息的酒店（评分或价格为空）
                query = query.filter(
                    (Hotel.rating_score == None) | (Hotel.base_price == None)
                )
            
            hotels = query.all()
            total = len(hotels)
            
            if total == 0:
                logger.info("没有需要补充信息的酒店")
                return 0
            
            logger.info(f"开始补充 {total} 家酒店的详细信息...")
            
            for i, hotel in enumerate(hotels, 1):
                try:
                    logger.info(f"[{i}/{total}] 获取酒店 {hotel.hotel_id} ({hotel.name}) 的详细信息...")
                    
                    if hotel.hotel_id is None:
                        logger.warning(f"酒店记录缺少 hotel_id，跳过: {hotel}")
                        continue

                    details = self.fetch_hotel_details(str(hotel.hotel_id))
                    
                    if details:
                        # 更新酒店信息
                        for key, value in details.items():
                            if value is not None:
                                setattr(hotel, key, value)
                        
                        enriched_count += 1
                        logger.info(f"成功补充 {len(details)} 个字段")
                    else:
                        logger.warning(f"未能获取酒店 {hotel.hotel_id} 的详细信息")
                    
                    # 延迟避免请求过快
                    self.anti_crawler.random_delay(2, 4)
                    
                except Exception as e:
                    logger.error(f"补充酒店 {hotel.hotel_id} 信息失败: {e}")
                    continue
        
        logger.info(f"补充完成: 成功 {enriched_count}/{total} 家酒店")
        return enriched_count

    def get_hotels_for_review_crawl(self) -> Generator[Hotel, None, None]:
        """获取需要爬取评论的酒店

        Yields:
            Hotel对象
        """
        with session_scope() as session:
            hotels = session.query(Hotel).all()

            sparse_tiers = set(settings.sampling_sparse_tier_levels) if settings.sampling_policy_enabled else set()
            relaxed_min = min(settings.sampling_threshold_steps) if settings.sampling_policy_enabled else settings.min_reviews_threshold

            filtered_hotels = []
            for hotel in hotels:
                review_count_raw = getattr(hotel, "review_count", 0)
                try:
                    if review_count_raw is None:
                        review_count = 0
                    else:
                        review_count = int(review_count_raw)
                except Exception:
                    review_count = 0

                price_level_raw = getattr(hotel, "price_level", "")
                price_level = "" if price_level_raw is None else str(price_level_raw)

                if review_count > settings.min_reviews_threshold:
                    filtered_hotels.append(hotel)
                    continue

                if (
                    settings.sampling_policy_enabled
                    and price_level in sparse_tiers
                    and review_count > relaxed_min
                ):
                    filtered_hotels.append(hotel)

            for hotel in filtered_hotels:
                yield hotel

