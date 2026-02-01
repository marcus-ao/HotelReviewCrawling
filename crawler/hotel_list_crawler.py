"""酒店列表爬虫模块"""
import json
import re
import time
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode
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
        check_in: str = None,
        check_out: str = None,
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

    def extract_hotels_from_page(
        self,
        max_hotels: int = None,
        exclude_ids: Optional[set[str]] = None,
        min_review_count: Optional[int] = None,
        sort_strategy: Optional[str] = None,
        price_range: Optional[dict] = None,
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
        max_pages = 50  # search deeper before fallback
        
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

            self.anti_crawler.scroll_to_bottom(step=500, max_scrolls=5)
            time.sleep(2)

            # 直接从HTML源码提取（优先解析页面内置的查询数据）
            html = page.html
            page_info = self._get_query_page_info(html)
            page_no = current_page
            total_page = None

            if page_info:
                try:
                    page_no = int(page_info.get("currentPage"))
                except Exception:
                    page_no = current_page
                try:
                    total_page = int(page_info.get("totalPage"))
                except Exception:
                    total_page = None


            logger.info(f"正在提取第 {page_no} 页...")

            # 统计本页新增的酒店数
            new_count = 0
            skipped_low_review = 0

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
                    seen_hotel_ids.add(hotel_id)
                    new_count += 1

                    # 如果达到最大数量，提前返回
                    if max_hotels and len(all_hotels) >= max_hotels:
                        logger.info(f"已达到目标数量 {max_hotels}，停止提取")
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
                    if price_range and not self._price_in_range(hotel_data.get('base_price'), price_range):
                        continue

                    try:
                        hotel_data = self._extract_hotel_from_html(html, hotel_id)
                        if hotel_data:
                            review_count = hotel_data.get('review_count') or 0
                            try:
                                review_count = int(review_count)
                            except Exception:
                                review_count = 0
                            if min_review_count and review_count <= min_review_count:
                                skipped_low_review += 1
                                continue
                            all_hotels.append(hotel_data)
                            seen_hotel_ids.add(hotel_id)
                            new_count += 1

                            # 如果达到最大数量，提前返回
                            if max_hotels and len(all_hotels) >= max_hotels:
                                logger.info(f"已达到目标数量 {max_hotels}，停止提取")
                                return all_hotels
                    except Exception as e:
                        logger.debug(f"提取酒店 {hotel_id} 失败: {e}")
                        continue
            
            logger.info(
                f"第 {page_no} 页新增 {new_count} 家酒店，跳过低评论 {skipped_low_review}，累计 {len(all_hotels)} 家"
            )
            
            # 检查是否需要翻页
            if total_page and page_no >= total_page:
                logger.info("Reached last page, stop pagination")
                break


            
            if max_pages and current_page >= max_pages:
                logger.info(f"已达到最大页数 {max_pages}，停止翻页")
                break
            
            # 尝试翻页
            if not self._go_to_next_page(page_info):
                logger.info("没有下一页或翻页失败，停止提取")
                break
            
            current_page += 1
            
            # 翻页后随机延迟
            self.anti_crawler.random_delay(3, 5)
        
        logger.info(f"共提取 {len(all_hotels)} 家酒店（{current_page} 页）")
        return all_hotels

    def _extract_json_blob(self, html: str, var_name: str) -> Optional[str]:
        """Extract a JSON object assigned to a JS variable from HTML."""
        token = f"{var_name} ="
        start_idx = html.find(token)
        if start_idx == -1:
            return None

        brace_start = html.find("{", start_idx)
        if brace_start == -1:
            return None

        depth = 0
        in_string = False
        escape = False

        for i in range(brace_start, len(html)):
            ch = html[i]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
            else:
                if ch == '"':
                    in_string = True
                elif ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        return html[brace_start:i + 1]

        return None

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
        blob = self._extract_json_blob(html, "__QUERY_RESULT_DATA__")
        if not blob:
            return []

        try:
            data = json.loads(blob)
        except Exception as e:
            logger.debug(f"query data json parse failed: {e}")
            return []

        hotel_list = data.get("hotelList") or []
        if not isinstance(hotel_list, list):
            return []

        hotels: list[dict] = []

        for item in hotel_list:
            if not isinstance(item, dict):
                continue

            hotel_id = item.get("shid") or item.get("hotelId") or item.get("hotel_id")
            if hotel_id is None:
                continue
            hotel_id = str(hotel_id)

            name = item.get("name") or item.get("hotelName") or item.get("title")
            if not name:
                continue

            name = normalize_hotel_name(clean_text(str(name)))
            if not name:
                continue

            rating_score = None
            rate_score_raw = item.get("rateScore")
            if rate_score_raw is not None:
                try:
                    rating_score = float(rate_score_raw)
                except Exception:
                    rating_score = None

            review_count = 0
            rate_num_raw = item.get("rateNum")
            if rate_num_raw is not None:
                try:
                    review_count = int(rate_num_raw)
                except Exception:
                    review_count = 0

            base_price = None
            price_disp = item.get("priceDesp")
            if price_disp is not None:
                base_price = extract_price(str(price_disp))

            if base_price is None:
                price_without_tax = item.get("priceWithoutTax") or {}
                if isinstance(price_without_tax, dict):
                    amount_cny = price_without_tax.get("amountCNY")
                    if amount_cny is not None:
                        try:
                            base_price = int(amount_cny)
                        except Exception:
                            base_price = None

            if base_price is None:
                price_raw = item.get("price")
                if price_raw is not None:
                    try:
                        price_raw = float(price_raw)
                        base_price = int(price_raw / 100) if price_raw > 1000 else int(price_raw)
                    except Exception:
                        base_price = None

            address = item.get("address")
            if address:
                address = clean_text(str(address))

            star_level = None
            level = item.get("level") or {}
            if isinstance(level, dict):
                star_level = level.get("desc") or level.get("starRate") or level.get("star")

            latitude = None
            longitude = None
            lat_raw = item.get("lat")
            lng_raw = item.get("lng")
            try:
                if lat_raw is not None:
                    latitude = float(lat_raw)
                if lng_raw is not None:
                    longitude = float(lng_raw)
            except Exception:
                latitude = None
                longitude = None

            hotels.append({
                "hotel_id": hotel_id,
                "name": name,
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
                "star_level": star_level,
                "rating_score": rating_score,
                "review_count": review_count,
                "base_price": base_price,
            })

        return hotels

    def _get_query_page_info(self, html: str) -> Optional[dict]:
        """Get paging info from __QUERY_RESULT_DATA__."""
        blob = self._extract_json_blob(html, "__QUERY_RESULT_DATA__")
        if not blob:
            return None

        try:
            data = json.loads(blob)
        except Exception:
            return None

        query = data.get("query")
        if not isinstance(query, dict):
            return None

        return {
            "currentPage": query.get("currentPage"),
            "totalPage": query.get("totalPage"),
            "pageSize": query.get("pageSize"),
            "offset": query.get("offset"),
        }

    def _update_url_param(self, url: str, key: str, value: int) -> str:
        """Update/add a query param for pagination."""
        if not url:
            return url

        parts = urlsplit(url)
        query = parse_qs(parts.query, keep_blank_values=True)
        query[key] = [str(value)]
        new_query = urlencode(query, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

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
                current_page = int(page_info.get("currentPage"))
            except Exception:
                current_page = None
            try:
                total_page = int(page_info.get("totalPage"))
            except Exception:
                total_page = None
            try:
                page_size = int(page_info.get("pageSize"))
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
                    if not self.anti_crawler.navigate_to(url):
                        continue
                    time.sleep(2)
                    new_info = self._get_query_page_info(page.html)
                    if new_info:
                        try:
                            new_page = int(new_info.get("currentPage"))
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
            return True

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
        target_count: int = None,
        exclude_ids: Optional[set[str]] = None,
        save_to_db: bool = True,
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

        logger.info(f"开始爬取: {region_type} - {zone_name} - {price_level} (目标: {top_n}家)")

        # 构建URL
        url = self.build_search_url(
            business_zone_code=zone_code,
            price_min=price_range['min'],
            price_max=price_range['max'],
        )

        # 导航到页面
        if not self.anti_crawler.navigate_to(url):
            logger.error(f"导航失败: {url}")
            return []

        # 等待页面加载 - 增加等待时间
        logger.info("等待页面完全加载...")
        self.anti_crawler.random_delay(5, 8)



        # 提取酒店（支持翻页，直到达到目标数量）
        hotels = self.extract_hotels_from_page(
            max_hotels=top_n,
            exclude_ids=exclude_ids,
            min_review_count=settings.min_reviews_threshold,
            price_range=price_range,
        )

        # 按起步价校验所属档次，确保价格区间与档次一致
        filtered_hotels = []
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
            filtered_hotels.append(hotel)

        if skipped_no_price or skipped_price_mismatch:
            logger.info(
                f"{zone_name} - {price_level} 过滤: "
                f"无价格{skipped_no_price}家, 价格不匹配{skipped_price_mismatch}家"
            )
        hotels = filtered_hotels

        logger.info(f"爬取完成: {len(hotels)}/{top_n} 家酒店")

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

            actual = len(hotels)
            remaining -= actual
            carry_over = target - actual

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

                actual = len(hotels)
                remaining -= actual

                # 每次爬取后随机延迟
                self.anti_crawler.random_delay(3, 6)

            if remaining > 0:
                logger.warning(
                    f"{zone_name} 最终仍缺少 {remaining} 家，"
                    f"可能需要调整筛选或增加页数"
                )

        logger.info(
            f"商圈 {zone_name} 弹性补位完成，实际 {len(zone_hotels)}/{zone_target_total} 家"
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

        logger.info(f"全部爬取完成，共 {len(all_hotels)} 家酒店")
        return all_hotels

    def _save_hotels(self, hotels: list[dict], fetch_details: bool = False) -> int:
        """保存酒店到数据库（优化去重逻辑，避免跨商圈/价格档次的重复）

        Args:
            hotels: 酒店数据列表
            fetch_details: 是否获取详细信息（默认False，因为详情页不可用）

        Returns:
            成功保存的数量
        """
        if not hotels:
            return 0

        min_review_count = settings.min_reviews_threshold
        filtered_hotels = []
        skipped_low_review = 0
        if min_review_count:
            for hotel in hotels:
                review_count = hotel.get('review_count') or 0
                try:
                    review_count = int(review_count)
                except Exception:
                    review_count = 0
                if review_count <= min_review_count:
                    skipped_low_review += 1
                    continue
                filtered_hotels.append(hotel)
            hotels = filtered_hotels

        if not hotels:
            if skipped_low_review:
                logger.info(f"低评论酒店已过滤: {skipped_low_review} 家")
            return 0

        saved_count = 0
        updated_count = 0
        skipped_count = 0

        with session_scope() as session:
            # 批量查询已存在的酒店ID（减少数据库查询次数）
            hotel_ids = [h['hotel_id'] for h in hotels if 'hotel_id' in h]
            existing_hotels = session.query(Hotel).filter(
                Hotel.hotel_id.in_(hotel_ids)
            ).all()
            
            # 构建已存在酒店的字典，包含商圈和价格档次信息
            # 格式: {hotel_id: {(business_zone_code, price_level): Hotel对象}}
            existing_dict = {}
            for h in existing_hotels:
                if h.hotel_id not in existing_dict:
                    existing_dict[h.hotel_id] = {}
                key = (h.business_zone_code, h.price_level)
                existing_dict[h.hotel_id][key] = h
            
            logger.debug(f"批量查询: {len(hotel_ids)}个酒店ID, 已存在{len(existing_hotels)}条记录")

            for hotel_data in hotels:
                try:
                    hotel_id = hotel_data.get('hotel_id')
                    business_zone_code = hotel_data.get('business_zone_code')
                    price_level = hotel_data.get('price_level')
                    
                    # 检查是否存在相同商圈和价格档次的记录
                    key = (business_zone_code, price_level)
                    is_duplicate = (
                        hotel_id in existing_dict and
                        key in existing_dict[hotel_id]
                    )
                    
                    if is_duplicate:
                        # 同一酒店在同一商圈和价格档次下已存在，跳过
                        skipped_count += 1
                        logger.debug(f"跳过重复酒店: {hotel_data.get('name')} (商圈:{business_zone_code}, 价格档次:{price_level})")
                        continue
                    
                    # 如果是新酒店且需要获取详情
                    if fetch_details and hotel_id not in existing_dict:
                        logger.info(f"获取酒店 {hotel_id} 的详细信息...")
                        details = self.fetch_hotel_details(hotel_id)
                        
                        if details:
                            # 合并详细信息
                            hotel_data.update(details)
                            logger.debug(f"成功获取详细信息，更新了 {len(details)} 个字段")
                        
                        # 每次详情请求后延迟，避免请求过快
                        self.anti_crawler.random_delay(2, 4)
                    
                    # 验证数据
                    mapped_level = self._map_price_level(hotel_data.get('base_price'))
                    if mapped_level:
                        hotel_data['price_level'] = mapped_level

                    validated = HotelModel(**hotel_data)

                    # 检查是否存在该酒店ID的任何记录
                    if hotel_id in existing_dict:
                        # 酒店已存在但在不同的商圈或价格档次，更新基本信息但保留分层信息
                        # 获取第一个已存在的记录用于更新基本信息
                        first_existing = list(existing_dict[hotel_id].values())[0]
                        
                        # 只更新基本信息字段（不更新分层信息）
                        basic_fields = ['name', 'address', 'latitude', 'longitude',
                                       'star_level', 'rating_score', 'review_count', 'base_price']
                        for field in basic_fields:
                            value = validated.model_dump().get(field)
                            if value is not None:
                                setattr(first_existing, field, value)

                        if mapped_level:
                            first_existing.price_level = mapped_level
                        
                        updated_count += 1
                        logger.debug(f"更新酒店基本信息: {validated.name}")
                    else:
                        # 创建新记录
                        hotel = Hotel(**validated.model_dump())
                        session.add(hotel)
                        saved_count += 1
                        logger.debug(f"新增酒店: {validated.name} (商圈:{business_zone_code}, 价格档次:{price_level})")

                except Exception as e:
                    logger.warning(f"保存酒店失败: {e}")
                    continue

        total = saved_count + updated_count
        logger.info(f"保存完成: 新增{saved_count}家, 更新{updated_count}家, 跳过{skipped_count}家, 共处理{total}家")
        return total

    def enrich_hotel_details(self, hotel_ids: list[str] = None) -> int:
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
                    
                    details = self.fetch_hotel_details(hotel.hotel_id)
                    
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
            hotels = session.query(Hotel).filter(
                Hotel.review_count >= settings.min_reviews_threshold
            ).all()

            for hotel in hotels:
                yield hotel

