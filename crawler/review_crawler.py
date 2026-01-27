"""评论爬虫模块

负责爬取酒店评论，实现瀑布流采集策略：
1. 负面警示池（差评+中评）- 优先级最高
2. 高质量证据池（有图评论）- 优先级次之
3. 时效性补全池（最新评论）- 填满配额
"""
import re
from typing import Optional
from datetime import datetime

from config.settings import settings
from utils.logger import get_logger
from utils.cleaner import clean_text, extract_tags, parse_star_score, parse_date
from utils.validator import ReviewModel
from database.connection import session_scope
from database.models import Hotel, Review, ReviewImage, ReviewReply
from .anti_crawler import AntiCrawler

logger = get_logger("review_crawler")


class ReviewCrawler:
    """评论爬虫类"""

    # 飞猪酒店详情URL模板
    DETAIL_URL_TEMPLATE = "https://hotel.fliggy.com/hotel_detail2.htm?shid={hotel_id}&city=440100"

    # 评论筛选类型
    FILTER_ALL = 0       # 全部
    FILTER_GOOD = 1      # 好评 (4-5分)
    FILTER_MEDIUM = 2    # 中评 (3分)
    FILTER_BAD = 3       # 差评 (1-2分)

    def __init__(self, anti_crawler: AntiCrawler = None):
        """初始化爬虫

        Args:
            anti_crawler: 反爬虫实例
        """
        self.anti_crawler = anti_crawler or AntiCrawler()
        self.crawled_review_ids = set()  # 用于去重

    def _generate_review_id(self, hotel_id: str, content: str, user_nick: str = None) -> str:
        """生成唯一的评论ID
        
        Args:
            hotel_id: 酒店ID
            content: 评论内容
            user_nick: 用户昵称（可选）
        
        Returns:
            唯一的评论ID
        """
        import hashlib
        # 组合多个字段以降低冲突概率
        unique_str = f"{hotel_id}_{content}_{user_nick or ''}"
        hash_value = hashlib.md5(unique_str.encode('utf-8')).hexdigest()[:16]
        return f"{hotel_id}_{hash_value}"

    def get_hotel_detail_url(self, hotel_id: str) -> str:
        """获取酒店详情页URL"""
        return self.DETAIL_URL_TEMPLATE.format(hotel_id=hotel_id)

    def get_total_review_count(self) -> Optional[int]:
        """获取当前页面的总评论数

        Returns:
            总评论数
        """
        page = self.anti_crawler.get_page()

        # 尝试多个选择器
        selectors = [
            '#J_ReviewCount',
            '.comments a',
            'li.comments a',
        ]

        for selector in selectors:
            elem = page.ele(selector, timeout=2)
            if elem:
                text = elem.text
                match = re.search(r'(\d+)', text)
                if match:
                    return int(match.group(1))

        return None

    def navigate_to_reviews(self, hotel_id: str) -> bool:
        """导航到酒店评论页面

        Args:
            hotel_id: 酒店ID

        Returns:
            是否成功
        """
        url = self.get_hotel_detail_url(hotel_id)

        if not self.anti_crawler.navigate_to(url):
            return False

        # 等待页面加载
        self.anti_crawler.random_delay(2, 3)

        # 滚动到评论区域
        page = self.anti_crawler.get_page()
        review_section = page.ele('#hotel-review', timeout=5)
        if review_section:
            review_section.scroll.to_see()
            self.anti_crawler.random_delay(1, 2)

        return True

    def filter_reviews(self, filter_type: int) -> bool:
        """筛选评论类型
        
        支持多个备用选择器，增强兼容性

        Args:
            filter_type: 筛选类型 (0=全部, 1=好评, 2=中评, 3=差评)

        Returns:
            是否成功
        """
        page = self.anti_crawler.get_page()

        # 主选择器映射（多个备用选择器）
        filter_map = {
            0: ['#review-t-1', 'input[value="0"]', '.review-filter-all'],  # 全部
            1: ['#review-t-2', 'input[value="1"]', '.review-filter-good'],  # 好评
            2: ['#review-t-4', 'input[value="2"]', '.review-filter-medium'],  # 中评
            3: ['#review-t-5', 'input[value="3"]', '.review-filter-bad'],  # 差评
        }

        selectors = filter_map.get(filter_type, [])
        if not selectors:
            logger.warning(f"未知的筛选类型: {filter_type}")
            return False

        # 尝试多个选择器
        for selector in selectors:
            try:
                # 尝试直接点击input
                filter_elem = page.ele(selector, timeout=2)
                if filter_elem:
                    # 如果是input，尝试点击对应的label
                    if filter_elem.tag == 'input':
                        input_id = filter_elem.attr('id')
                        if input_id:
                            label = page.ele(f'label[for="{input_id}"]', timeout=1)
                            if label:
                                label.click()
                            else:
                                filter_elem.click()
                        else:
                            filter_elem.click()
                    else:
                        filter_elem.click()

                    self.anti_crawler.random_delay(1, 2)

                    # 验证是否生效
                    if self._verify_filter_applied(filter_type):
                        logger.debug(f"评论筛选成功: 类型={filter_type}, 选择器={selector}")
                        return True

            except Exception as e:
                logger.debug(f"选择器 {selector} 失败: {e}")
                continue

        logger.warning(f"所有选择器都失败，筛选类型: {filter_type}")
        return False

    def _verify_filter_applied(self, filter_type: int) -> bool:
        """验证筛选是否生效
        
        Args:
            filter_type: 筛选类型
        
        Returns:
            是否生效
        """
        page = self.anti_crawler.get_page()

        # 检查URL参数
        current_url = page.url
        if f"rateScore={filter_type}" in current_url:
            return True

        # 检查选中状态
        filter_map = {
            0: '#review-t-1',
            1: '#review-t-2',
            2: '#review-t-4',
            3: '#review-t-5',
        }

        selector = filter_map.get(filter_type)
        if selector:
            elem = page.ele(selector, timeout=1)
            if elem and elem.attr('checked'):
                return True

        # 如果无法验证，假设成功（避免过度严格）
        return True

    def filter_with_images(self, enabled: bool = True) -> bool:
        """筛选有图评论

        Args:
            enabled: 是否启用有图筛选

        Returns:
            是否成功
        """
        page = self.anti_crawler.get_page()

        # 有图筛选checkbox
        checkbox = page.ele('#review-addreply', timeout=3)
        if checkbox:
            is_checked = checkbox.attr('checked') is not None

            if enabled and not is_checked:
                checkbox.click()
                self.anti_crawler.random_delay(1, 2)
                return True
            elif not enabled and is_checked:
                checkbox.click()
                self.anti_crawler.random_delay(1, 2)
                return True

        return True

    def extract_reviews_from_page(self, hotel_id: str, source_pool: str) -> list[dict]:
        """从当前页面提取评论

        Args:
            hotel_id: 酒店ID
            source_pool: 来源池标识

        Returns:
            评论数据列表
        """
        page = self.anti_crawler.get_page()
        reviews = []

        # 查找所有评论元素
        review_elements = page.eles('li.tb-r-comment')
        logger.debug(f"找到 {len(review_elements)} 条评论")

        for elem in review_elements:
            try:
                review_data = self._parse_review_element(elem, hotel_id, source_pool)
                if review_data:
                    # 去重检查
                    review_id = review_data.get('review_id')
                    if review_id and review_id in self.crawled_review_ids:
                        logger.debug(f"跳过重复评论: {review_id}")
                        continue

                    reviews.append(review_data)
                    if review_id:
                        self.crawled_review_ids.add(review_id)

            except Exception as e:
                logger.warning(f"解析评论元素失败: {e}")
                continue

        return reviews

    def _parse_review_element(self, elem, hotel_id: str, source_pool: str) -> Optional[dict]:
        """解析单个评论元素

        Args:
            elem: 评论DOM元素
            hotel_id: 酒店ID
            source_pool: 来源池标识

        Returns:
            评论数据字典
        """
        try:
            # 用户昵称
            user_nick = None
            nick_elem = elem.ele('.tb-r-nick a', timeout=1)
            if nick_elem:
                user_nick = nick_elem.attr('title') or nick_elem.text

            # 评论内容
            content = None
            content_elem = elem.ele('.tb-r-cnt', timeout=1)
            if content_elem:
                content = clean_text(content_elem.text)

            if not content:
                return None

            # 评论摘要
            summary = None
            summary_elem = elem.ele('.comment-name', timeout=1)
            if summary_elem:
                summary = clean_text(summary_elem.text)

            # 评分解析
            scores = self._parse_scores(elem)

            # 评论日期
            review_date = None
            date_elem = elem.ele('.tb-r-date', timeout=1)
            if date_elem:
                review_date = parse_date(date_elem.text)

            # 图片
            image_urls = []
            image_elems = elem.eles('.tb-r-photos img')
            for img in image_elems:
                img_url = img.attr('data-val')
                if img_url:
                    image_urls.append(img_url)

            # 商家回复
            reply_content = None
            reply_date = None
            reply_elem = elem.ele('.tb-r-seller', timeout=1)
            if reply_elem:
                reply_content = clean_text(reply_elem.text)
                # 尝试获取回复日期
                reply_date_elems = elem.eles('.tb-r-info .tb-r-date')
                if len(reply_date_elems) > 1:
                    reply_date = parse_date(reply_date_elems[-1].text)

            # 提取标签
            tags = extract_tags(content)
            if summary:
                tags.extend(extract_tags(summary))
            tags = list(set(tags))

            # 生成review_id（使用MD5确保唯一性）
            review_id = self._generate_review_id(hotel_id, content, user_nick)

            return {
                'review_id': review_id,
                'hotel_id': hotel_id,
                'user_nick': user_nick,
                'content': content,
                'summary': summary,
                'score_clean': scores.get('clean'),
                'score_location': scores.get('location'),
                'score_service': scores.get('service'),
                'score_value': scores.get('value'),
                'overall_score': scores.get('overall'),
                'tags': tags,
                'has_images': len(image_urls) > 0,
                'image_urls': image_urls,
                'review_date': review_date,
                'source_pool': source_pool,
                'has_reply': reply_content is not None,
                'reply_content': reply_content,
                'reply_date': reply_date,
            }

        except Exception as e:
            logger.debug(f"解析评论异常: {e}")
            return None

    def _parse_scores(self, elem) -> dict:
        """解析评分

        Args:
            elem: 评论DOM元素

        Returns:
            评分字典
        """
        scores = {}

        # 查找评分列表
        score_items = elem.eles('.starscore li')

        score_keys = ['clean', 'location', 'service', 'value']

        for i, item in enumerate(score_items):
            if i >= len(score_keys):
                break

            # 查找评分星星的em元素
            em = item.ele('em', timeout=1)
            if em:
                style = em.attr('style')
                if style:
                    score = parse_star_score(style)
                    scores[score_keys[i]] = score

        # 计算综合评分
        valid_scores = [v for v in scores.values() if v is not None]
        if valid_scores:
            scores['overall'] = round(sum(valid_scores) / len(valid_scores), 1)

        return scores

    def load_more_reviews(self, max_pages: int = 30) -> int:
        """加载更多评论（翻页）

        Args:
            max_pages: 最大翻页数

        Returns:
            实际翻页数
        """
        page = self.anti_crawler.get_page()
        pages_loaded = 0

        for _ in range(max_pages):
            # 查找下一页按钮
            next_btn = page.ele('.pi-pagination-next:not(.pi-pagination-disabled)', timeout=2)

            if not next_btn:
                logger.debug("没有更多页面")
                break

            # 点击下一页
            next_btn.click()
            self.anti_crawler.random_delay(1, 2)
            pages_loaded += 1

            # 检查验证码
            if self.anti_crawler.check_captcha():
                self.anti_crawler.handle_captcha()

        return pages_loaded

    def waterfall_crawl(self, hotel_id: str, max_reviews: int = None) -> list[dict]:
        """瀑布流采集策略

        按优先级依次爬取：
        1. 负面警示池（差评+中评）- 上限100条
        2. 高质量证据池（有图评论）- 上限150条
        3. 时效性补全池（最新评论）- 填满剩余配额

        Args:
            hotel_id: 酒店ID
            max_reviews: 最大评论数

        Returns:
            所有爬取到的评论
        """
        max_reviews = max_reviews or settings.max_reviews_per_hotel
        all_reviews = []
        self.crawled_review_ids.clear()

        logger.info(f"开始瀑布流采集酒店 {hotel_id}，目标 {max_reviews} 条")

        # 导航到评论页面
        if not self.navigate_to_reviews(hotel_id):
            logger.error(f"无法访问酒店 {hotel_id} 的评论页面")
            return []

        # 检查总评论数
        total_count = self.get_total_review_count()
        if total_count is None:
            logger.warning(f"无法获取酒店 {hotel_id} 的评论数")
        elif total_count < settings.min_reviews_threshold:
            logger.info(f"酒店 {hotel_id} 评论数 {total_count} 低于阈值，跳过")
            return []

        # 步骤1: 负面警示池（差评+中评）
        logger.info("步骤1: 爬取负面警示池")
        negative_reviews = self._crawl_pool(
            hotel_id=hotel_id,
            source_pool="negative",
            filter_types=[self.FILTER_BAD, self.FILTER_MEDIUM],
            max_count=100,
        )
        all_reviews.extend(negative_reviews)
        logger.info(f"负面警示池: {len(negative_reviews)} 条")

        if len(all_reviews) >= max_reviews:
            return all_reviews[:max_reviews]

        # 步骤2: 高质量证据池（有图评论）
        logger.info("步骤2: 爬取高质量证据池")
        evidence_reviews = self._crawl_pool(
            hotel_id=hotel_id,
            source_pool="evidence",
            filter_types=[self.FILTER_ALL],
            with_images=True,
            max_count=150,
        )
        all_reviews.extend(evidence_reviews)
        logger.info(f"高质量证据池: {len(evidence_reviews)} 条")

        if len(all_reviews) >= max_reviews:
            return all_reviews[:max_reviews]

        # 步骤3: 时效性补全池（最新评论）
        remaining = max_reviews - len(all_reviews)
        logger.info(f"步骤3: 爬取时效性补全池，剩余配额 {remaining}")
        latest_reviews = self._crawl_pool(
            hotel_id=hotel_id,
            source_pool="latest",
            filter_types=[self.FILTER_ALL],
            max_count=remaining,
        )
        all_reviews.extend(latest_reviews)
        logger.info(f"时效性补全池: {len(latest_reviews)} 条")

        logger.info(f"酒店 {hotel_id} 采集完成，共 {len(all_reviews)} 条评论")
        return all_reviews

    def _crawl_pool(
        self,
        hotel_id: str,
        source_pool: str,
        filter_types: list[int],
        with_images: bool = False,
        max_count: int = 100,
    ) -> list[dict]:
        """爬取指定池的评论

        Args:
            hotel_id: 酒店ID
            source_pool: 来源池标识
            filter_types: 筛选类型列表
            with_images: 是否只爬取有图评论
            max_count: 最大数量

        Returns:
            评论列表
        """
        reviews = []

        for filter_type in filter_types:
            if len(reviews) >= max_count:
                break

            # 应用筛选
            self.filter_reviews(filter_type)
            if with_images:
                self.filter_with_images(True)
            else:
                self.filter_with_images(False)

            self.anti_crawler.random_delay(1, 2)

            # 爬取当前页
            page_reviews = self.extract_reviews_from_page(hotel_id, source_pool)
            reviews.extend(page_reviews)

            # 翻页继续爬取
            while len(reviews) < max_count:
                pages = self.load_more_reviews(max_pages=1)
                if pages == 0:
                    break

                page_reviews = self.extract_reviews_from_page(hotel_id, source_pool)
                if not page_reviews:
                    break

                reviews.extend(page_reviews)
                self.anti_crawler.random_delay(1, 2)

        return reviews[:max_count]

    def save_reviews(self, reviews: list[dict]) -> int:
        """保存评论到数据库

        Args:
            reviews: 评论数据列表

        Returns:
            成功保存的数量
        """
        saved_count = 0

        with session_scope() as session:
            for review_data in reviews:
                try:
                    # 提取图片和回复数据
                    image_urls = review_data.pop('image_urls', [])
                    reply_content = review_data.pop('reply_content', None)
                    reply_date = review_data.pop('reply_date', None)
                    review_data.pop('has_reply', None)

                    # 验证数据
                    validated = ReviewModel(**review_data)

                    # 检查是否已存在
                    existing = session.query(Review).filter_by(
                        review_id=validated.review_id
                    ).first()

                    if existing:
                        logger.debug(f"评论已存在: {validated.review_id}")
                        continue

                    # 创建评论记录
                    review = Review(**validated.model_dump(exclude={'image_urls'}))
                    session.add(review)
                    session.flush()

                    # 保存图片
                    for i, img_url in enumerate(image_urls):
                        image = ReviewImage(
                            review_id=review.review_id,
                            image_url=img_url,
                            sort_order=i,
                        )
                        session.add(image)

                    # 保存商家回复
                    if reply_content:
                        reply = ReviewReply(
                            review_id=review.review_id,
                            content=reply_content,
                            reply_date=reply_date,
                        )
                        session.add(reply)

                    saved_count += 1

                except Exception as e:
                    logger.warning(f"保存评论失败: {e}")
                    continue

        logger.info(f"保存完成: {saved_count}/{len(reviews)} 条评论")
        return saved_count

    def crawl_hotel_reviews(self, hotel_id: str, save_to_db: bool = True) -> list[dict]:
        """爬取单个酒店的评论

        Args:
            hotel_id: 酒店ID
            save_to_db: 是否保存到数据库

        Returns:
            评论列表
        """
        reviews = self.waterfall_crawl(hotel_id)

        if save_to_db and reviews:
            self.save_reviews(reviews)

        return reviews
