"""评论爬虫模块。

当前采用双池策略：
1. negative：稳定的差评在线分页主采
2. positive：人工辅助翻页 + 机器提取
"""
import math
import json
import re
import time
from typing import Any, Optional, Sequence
from datetime import datetime
from urllib.parse import urljoin

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency fallback
    BeautifulSoup = None

from config.settings import settings
from utils.logger import get_logger
from utils.cleaner import clean_text, extract_tags, parse_star_score, parse_date
from utils.validator import ReviewModel
from utils.checkpoint_manager import CheckpointManager, looks_like_recoverable_error
from database.connection import session_scope
from database.models import Hotel, Review, ReviewNegative, ReviewPositive
from .anti_crawler import AntiCrawler
from .exceptions import (
    BrowserConnectionException,
    CaptchaCooldownException,
    CaptchaException,
    CaptchaTimeoutException,
    RecoverableInterruption,
)

logger = get_logger("review_crawler")


class ReviewCrawler:
    """评论爬虫类"""

    # 飞猪酒店详情URL模板
    DETAIL_URL_TEMPLATE = "https://hotel.fliggy.com/hotel_detail2.htm?shid={hotel_id}&city=440100"
    REVIEW_URL_TEMPLATE = "https://www.fliggy.com/jiudian/detail/440100/{hotel_id}/dianping"

    # 评论筛选类型
    FILTER_ALL = 0       # 全部
    FILTER_GOOD = 1      # 好评 (4-5分)
    FILTER_BAD = 2       # 差评
    SAFE_JSONP_FILTER_TYPES = {FILTER_BAD}

    VERIFICATION_EXPIRED_KEYWORDS = (
        "验证已过期",
        "验证失效",
        "请刷新",
        "页面已失效",
        "重新验证",
        "点击刷新",
    )

    REVIEW_LIST_SELECTORS: tuple[str, ...] = (
        '#J_ReviewList li.tb-r-comment',
        '#J_ReviewList .tb-r-comment',
        '#J_Reviews li.tb-r-comment',
        '.review-content.review-bd.no-border li.tb-r-comment',
        'div.review-content.review-bd.no-border .review-list li.tb-r-comment',
        '#J_ReviewList li',
        'li.tb-r-comment',
        '.tb-r-comment',
        '.review-list li',
    )

    CONTENT_SELECTORS: tuple[str, ...] = (
        '.tb-r-cnt',
        '.review-content',
        '.comment-content',
    )

    SUMMARY_SELECTORS: tuple[str, ...] = (
        '.comment-name',
        '.tb-r-summary',
        '.review-title',
    )

    DATE_SELECTORS: tuple[str, ...] = (
        '.tb-r-date',
        '.review-date',
    )

    NICK_SELECTORS: tuple[str, ...] = (
        '.tb-r-nick a',
        '.tb-r-nick',
        '.review-user a',
    )

    HOTEL_ASPECT_KEYWORDS: tuple[str, ...] = (
        "位置", "交通", "地铁", "机场", "车站", "卫生", "干净", "隔音", "噪音",
        "服务", "前台", "早餐", "设施", "房间", "床", "空调", "网络", "价格",
        "性价比", "停车", "环境", "浴室", "热水", "窗户", "电梯", "浴缸", "洗手间",
        "wifi", "WiFi", "大厅", "周边", "商圈",
    )

    NEGATIVE_ASPECT_KEYWORDS: tuple[str, ...] = (
        "发霉", "脏", "吵", "漏水", "异味", "冷漠", "排队", "卡顿", "坏", "旧", "小",
        "差", "潮湿", "霉味", "蟑螂", "蚊子", "烟味", "臭", "堵", "慢", "黑",
    )

    SPECIFIC_SHORT_TRIGGER_KEYWORDS: tuple[str, ...] = (
        "方便", "不错", "干净", "安静", "卫生", "热情", "快捷", "丰盛", "舒适", "划算",
        "很差", "太差", "发霉", "漏水", "异味", "冷漠", "排队", "卡顿", "吵", "脏",
        "旧", "小", "坏", "慢", "臭", "霉", "差劲",
    )

    GENERIC_SHORT_COMMENT_BLACKLIST: tuple[str, ...] = (
        "不错", "满意", "还行", "可以", "很好", "挺好", "一般", "还不错", "蛮好",
        "可以的", "还可以", "非常好", "不错不错", "好评", "推荐",
    )

    def __init__(self, anti_crawler: Optional[AntiCrawler] = None, positive_manual: Optional[bool] = None):
        """初始化爬虫

        Args:
            anti_crawler: 反爬虫实例
        """
        self.anti_crawler = anti_crawler or AntiCrawler()
        self.positive_manual = False if positive_manual is None else positive_manual
        self.crawled_review_ids = set()  # 用于去重
        self._review_detail_fallback_attempted: set[str] = set()
        self._review_context: dict[str, object] = {}
        self._pool_recovery_buffer: list[dict[str, Any]] = []
        self.checkpoints = CheckpointManager()
        self._network_capture_targets = [
            'getHotelRates.htm',
            'getItemRates.htm',
            'getReviews.do',
            'getratelist',
            'mtop.trip.hotel',
            'review',
        ]
        self._jsonp_punished_scores: set[int] = set()
        self.last_crawl_summary: dict[str, Any] = {}
        self._quality_pool_stats: dict[str, dict[str, Any]] = {}
        self._manual_takeover_count = 0
        self._manual_timeout_reminder_count = 0

    def _update_review_context(self, **kwargs: object) -> None:
        """更新评论采集上下文，便于统一输出更清晰的日志。"""
        for key, value in kwargs.items():
            if value is None:
                self._review_context.pop(key, None)
            else:
                self._review_context[key] = value

    def _clear_review_context(self) -> None:
        """清理评论采集上下文。"""
        self._review_context.clear()

    def _format_review_context(self) -> str:
        """格式化评论采集上下文。"""
        ordered_keys = [
            "hotel_id",
            "source_pool",
            "filter_type",
            "page_no",
            "progress",
            "current_url",
        ]
        parts = []
        for key in ordered_keys:
            value = self._review_context.get(key)
            if value is not None:
                parts.append(f"{key}={value}")
        return ", ".join(parts) if parts else "context=unknown"

    @staticmethod
    def _review_checkpoint_key(hotel_id: str) -> str:
        return str(hotel_id).strip()

    def _load_review_checkpoint(self, hotel_id: str) -> Optional[dict[str, Any]]:
        return self.checkpoints.load("reviews", self._review_checkpoint_key(hotel_id))

    def _save_review_checkpoint(self, hotel_id: str, payload: dict[str, Any]) -> str:
        return self.checkpoints.save("reviews", self._review_checkpoint_key(hotel_id), payload)

    def _clear_review_checkpoint(self, hotel_id: str) -> None:
        self.checkpoints.clear("reviews", self._review_checkpoint_key(hotel_id))

    def _load_existing_reviews_from_db(self, hotel_id: str) -> list[dict[str, Any]]:
        """从数据库回填当前酒店已保存评论，用于断点恢复后的计数与去重。"""
        reviews: list[dict[str, Any]] = []
        with session_scope() as session:
            rows = (
                session.query(Review)
                .filter(Review.hotel_id == hotel_id)
                .order_by(Review.created_at.asc(), Review.id.asc())
                .all()
            )
            for row in rows:
                reviews.append(
                    {
                        "review_id": row.review_id,
                        "hotel_id": row.hotel_id,
                        "user_nick": row.user_nick,
                        "content": row.content,
                        "summary": row.summary,
                        "score_clean": row.score_clean,
                        "score_location": row.score_location,
                        "score_service": row.score_service,
                        "score_value": row.score_value,
                        "overall_score": row.overall_score,
                        "tags": row.tags or [],
                        "room_type": row.room_type,
                        "review_date": row.review_date,
                        "source_pool": row.source_pool,
                    }
                )
        return reviews

    def _get_hotel_review_count_from_db(self, hotel_id: str) -> Optional[int]:
        """读取酒店表中的评论总量，用于计算动态配额。"""
        with session_scope() as session:
            hotel = session.query(Hotel).filter(Hotel.hotel_id == hotel_id).one_or_none()

        if not hotel or getattr(hotel, "review_count", None) is None:
            return None

        try:
            return int(hotel.review_count)
        except Exception:
            return None

    def _calculate_review_targets(
        self,
        review_count: Optional[int],
        *,
        max_reviews_cap: Optional[int] = None,
    ) -> dict[str, int]:
        """根据酒店热度计算动态评论采样配额。"""
        sample_ratio = float(getattr(settings, "review_total_sample_ratio", 0.20))
        total_min = int(getattr(settings, "review_total_min_per_hotel", 80))
        total_max = int(getattr(settings, "review_total_max_per_hotel", getattr(settings, "max_reviews_per_hotel", 300)))
        negative_ratio = float(getattr(settings, "review_negative_target_ratio", 0.25))
        negative_min = int(getattr(settings, "review_negative_min_per_hotel", 20))
        negative_max = int(
            getattr(
                settings,
                "review_negative_max_per_hotel",
                getattr(settings, "review_negative_pool_limit", 80),
            )
        )

        if review_count is None:
            base_total = total_max
        else:
            try:
                base_total = int(round(int(review_count) * sample_ratio))
            except Exception:
                base_total = total_max

        target_total = max(total_min, base_total)
        target_total = min(target_total, total_max)

        if max_reviews_cap is not None:
            target_total = min(target_total, int(max_reviews_cap))
        target_total = max(target_total, 0)

        if target_total <= 0:
            return {
                "review_count": int(review_count or 0),
                "target_total": 0,
                "target_negative": 0,
                "target_positive": 0,
            }

        target_negative = int(round(target_total * negative_ratio))
        target_negative = max(negative_min, target_negative)
        target_negative = min(target_negative, negative_max, target_total)
        target_positive = max(target_total - target_negative, 0)

        return {
            "review_count": int(review_count or 0),
            "target_total": int(target_total),
            "target_negative": int(target_negative),
            "target_positive": int(target_positive),
        }

    @staticmethod
    def _summarize_review_mix(reviews: list[dict[str, Any]]) -> dict[str, int | float]:
        """汇总当前评论集合的正负数量。"""
        negative = sum(1 for item in reviews if str(item.get("source_pool") or "").lower() == "negative")
        positive = sum(1 for item in reviews if str(item.get("source_pool") or "").lower() == "positive")
        total = len(reviews)
        ratio = round(negative / total, 4) if total > 0 else 0.0
        return {
            "actual_total": total,
            "actual_negative": negative,
            "actual_positive": positive,
            "actual_negative_ratio": ratio,
        }

    def _record_last_crawl_summary(
        self,
        hotel_id: str,
        *,
        review_count: Optional[int],
        target_total: int,
        target_negative: int,
        target_positive: int,
        reviews: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """记录最近一次酒店评论采集摘要，供批量报表复用。"""
        summary = {
            "hotel_id": hotel_id,
            "review_count": int(review_count or 0),
            "target_total": int(target_total),
            "target_negative": int(target_negative),
            "target_positive": int(target_positive),
        }
        summary.update(self._summarize_review_mix(reviews))
        quality_totals = self._combine_quality_pool_stats()
        if int(quality_totals.get("accepted_total", 0) or 0) <= 0:
            quality_totals = {
                **quality_totals,
                **self._summarize_quality_from_reviews(reviews),
            }
        summary.update(quality_totals)
        summary["manual_takeover_count"] = int(self._manual_takeover_count)
        summary["manual_timeout_reminder_count"] = int(self._manual_timeout_reminder_count)
        self.last_crawl_summary = summary
        return summary

    def _summarize_quality_from_reviews(self, reviews: list[dict[str, Any]]) -> dict[str, Any]:
        """直接从评论集合汇总质量统计，供断点恢复/补齐报表使用。"""
        normalized_reviews = [self._ensure_review_quality_metadata(item) for item in reviews]
        accepted_total = len(normalized_reviews)
        high_quality_count = len(
            [item for item in normalized_reviews if str(item.get("quality_tier") or "") in {"S", "A", "B"}]
        )
        short_comment_count = len(
            [
                item for item in normalized_reviews
                if item.get("quality_tier") == "C" or bool(item.get("is_specific_short"))
            ]
        )
        specific_short_count = len([item for item in normalized_reviews if bool(item.get("is_specific_short"))])
        return {
            "candidate_total": accepted_total,
            "accepted_total": accepted_total,
            "filtered_quality_total": 0,
            "high_quality_count": high_quality_count,
            "short_comment_count": short_comment_count,
            "specific_short_count": specific_short_count,
            "short_comment_ratio": round(short_comment_count / accepted_total, 4) if accepted_total else 0.0,
            "high_quality_ratio": round(high_quality_count / accepted_total, 4) if accepted_total else 0.0,
        }

    def _combine_quality_pool_stats(self) -> dict[str, Any]:
        """汇总正负评论池的质量统计。"""
        stats = list(self._quality_pool_stats.values())
        candidate_total = sum(int(item.get("candidate_total", 0) or 0) for item in stats)
        accepted_total = sum(int(item.get("accepted_total", 0) or 0) for item in stats)
        filtered_quality_total = max(candidate_total - accepted_total, 0)
        high_quality_total = sum(int(item.get("high_quality_count", 0) or 0) for item in stats)
        short_comment_total = sum(int(item.get("short_comment_count", 0) or 0) for item in stats)
        specific_short_total = sum(int(item.get("specific_short_count", 0) or 0) for item in stats)
        quality_relaxed_used = any(bool(item.get("quality_relaxed_used")) for item in stats)
        quality_fallback_used = any(bool(item.get("quality_fallback_used")) for item in stats)
        return {
            "candidate_total": candidate_total,
            "accepted_total": accepted_total,
            "filtered_quality_total": filtered_quality_total,
            "high_quality_count": high_quality_total,
            "short_comment_count": short_comment_total,
            "specific_short_count": specific_short_total,
            "short_comment_ratio": round(short_comment_total / accepted_total, 4) if accepted_total else 0.0,
            "high_quality_ratio": round(high_quality_total / accepted_total, 4) if accepted_total else 0.0,
            "quality_relaxed_used": quality_relaxed_used,
            "quality_fallback_used": quality_fallback_used,
        }

    @staticmethod
    def _effective_length(text: str) -> int:
        """统计有效字符长度，中文按2单位、英文数字按1单位。"""
        cleaned = clean_text(text or "")
        if not cleaned:
            return 0
        length = 0
        for char in cleaned:
            if re.fullmatch(r"[\u4e00-\u9fff]", char):
                length += 2
            elif re.fullmatch(r"[A-Za-z0-9]", char):
                length += 1
        return length

    @staticmethod
    def _normalize_comment_text(text: str) -> str:
        """归一化评论文本，用于短评黑名单匹配。"""
        cleaned = clean_text(text or "")
        if not cleaned:
            return ""
        return re.sub(r"[\s\W_]+", "", cleaned, flags=re.UNICODE).lower()

    @classmethod
    def _keyword_hits(cls, text: str, keywords: Sequence[str]) -> list[str]:
        """返回命中的关键词列表。"""
        normalized_text = clean_text(text or "")
        if not normalized_text:
            return []
        hits: list[str] = []
        seen: set[str] = set()
        for keyword in keywords:
            if keyword in normalized_text and keyword not in seen:
                hits.append(keyword)
                seen.add(keyword)
        return hits

    def _build_review_quality_metadata(
        self,
        *,
        content: str,
        summary: Optional[str],
        source_pool: str,
    ) -> dict[str, Any]:
        """为评论附加统一的信息量/长度质量元数据。"""
        combined_text = " ".join(part for part in [summary or "", content or ""] if part).strip()
        effective_length = self._effective_length(content)
        aspect_keywords = list(self.HOTEL_ASPECT_KEYWORDS)
        if source_pool == "negative":
            aspect_keywords.extend(self.NEGATIVE_ASPECT_KEYWORDS)

        aspect_hits = self._keyword_hits(combined_text, aspect_keywords)
        aspect_hit_count = len(aspect_hits)
        strict_tier_s_min_len = int(getattr(settings, "review_strict_tier_s_min_len", 40))
        strict_tier_s_aspect_min_len = int(getattr(settings, "review_strict_tier_s_aspect_min_len", 25))
        tier_b_min_len = int(getattr(settings, "review_tier_b_min_len", 12))
        tier_c_min_len = int(getattr(settings, "review_tier_c_min_len", 8))

        if effective_length >= strict_tier_s_min_len or (
            effective_length >= strict_tier_s_aspect_min_len and aspect_hit_count >= 2
        ):
            quality_tier = "S"
        elif effective_length >= 20:
            quality_tier = "A"
        elif effective_length >= tier_b_min_len:
            quality_tier = "B"
        elif effective_length >= tier_c_min_len and aspect_hit_count >= 1:
            quality_tier = "C"
        else:
            quality_tier = "D"

        specific_hits = self._keyword_hits(combined_text, self.SPECIFIC_SHORT_TRIGGER_KEYWORDS)
        is_specific_short = (
            effective_length >= 6
            and effective_length < tier_b_min_len
            and aspect_hit_count >= 1
            and bool(specific_hits)
        )

        normalized_text = self._normalize_comment_text(content)
        is_generic_short = (
            effective_length <= max(6, tier_c_min_len)
            and normalized_text in {self._normalize_comment_text(item) for item in self.GENERIC_SHORT_COMMENT_BLACKLIST}
        )

        return {
            "effective_length": effective_length,
            "aspect_hit_count": aspect_hit_count,
            "quality_tier": quality_tier,
            "is_specific_short": is_specific_short,
            "quality_accept_reason": quality_tier,
            "is_generic_short": is_generic_short,
            "aspect_hits": aspect_hits[:5],
            "specific_hits": specific_hits[:5],
        }

    def _ensure_review_quality_metadata(
        self,
        review: dict[str, Any],
        fallback_source_pool: Optional[str] = None,
    ) -> dict[str, Any]:
        """确保评论包含质量元数据；缺失时按当前规则补齐。"""
        normalized_review = dict(review)
        if "effective_length" in normalized_review and "quality_tier" in normalized_review:
            return normalized_review

        source_pool = str(normalized_review.get("source_pool") or fallback_source_pool or "")
        normalized_review.update(
            self._build_review_quality_metadata(
                content=str(normalized_review.get("content") or ""),
                summary=normalized_review.get("summary"),
                source_pool=source_pool,
            )
        )
        return normalized_review

    @staticmethod
    def _hotel_quality_bucket(review_count: Optional[int]) -> str:
        """按酒店评论总量划分质量控制档位。"""
        try:
            count = int(review_count or 0)
        except Exception:
            count = 0
        if count >= 1000:
            return "high"
        if count >= 300:
            return "mid"
        return "low"

    def _short_comment_ratio_cap(self, review_count: Optional[int]) -> float:
        """按酒店评论量读取短评允许占比。"""
        bucket = self._hotel_quality_bucket(review_count)
        if bucket == "high":
            return float(getattr(settings, "review_short_comment_max_ratio_high", 0.10))
        if bucket == "mid":
            return float(getattr(settings, "review_short_comment_max_ratio_mid", 0.20))
        return float(getattr(settings, "review_short_comment_max_ratio_low", 0.30))

    @staticmethod
    def _quality_stat_template() -> dict[str, Any]:
        """构造默认质量统计结构。"""
        return {
            "candidate_total": 0,
            "accepted_total": 0,
            "filtered_quality_total": 0,
            "high_quality_count": 0,
            "short_comment_count": 0,
            "specific_short_count": 0,
            "short_comment_ratio": 0.0,
            "high_quality_ratio": 0.0,
            "quality_relaxed_used": False,
            "quality_fallback_used": False,
        }

    def _clone_review_with_reason(self, review: dict[str, Any], reason: str) -> dict[str, Any]:
        """复制评论并标注被接纳的质量原因。"""
        cloned = dict(review)
        cloned["quality_accept_reason"] = reason
        return cloned

    def _apply_quality_selection(
        self,
        reviews: list[dict[str, Any]],
        *,
        hotel_review_count: Optional[int],
        source_pool: str,
        target_count: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """在池内配额前统一执行评论质量分层与动态放宽。"""
        if target_count <= 0:
            return [], self._quality_stat_template()

        candidate_total = len(reviews)
        if not bool(getattr(settings, "review_quality_strict_enable", True)):
            accepted_reviews = [self._clone_review_with_reason(item, "quality_disabled") for item in reviews[:target_count]]
            stats = self._quality_stat_template()
            stats.update(
                {
                    "candidate_total": candidate_total,
                    "accepted_total": len(accepted_reviews),
                    "filtered_quality_total": max(candidate_total - len(accepted_reviews), 0),
                    "high_quality_count": len(
                        [item for item in accepted_reviews if item.get("quality_tier") in {"S", "A", "B"}]
                    ),
                    "short_comment_count": len(
                        [
                            item for item in accepted_reviews
                            if item.get("quality_tier") == "C" or bool(item.get("is_specific_short"))
                        ]
                    ),
                    "specific_short_count": len(
                        [item for item in accepted_reviews if bool(item.get("is_specific_short"))]
                    ),
                }
            )
            stats["short_comment_ratio"] = round(stats["short_comment_count"] / len(accepted_reviews), 4) if accepted_reviews else 0.0
            stats["high_quality_ratio"] = round(stats["high_quality_count"] / len(accepted_reviews), 4) if accepted_reviews else 0.0
            return accepted_reviews, stats

        short_cap = max(1, int(math.ceil(target_count * self._short_comment_ratio_cap(hotel_review_count))))
        specific_short_cap = max(
            1,
            int(math.ceil(target_count * float(getattr(settings, "review_specific_short_max_ratio", 0.15)))),
        )
        relax_trigger = float(getattr(settings, "review_quality_relax_trigger_ratio", 0.70))
        positive_min_effective_len = int(getattr(settings, "review_positive_min_effective_len", 12))
        negative_min_effective_len = int(getattr(settings, "review_negative_min_effective_len", 10))
        positive_short_fallback_min_len = int(getattr(settings, "review_positive_short_fallback_min_len", 8))
        negative_short_fallback_min_len = int(getattr(settings, "review_negative_short_fallback_min_len", 6))

        high_quality: list[dict[str, Any]] = []
        c_tier: list[dict[str, Any]] = []
        specific_short: list[dict[str, Any]] = []

        for review in reviews:
            normalized_review = self._ensure_review_quality_metadata(review, source_pool)
            quality_tier = str(normalized_review.get("quality_tier") or "D")
            is_specific_short = bool(normalized_review.get("is_specific_short"))
            is_generic_short = bool(normalized_review.get("is_generic_short"))
            effective_length = int(normalized_review.get("effective_length", 0) or 0)

            if is_generic_short:
                continue
            if quality_tier in {"S", "A", "B"}:
                min_effective_len = negative_min_effective_len if source_pool == "negative" else positive_min_effective_len
                if effective_length >= min_effective_len:
                    high_quality.append(normalized_review)
                continue
            if quality_tier == "C":
                c_tier.append(normalized_review)
                continue
            if is_specific_short:
                specific_short.append(normalized_review)

        accepted: list[dict[str, Any]] = []
        current_short_count = 0
        current_specific_short_count = 0
        quality_relaxed_used = False
        quality_fallback_used = False

        for review in high_quality:
            if len(accepted) >= target_count:
                break
            accepted.append(self._clone_review_with_reason(review, "strict_high_quality"))

        if source_pool == "negative":
            for review in specific_short:
                effective_length = int(review.get("effective_length", 0) or 0)
                if len(accepted) >= target_count:
                    break
                if current_short_count >= short_cap or current_specific_short_count >= specific_short_cap:
                    break
                if effective_length < negative_short_fallback_min_len:
                    continue
                accepted.append(self._clone_review_with_reason(review, "negative_specific_short"))
                current_short_count += 1
                current_specific_short_count += 1

        if len(accepted) < int(math.ceil(target_count * relax_trigger)):
            relaxed_before = len(accepted)
            for review in c_tier:
                if len(accepted) >= target_count or current_short_count >= short_cap:
                    break
                accepted.append(self._clone_review_with_reason(review, "tier_c_relaxed"))
                current_short_count += 1
            quality_relaxed_used = len(accepted) > relaxed_before

        if len(accepted) < target_count:
            fallback_before = len(accepted)
            for review in specific_short:
                effective_length = int(review.get("effective_length", 0) or 0)
                if len(accepted) >= target_count:
                    break
                if any(item.get("review_id") == review.get("review_id") for item in accepted):
                    continue
                if current_short_count >= short_cap or current_specific_short_count >= specific_short_cap:
                    break
                min_fallback_len = (
                    negative_short_fallback_min_len
                    if source_pool == "negative"
                    else positive_short_fallback_min_len
                )
                if effective_length < min_fallback_len:
                    continue
                accepted.append(self._clone_review_with_reason(review, "specific_short_fallback"))
                current_short_count += 1
                current_specific_short_count += 1
            quality_fallback_used = len(accepted) > fallback_before

        accepted_reviews = accepted[:target_count]
        accepted_total = len(accepted_reviews)
        high_quality_count = len(
            [item for item in accepted_reviews if str(item.get("quality_tier") or "") in {"S", "A", "B"}]
        )
        short_comment_count = len(
            [item for item in accepted_reviews if item.get("quality_tier") == "C" or bool(item.get("is_specific_short"))]
        )
        specific_short_count = len([item for item in accepted_reviews if bool(item.get("is_specific_short"))])
        stats = {
            "candidate_total": candidate_total,
            "accepted_total": accepted_total,
            "filtered_quality_total": max(candidate_total - accepted_total, 0),
            "high_quality_count": high_quality_count,
            "short_comment_count": short_comment_count,
            "specific_short_count": specific_short_count,
            "short_comment_ratio": round(short_comment_count / accepted_total, 4) if accepted_total else 0.0,
            "high_quality_ratio": round(high_quality_count / accepted_total, 4) if accepted_total else 0.0,
            "quality_relaxed_used": quality_relaxed_used,
            "quality_fallback_used": quality_fallback_used,
        }
        return accepted_reviews, stats

    def _raise_recoverable_review_interruption(
        self,
        *,
        hotel_id: str,
        action: str,
        stage: str,
        progress: str,
        message: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        payload = {
            "hotel_id": hotel_id,
            "stage": stage,
            "progress": progress,
            "current_url": str(getattr(self.anti_crawler.get_page(), "url", "") or ""),
        }
        if extra:
            payload.update(extra)
        checkpoint_path = self._save_review_checkpoint(hotel_id, payload)
        raise RecoverableInterruption(
            message,
            action=action,
            checkpoint_path=checkpoint_path,
            context=payload,
        )

    def _current_filter_text(self, page) -> str:
        """读取当前评论筛选标签文本。"""
        try:
            return str(
                page.run_js(
                    """
                    const current = document.querySelector('#hotel-review li.filter-current, #hotel-review li.current');
                    return current ? (current.innerText || '').trim() : '';
                    """
                )
                or ""
            )
        except Exception:
            return ""

    def _build_manual_positive_prompt(self, page_no: int, current_count: int, target_count: int) -> str:
        """构造终端中的人工正向采集提示。"""
        return (
            f"[positive][需要人工接管] 当前已采集 {current_count}/{target_count}。"
            f"请手动翻到目标页（建议确认仍在“好评”或目标正向列表），等待渲染完成后按 Enter 继续；"
            f"输入 i 查看当前页摘要，输入 s 让程序再次自动尝试，输入 q 结束："
        )

    def _get_manual_page_signature(self, hotel_id: str) -> tuple[str, ...]:
        """基于当前页面前几条评论生成页签名，便于识别人工未翻页。"""
        page = self.anti_crawler.get_page()
        html = str(getattr(page, "html", "") or "")
        reviews = self._extract_manual_positive_preview_rows(html, hotel_id)
        return tuple(item.get("review_id", "") for item in reviews[:5])

    def _extract_manual_positive_preview_rows(self, html: str, hotel_id: str) -> list[dict]:
        """提取当前页预览评论，避免人工模式下日志过噪。"""
        if not html or "tb-r-comment" not in html or BeautifulSoup is None:
            return []

        soup = BeautifulSoup(html, "html.parser")
        review_nodes = soup.select("li.tb-r-comment")
        previews: list[dict] = []
        local_seen: set[str] = set()
        for node in review_nodes:
            content = self._extract_content_from_bs4_node(node)
            if not content:
                continue

            user_nick = self._get_text_by_selectors_bs4(node, self.NICK_SELECTORS)
            summary = self._get_text_by_selectors_bs4(node, self.SUMMARY_SELECTORS)
            date_text = self._get_text_by_selectors_bs4(node, self.DATE_SELECTORS)
            review_date = parse_date(date_text) if date_text else None
            scores = self._parse_scores_bs4(node)
            review_id = self._generate_review_id(hotel_id, content, user_nick)
            if review_id in local_seen:
                continue
            local_seen.add(review_id)

            previews.append(
                {
                    "review_id": review_id,
                    "user_nick": user_nick,
                    "summary": summary,
                    "content": content,
                    "review_date": review_date,
                    "overall_score": scores.get("overall"),
                }
            )
        return previews

    def _get_manual_page_state(self, hotel_id: str) -> dict[str, Any]:
        """读取人工正向采集当前页状态，便于终端提示。"""
        page = self.anti_crawler.get_page()
        html = str(getattr(page, "html", "") or "")
        previews = self._extract_manual_positive_preview_rows(html, hotel_id)
        current_filter_text = self._current_filter_text(page)
        current_page_index = ""
        try:
            current_page_index = str(
                page.run_js(
                    """
                    const current = document.querySelector('#hotel-review .quickbuy-page .current, .quickbuy-page .current');
                    if (!current) return '';
                    return String(current.getAttribute('data-value') || current.innerText || '').trim();
                    """
                )
                or ""
            )
        except Exception:
            current_page_index = ""

        preview_items = []
        for item in previews[:2]:
            content = item.get("summary") or item.get("content") or ""
            preview_items.append(
                {
                    "review_id": item.get("review_id"),
                    "review_date": item.get("review_date"),
                    "overall_score": item.get("overall_score"),
                    "preview": clean_text(str(content))[:80],
                }
            )

        return {
            "signature": tuple(item.get("review_id", "") for item in previews[:5]),
            "filter_text": current_filter_text,
            "js_count": self._js_review_item_count(page),
            "page_index": current_page_index,
            "preview_items": preview_items,
        }

    def _log_manual_positive_page_state(self, hotel_id: str, page_no: int, state: dict[str, Any]) -> None:
        """输出人工正向采集当前页摘要。"""
        preview_items = state.get("preview_items") or []
        compact = [
            f"{item.get('review_date') or 'n/a'}|{item.get('overall_score') or 'n/a'}|{item.get('preview') or ''}"
            for item in preview_items
        ]
        logger.info(
            f"人工正向采集当前页状态: hotel_id={hotel_id}, page_no={page_no}, "
            f"filter={state.get('filter_text') or 'unknown'}, js_count={state.get('js_count')}, "
            f"preview={compact or ['<empty>']}"
        )

    def _build_manual_positive_status_line(
        self,
        *,
        page_no: int,
        current_state: dict[str, Any],
        raw_count: Optional[int] = None,
        kept_count: Optional[int] = None,
        total_count: Optional[int] = None,
    ) -> str:
        """构造更紧凑的单行状态栏。"""
        parts = [
            f"page={page_no}",
            f"filter={current_state.get('filter_text') or 'unknown'}",
            f"js={current_state.get('js_count') or 0}",
        ]
        if current_state.get("page_index"):
            parts.append(f"dom_page={current_state.get('page_index')}")
        if raw_count is not None:
            parts.append(f"raw={raw_count}")
        if kept_count is not None:
            parts.append(f"kept={kept_count}")
        if total_count is not None:
            parts.append(f"total={total_count}")
        return " | ".join(parts)

    @staticmethod
    def _describe_manual_page_transition(previous_state: dict[str, Any], current_state: dict[str, Any]) -> str:
        """生成人工翻页前后差异描述。"""
        prev_preview = (previous_state.get("preview_items") or [{}])[0]
        curr_preview = (current_state.get("preview_items") or [{}])[0]
        prev_desc = (
            f"page={previous_state.get('page_index') or 'n/a'}|"
            f"{prev_preview.get('review_date') or 'n/a'}|"
            f"{prev_preview.get('overall_score') or 'n/a'}|"
            f"{prev_preview.get('preview') or ''}"
        )
        curr_desc = (
            f"page={current_state.get('page_index') or 'n/a'}|"
            f"{curr_preview.get('review_date') or 'n/a'}|"
            f"{curr_preview.get('overall_score') or 'n/a'}|"
            f"{curr_preview.get('preview') or ''}"
        )
        return f"before={prev_desc} -> after={curr_desc}"

    def _log_manual_positive_page_result(
        self,
        *,
        hotel_id: str,
        page_no: int,
        raw_count: int,
        kept_count: int,
        filtered_count: int,
        total_count: int,
        current_state: dict[str, Any],
    ) -> None:
        """输出人工正向采集结果汇总。"""
        logger.info(
            f"人工正向采集结果: hotel_id={hotel_id}, page_no={page_no}, "
            f"filter={current_state.get('filter_text') or 'unknown'}, js_count={current_state.get('js_count')}, "
            f"raw={raw_count}, kept={kept_count}, filtered={filtered_count}, total={total_count}"
        )
        logger.info(
            f"人工正向采集状态栏: {self._build_manual_positive_status_line(page_no=page_no, current_state=current_state, raw_count=raw_count, kept_count=kept_count, total_count=total_count)}"
        )

    def _save_manual_positive_snapshot(self, hotel_id: str, page_no: int) -> Optional[str]:
        """为人工正向采集落地当前页 HTML 快照，便于追溯。"""
        page = self.anti_crawler.get_page()
        html = str(getattr(page, "html", "") or "")
        if not html:
            return None

        snapshot_dir = settings.log_path / "positive_manual"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = snapshot_dir / f"{ts}_{hotel_id}_page_{page_no}.html"
        path.write_text(html, encoding="utf-8", errors="ignore")
        return str(path)

    def _ensure_positive_manual_entry(self, hotel_id: str) -> bool:
        """在正向采集开始前尽量切到“好评”状态，失败时允许降级到“全部+高分过滤”。"""
        progress = "0/0"
        try:
            filter_applied = self.filter_reviews(
                self.FILTER_GOOD,
                hotel_id=hotel_id,
                source_pool="positive",
                progress=progress,
            )
            page = self.anti_crawler.get_page()
            filter_text = self._current_filter_text(page)
            if filter_applied and any(keyword in filter_text for keyword in ("好评",)):
                logger.info(
                    f"正向采集入口已切到好评页: hotel_id={hotel_id}, filter={filter_text or 'unknown'}"
                )
                return True

            logger.warning(
                f"正向采集未稳定停留在好评页，将降级为“全部页 + 高分过滤”模式: "
                f"hotel_id={hotel_id}, filter={filter_text or 'unknown'}, filter_applied={filter_applied}"
            )
            return False
        except (CaptchaException, CaptchaCooldownException):
            raise
        except Exception as exc:
            logger.warning(f"正向人工采集预切换好评失败: hotel_id={hotel_id}, error={exc}")
            return False

    def _find_positive_next_button(self, page):
        """查找正向采集流程中的下一页按钮。"""
        selectors = (
            '.pi-pagination-next:not(.pi-pagination-disabled)',
            '.pi-pagination-next',
            'a.next-page.J_quickbuyPage',
            'a.next-page',
            '.J_quickbuyPage.next-page',
            '.pagination-next:not(.disabled)',
            'a.next:not(.disabled)',
        )
        for selector in selectors:
            try:
                btn = page.ele(selector, timeout=1)
            except Exception:
                btn = None
            if btn:
                return btn, selector
        try:
            found = page.run_js(
                """
                const candidates = Array.from(document.querySelectorAll('a, button, span, div'));
                const target = candidates.find((el) => {
                    const text = (el.innerText || el.textContent || '').trim();
                    const className = String(el.className || '');
                    const disabled = el.classList?.contains('disabled') || /disabled/.test(className);
                    if (disabled) return false;
                    return text.includes('下一页') || /next-page|page-next/.test(className);
                });
                if (!target) return '';
                if (target.classList?.contains('next-page')) return 'a.next-page';
                if ((target.innerText || target.textContent || '').trim().includes('下一页')) return '__text_next__';
                return '';
                """
            )
            found_text = str(found or "").strip()
            if found_text == 'a.next-page':
                try:
                    btn = page.ele('a.next-page', timeout=1)
                except Exception:
                    btn = None
                if btn:
                    return btn, 'a.next-page'
            if found_text == '__text_next__':
                return None, '__text_next__'
        except Exception:
            pass
        return None, None

    def _scroll_positive_pagination_into_view(
        self,
        page,
        hotel_id: str,
        page_no: int,
        progress: str,
    ) -> None:
        """尽量滚到评论分页区域，避免按钮因未进入视区而无法命中。"""
        try:
            page.run_js(
                """
                const candidates = [
                  'a.next-page.J_quickbuyPage',
                  'a.next-page',
                  '.J_quickbuyPage.next-page',
                  '.pi-pagination-next',
                  '.pagination-next',
                ];
                for (const selector of candidates) {
                  const el = document.querySelector(selector);
                  if (el && typeof el.scrollIntoView === 'function') {
                    el.scrollIntoView({ block: 'end', inline: 'nearest' });
                    window.scrollBy(0, 240);
                    return selector;
                  }
                }
                const review = document.querySelector('#hotel-review');
                if (review && typeof review.scrollIntoView === 'function') {
                  review.scrollIntoView({ block: 'end', inline: 'nearest' });
                  window.scrollBy(0, 900);
                  return '#hotel-review';
                }
                window.scrollTo(0, document.body.scrollHeight);
                return 'body-bottom';
                """
            )
        except Exception as exc:
            logger.debug(
                f"正向自动翻页滚动分页区域失败: hotel_id={hotel_id}, page_no={page_no}, progress={progress}, error={exc}"
            )

    def _click_positive_next_by_text(self, page) -> bool:
        """按“下一页”文本或 next-page 类名匹配并点击。"""
        try:
            return bool(
                page.run_js(
                    """
                    const scopes = [
                      document.querySelector('#hotel-review .quickbuy-page-box'),
                      document.querySelector('.quickbuy-page-box'),
                      document.querySelector('#hotel-review'),
                      document,
                    ].filter(Boolean);
                    for (const scope of scopes) {
                      const candidates = Array.from(scope.querySelectorAll('a, button, span, div'));
                      const target = candidates.find((el) => {
                        const text = (el.innerText || el.textContent || '').trim();
                        const className = String(el.className || '');
                        const disabled = el.classList?.contains('disabled') || /disabled|first-page/.test(className);
                        if (disabled) return false;
                        return text === '下一页' || text.includes('下一页') || /next-page|page-next/.test(className);
                      });
                      if (!target) continue;
                      if (typeof target.scrollIntoView === 'function') {
                        target.scrollIntoView({ block: 'end', inline: 'nearest' });
                        window.scrollBy(0, 160);
                      }
                      const events = ['mouseover', 'mousedown', 'mouseup', 'click'];
                      for (const type of events) {
                        target.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true }));
                      }
                      if (typeof target.click === 'function') {
                        target.click();
                      }
                      return true;
                    }
                    return false;
                    """
                )
            )
        except Exception:
            return False

    def _auto_advance_positive_page(
        self,
        hotel_id: str,
        page_no: int,
        progress: str,
        current_state: dict[str, Any],
    ) -> tuple[bool, dict[str, Any], str]:
        """按真人节奏自动滚到底并点击下一页，仅在失败时再交给人工。"""
        page = self.anti_crawler.get_page()
        self._ensure_review_page_ready(hotel_id, "positive", "positive_auto_page_advance_precheck", progress)
        self.anti_crawler.ensure_page_foreground("positive_auto_page_advance")
        self.anti_crawler.simulate_human_presence("positive_auto_page_advance", include_scroll=False)

        review_section = page.ele('#hotel-review', timeout=2)
        if review_section:
            try:
                review_section.scroll.to_see()
            except Exception:
                pass
            self.anti_crawler.random_delay(0.8, 1.8)

        self.anti_crawler.scroll_to_bottom(step=700, max_scrolls=24)
        self._scroll_positive_pagination_into_view(page, hotel_id, page_no, progress)
        self.anti_crawler.random_delay(1.5, 3.0)

        next_btn, selector = self._find_positive_next_button(page)
        if not next_btn and selector == '__text_next__':
            clicked = self._click_positive_next_by_text(page)
            if not clicked:
                logger.warning(
                    f"正向自动翻页文本按钮点击失败: hotel_id={hotel_id}, page_no={page_no}, progress={progress}"
                )
                return False, current_state, "next_button_click_failed"
        elif not next_btn or not selector:
            logger.warning(
                f"正向自动翻页未找到下一页按钮: hotel_id={hotel_id}, page_no={page_no}, progress={progress}, "
                "hint=检查分页按钮是否未进入视区或页面分页DOM已改版"
            )
            return False, current_state, "next_button_missing"
        else:
            clicked = self._click_with_fallback(
                page,
                next_btn,
                selector,
                action="positive_auto_next_click",
                hotel_id=hotel_id,
                source_pool="positive",
                progress=progress,
            )
            if not clicked:
                logger.warning(
                    f"正向自动翻页点击失败: hotel_id={hotel_id}, page_no={page_no}, progress={progress}, selector={selector}"
                )
                return False, current_state, "next_button_click_failed"

        self._ensure_review_page_ready(hotel_id, "positive", "positive_auto_next_click", progress)
        self._wait_review_list_ready(hotel_id, "positive", "positive_auto_next_wait", progress)
        wait_min = settings.review_positive_auto_page_delay_min_seconds
        wait_max = settings.review_positive_auto_page_delay_max_seconds
        self.anti_crawler.random_delay(wait_min, wait_max)
        self._ensure_review_page_ready(hotel_id, "positive", "positive_auto_page_advance_postwait", progress)
        next_state = self._get_manual_page_state(hotel_id)
        page_changed = False
        if next_state.get("page_index") and current_state.get("page_index"):
            page_changed = str(next_state.get("page_index")) != str(current_state.get("page_index"))
        if not page_changed and next_state.get("signature") and next_state.get("signature") != current_state.get("signature"):
            page_changed = True

        if not page_changed:
            logger.warning(
                f"正向自动翻页后页面未变化: hotel_id={hotel_id}, page_no={page_no}, progress={progress}, "
                f"signature={tuple(next_state.get('signature') or tuple())[:3]}"
            )
            return False, next_state, "page_unchanged"

        logger.info(
            f"正向自动翻页成功: hotel_id={hotel_id}, from_page={page_no}, "
            f"wait_range=({wait_min:.1f},{wait_max:.1f}), "
            f"{self._describe_manual_page_transition(current_state, next_state)}"
        )

        return True, next_state, "ok"

    def _prepare_manual_positive_takeover(
        self,
        hotel_id: str,
        page_no: int,
        current_count: int,
        target_count: int,
    ) -> None:
        """人工接管前先把页面滚到底部并预留点击“下一页”的时间。"""
        page = self.anti_crawler.get_page()
        progress = f"{current_count}/{target_count}"
        self.anti_crawler.ensure_page_foreground("positive_manual_takeover")
        try:
            self.anti_crawler.scroll_to_bottom(step=700, max_scrolls=24)
        except Exception as exc:
            logger.debug(f"人工正向接管预滚动失败: hotel_id={hotel_id}, page_no={page_no}, error={exc}")
        try:
            self._scroll_positive_pagination_into_view(page, hotel_id, page_no, progress)
        except Exception as exc:
            logger.debug(f"人工正向接管预定位分页区失败: hotel_id={hotel_id}, page_no={page_no}, error={exc}")

        prewait_seconds = float(getattr(settings, "review_positive_manual_prewait_seconds", 10.0))
        if prewait_seconds > 0:
            logger.info(
                f"人工正向接管预等待: hotel_id={hotel_id}, page_no={page_no}, "
                f"progress={progress}, seconds={prewait_seconds:.1f}"
            )
            time.sleep(prewait_seconds)

    def _input_with_timeout_reminder(
        self,
        *,
        prompt: str,
        reminder_seconds: float,
        reminder_message: str,
    ) -> str:
        """带超时提醒的输入等待；超时只提醒，不自动跳过。"""
        if reminder_seconds <= 0:
            return input(prompt)

        result: dict[str, Any] = {}

        def _reader() -> None:
            try:
                result["value"] = input(prompt)
            except BaseException as exc:  # pragma: no cover - 防御性分支
                result["error"] = exc

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()

        while reader.is_alive():
            reader.join(timeout=reminder_seconds)
            if reader.is_alive():
                self._manual_timeout_reminder_count += 1
                logger.warning(reminder_message)

        if "error" in result:
            raise result["error"]
        return str(result.get("value") or "")

    def _request_manual_positive_takeover(
        self,
        hotel_id: str,
        page_no: int,
        current_count: int,
        target_count: int,
        current_state: dict[str, Any],
    ) -> Optional[str]:
        """自动翻页失败时，请求人工介入。返回 quit/retry/continue。"""
        self._manual_takeover_count += 1
        self._prepare_manual_positive_takeover(hotel_id, page_no, current_count, target_count)
        reminder_seconds = float(getattr(settings, "review_positive_manual_reminder_seconds", 90.0))
        while True:
            logger.info(
                f"人工正向采集状态栏: {self._build_manual_positive_status_line(page_no=page_no, current_state=current_state, total_count=current_count)}"
            )
            reminder_message = (
                f"[positive][等待人工翻页] hotel_id={hotel_id}, page_no={page_no}, "
                f"progress={current_count}/{target_count}，请确认是否已点击“下一页”或是否仍停留在旧页。"
            )
            user_input = self._input_with_timeout_reminder(
                prompt=self._build_manual_positive_prompt(page_no, current_count, target_count),
                reminder_seconds=reminder_seconds,
                reminder_message=reminder_message,
            ).strip().lower()
            if user_input in {"i", "info", "?", "help"}:
                current_state = self._get_manual_page_state(hotel_id)
                self._log_manual_positive_page_state(hotel_id, page_no, current_state)
                continue
            if user_input in {"q", "quit", "exit"}:
                return "quit"
            if user_input in {"s", "skip"}:
                return "retry"
            return "continue"

    def _generate_review_id(self, hotel_id: str, content: str, user_nick: Optional[str] = None) -> str:
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

    def get_hotel_review_url(self, hotel_id: str) -> str:
        """获取点评页 URL（作为评论区兜底入口）。"""
        return self.REVIEW_URL_TEMPLATE.format(hotel_id=hotel_id)

    def _extract_review_tab_href(self, page) -> Optional[str]:
        """从当前页面提取“住客评价/点评”链接地址。"""
        script = """
        const anchors = [...document.querySelectorAll('a[href]')];
        const target = anchors.find((a) => {
            const text = (a.innerText || a.textContent || '').trim();
            if (!/住客评价|点评|评论/.test(text)) return false;
            const href = String(a.getAttribute('href') || '');
            return /dianping|hotel-review|review/i.test(href);
        });
        return target ? target.href : '';
        """
        try:
            href = page.run_js(script)
        except Exception:
            return None
        href_text = str(href or "").strip()
        return href_text or None

    def _force_open_review_detail_page(
        self,
        hotel_id: str,
        source_pool: Optional[str],
        progress: str,
        reason: str,
    ) -> bool:
        """当评论列表持续为空时，强制打开点评页后再尝试解析。"""
        if hotel_id in self._review_detail_fallback_attempted:
            return False

        self._review_detail_fallback_attempted.add(hotel_id)
        page = self.anti_crawler.get_page()
        current_url = str(getattr(page, "url", "") or "")
        extracted_href = self._extract_review_tab_href(page)

        candidate_urls: list[str] = []
        if extracted_href:
            candidate_urls.append(extracted_href)
        candidate_urls.append(self.get_hotel_review_url(hotel_id))

        normalized_candidates: list[str] = []
        for raw in candidate_urls:
            if not raw:
                continue
            url = raw.strip()
            if url.startswith("//"):
                url = f"https:{url}"
            elif url.startswith("/"):
                url = urljoin(current_url or "https://www.fliggy.com/", url)
            # 统一到 www 主域，减少 301/302 链路和风控噪声。
            url = re.sub(r"^https://fliggy\.com/", "https://www.fliggy.com/", url)
            if url and url not in normalized_candidates:
                normalized_candidates.append(url)

        for target_url in normalized_candidates:
            if target_url == current_url:
                continue

            logger.info(
                f"评论兜底导航: reason={reason}, hotel_id={hotel_id}, source_pool={source_pool}, "
                f"progress={progress}, target={target_url}"
            )
            try:
                self.anti_crawler.suppress_review_bootstrap_requests(
                    True,
                    reason="fallback_review_navigation",
                )
                if not self.anti_crawler.navigate_to(target_url):
                    continue
                self.anti_crawler.random_delay(1, 2)
                self._ensure_review_page_ready(
                    hotel_id,
                    source_pool,
                    action="fallback_review_detail_navigation",
                    progress=progress,
                )
                self._trigger_review_lazy_load(
                    hotel_id,
                    source_pool,
                    progress,
                    reason="fallback_review_detail_navigation",
                )
                self.anti_crawler.suppress_review_bootstrap_requests(
                    False,
                    reason="fallback_review_navigation_release",
                )
                self._reactivate_review_runtime(
                    hotel_id,
                    source_pool,
                    progress,
                    reason="fallback_review_detail_navigation",
                )
            except (CaptchaException, CaptchaCooldownException):
                raise
            except Exception as exc:
                logger.warning(
                    f"评论兜底导航失败: hotel_id={hotel_id}, target={target_url}, error={exc}"
                )
                continue
            finally:
                if self.anti_crawler._blocked_url_patterns:
                    self.anti_crawler.suppress_review_bootstrap_requests(
                        False,
                        reason="fallback_review_navigation_cleanup",
                    )

            refreshed_page = self.anti_crawler.get_page()
            if self._js_review_item_count(refreshed_page) > 0:
                logger.info(
                    f"评论兜底导航成功(JS): hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}"
                )
                return True

            review_elements = self._get_elements_by_selectors(refreshed_page, self.REVIEW_LIST_SELECTORS)
            if review_elements:
                logger.info(
                    f"评论兜底导航成功: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"progress={progress}, count={len(review_elements)}"
                )
                return True

        logger.warning(
            f"评论兜底导航后仍为空: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}"
        )
        return False

    def _log_captcha_failure(
        self,
        action: str,
        hotel_id: str,
        source_pool: Optional[str],
        progress: str,
        exc: Exception,
    ) -> None:
        """记录验证码失败上下文。"""
        self._update_review_context(
            hotel_id=hotel_id,
            source_pool=source_pool or "navigation",
            progress=progress,
            current_url=str(getattr(self.anti_crawler.get_page(), "url", "") or ""),
        )
        logger.error(
            f"[评论反爬] {action} 遇到验证码/风控中断: {exc} | {self._format_review_context()}"
        )

    def _ensure_review_page_ready(
        self,
        hotel_id: str,
        source_pool: Optional[str],
        action: str,
        progress: str,
    ) -> None:
        """在评论交互和解析前确认页面未被验证码或失效验证页中断。"""
        page = self.anti_crawler.get_page()

        try:
            html = page.html or ""
        except Exception:
            html = ""

        if any(keyword in html for keyword in self.VERIFICATION_EXPIRED_KEYWORDS):
            exc = CaptchaTimeoutException(
                timeout_seconds=settings.captcha_solve_timeout_seconds,
                elapsed_seconds=0,
                retry_count=1,
                max_retries=settings.captcha_max_retries,
            )
            self._log_captcha_failure(action, hotel_id, source_pool, progress, exc)
            raise exc

        if not self.anti_crawler.check_captcha():
            return

        try:
            self.anti_crawler.handle_captcha()
        except (CaptchaException, CaptchaCooldownException) as exc:
            self._log_captcha_failure(action, hotel_id, source_pool, progress, exc)
            raise

        if self.anti_crawler.check_captcha():
            exc = CaptchaTimeoutException(
                timeout_seconds=settings.captcha_solve_timeout_seconds,
                elapsed_seconds=0,
                retry_count=1,
                max_retries=settings.captcha_max_retries,
            )
            self._log_captcha_failure(action, hotel_id, source_pool, progress, exc)
            raise exc

    def _get_text_by_selectors(self, elem, selectors: Sequence[str]) -> Optional[str]:
        """按选择器优先级提取文本。"""
        for selector in selectors:
            try:
                node = elem.ele(selector, timeout=1)
            except Exception:
                continue
            if not node:
                continue
            text = clean_text(node.text)
            if text:
                return text
        return None

    def _get_elements_by_selectors(self, page, selectors: Sequence[str]):
        """按选择器优先级提取元素集合。"""
        for selector in selectors:
            try:
                elements = page.eles(selector)
            except Exception:
                continue
            if elements:
                return elements
        return []

    @staticmethod
    def _js_selector_count(page, selector: str) -> int:
        """用浏览器原生 querySelectorAll 统计节点数量。"""
        selector_json = json.dumps(selector, ensure_ascii=False)
        script = f"""
        const selector = {selector_json};
        try {{
          return document.querySelectorAll(selector).length;
        }} catch (e) {{
          return 0;
        }}
        """
        try:
            count = page.run_js(script)
        except Exception:
            return 0
        try:
            return int(count or 0)
        except Exception:
            return 0

    def _js_review_item_count(self, page) -> int:
        """统计评论项数量（JS主通道）。"""
        try:
            result = page.run_js(
                """
                const selectors = [
                  'li.tb-r-comment',
                  '.review-content.review-bd.no-border li.tb-r-comment',
                  'div.review-content.review-bd.no-border .review-list li.tb-r-comment',
                ];
                let maxCount = 0;
                for (const selector of selectors) {
                  const count = document.querySelectorAll(selector).length;
                  if (count > maxCount) maxCount = count;
                }
                return maxCount;
                """
            )
        except Exception:
            return 0
        try:
            return int(result or 0)
        except Exception:
            return 0

    def _click_with_fallback(
        self,
        page,
        element,
        selector: Optional[str],
        *,
        action: str,
        hotel_id: Optional[str],
        source_pool: Optional[str],
        progress: str,
    ) -> bool:
        """带回退策略的点击：原生 -> JS点击参数 -> 页面JS点击。"""
        if not element:
            return False

        for click_mode, kwargs in (("native", {}), ("by_js", {"by_js": True})):
            try:
                element.click(**kwargs)
                logger.debug(
                    f"点击成功: action={action}, mode={click_mode}, selector={selector}, "
                    f"hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}"
                )
                return True
            except Exception as exc:
                logger.debug(
                    f"点击失败: action={action}, mode={click_mode}, selector={selector}, "
                    f"hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, error={exc}"
                )

        if not selector:
            return False

        selector_json = json.dumps(selector, ensure_ascii=False)
        script = f"""
        const selector = {selector_json};
        const target = document.querySelector(selector);
        if (!target) return false;
        if (typeof target.scrollIntoView === 'function') {{
            target.scrollIntoView({{ block: 'center', inline: 'center' }});
        }}
        if (typeof target.click === 'function') {{
            target.click();
            return true;
        }}
        const events = ['mouseover', 'mousedown', 'mouseup', 'click'];
        for (const type of events) {{
            target.dispatchEvent(new MouseEvent(type, {{ bubbles: true, cancelable: true }}));
        }}
        return true;
        """

        try:
            clicked = bool(page.run_js(script))
            if clicked:
                logger.debug(
                    f"点击成功: action={action}, mode=run_js, selector={selector}, "
                    f"hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}"
                )
            return clicked
        except Exception as exc:
            logger.warning(
                f"点击失败(全部回退耗尽): action={action}, selector={selector}, "
                f"hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, error={exc}"
            )
            return False

    def _is_checkbox_checked(self, page, selector: str) -> Optional[bool]:
        """读取checkbox状态，优先DOM checked属性。"""
        selector_json = json.dumps(selector, ensure_ascii=False)
        script = f"""
        const selector = {selector_json};
        const el = document.querySelector(selector);
        if (!el) return null;
        return !!(el.checked || el.getAttribute('checked') !== null);
        """
        try:
            result = page.run_js(script)
            if isinstance(result, bool):
                return result
            return None
        except Exception:
            return None

    def _wait_review_list_ready(
        self,
        hotel_id: Optional[str],
        source_pool: Optional[str],
        action: str,
        progress: str,
    ) -> None:
        """等待评论列表可解析，降低固定延迟导致的空抓风险。"""
        if hotel_id:
            self._ensure_review_page_ready(hotel_id, source_pool, action, progress)

        page = self.anti_crawler.get_page()
        if self._js_review_item_count(page) > 0:
            return

        for selector in self.REVIEW_LIST_SELECTORS:
            try:
                if page.eles(selector, timeout=3):
                    return
            except Exception:
                continue

        logger.info(f"第一次选择器检查未命中，准备触发懒加载: hotel_id={hotel_id}, action={action}")
        self._reactivate_review_runtime(hotel_id, source_pool, progress, reason=action)

        if self._js_review_item_count(page) > 0:
            return

        for selector in self.REVIEW_LIST_SELECTORS:
            try:
                if page.eles(selector, timeout=3):
                    return
            except Exception:
                continue

        # 注意：筛选点击后的短等待阶段不能执行“强制重开点评页”，
        # 否则会把刚刚点击的筛选状态重置为“全部”，导致筛选验证失败。
        allow_reopen = hotel_id is not None and action not in {"filter_reviews_wait"}
        if allow_reopen and hotel_id is not None:
            reopened = self._force_open_review_detail_page(
                hotel_id=hotel_id,
                source_pool=source_pool,
                progress=progress,
                reason=action,
            )
            if reopened:
                refreshed_page = self.anti_crawler.get_page()
                if self._js_review_item_count(refreshed_page) > 0:
                    return
                for selector in self.REVIEW_LIST_SELECTORS:
                    try:
                        if refreshed_page.eles(selector, timeout=3):
                            return
                    except Exception:
                        continue

        # 如果短等待仍未命中，走一次兜底随机延迟
        self.anti_crawler.random_delay(1.2, 2.0)

    def _trigger_review_lazy_load(
        self,
        hotel_id: Optional[str],
        source_pool: Optional[str],
        progress: str,
        reason: str,
    ) -> None:
        """触发评论区懒加载和Tab激活。"""
        page = self.anti_crawler.get_page()
        self.anti_crawler.ensure_page_foreground(f"review_lazy_load:{reason}")
        self.anti_crawler.simulate_human_presence(reason=f"review_lazy_load:{reason}", include_scroll=False)

        try:
            page.run_js(
                """
                const preferred = [...document.querySelectorAll('a[href]')]
                  .find((el) => {
                    const text = (el.innerText || '').trim();
                    const href = String(el.getAttribute('href') || '');
                    return /住客评价|点评|评论/.test(text) && /dianping|hotel-review|review/i.test(href);
                  });
                if (preferred) {
                    preferred.click();
                } else {
                    const reviewTab = [...document.querySelectorAll('.pi-tab-item, .pi-tab-list li, a, span')]
                      .find((el) => /住客评价|点评|评论/.test((el.innerText || '').trim()));
                    if (reviewTab) {
                        reviewTab.click();
                    }
                }
                const reviewSection = document.querySelector('#hotel-review');
                if (reviewSection && typeof reviewSection.scrollIntoView === 'function') {
                    reviewSection.scrollIntoView({ block: 'center', inline: 'nearest' });
                }
                """
            )
        except Exception as exc:
            logger.debug(f"触发评论懒加载脚本失败: {exc}")

        try:
            section = page.ele('#hotel-review', timeout=2)
            if section:
                section.scroll.to_see()
        except Exception:
            logger.debug("评论区定位失败，继续执行页面滚动")

        try:
            self.anti_crawler.scroll_page("down", 450)
            self.anti_crawler.scroll_page("down", 450)
            self.anti_crawler.scroll_page("up", 260)
        except Exception:
            logger.debug("触发评论懒加载时页面滚动失败")

        logger.info(
            f"已触发评论懒加载: reason={reason}, hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}"
        )

    def _capture_review_packets(
        self,
        hotel_id: str,
        source_pool: str,
        progress: str,
        *,
        max_packets: int = 8,
        wait_per_packet: float = 1.2,
    ) -> list[dict[str, Any]]:
        """监听并捕获评论相关网络包。"""
        page = self.anti_crawler.get_page()
        packets: list[dict[str, Any]] = []
        attempt_limit = max(1, settings.review_request_capture_attempts)

        for attempt in range(attempt_limit):
            targeted_only = attempt == 0
            attempt_reason = f"network_capture_{attempt + 1}"
            try:
                try:
                    page.listen.stop()
                except Exception:
                    logger.debug("启动前停止旧网络监听失败，忽略")

                if targeted_only:
                    page.listen.start(targets=self._network_capture_targets, res_type=("XHR", "Fetch"))
                else:
                    page.listen.start(res_type=("XHR", "Fetch"))

                if attempt > 0:
                    self.anti_crawler.warm_session(reason=attempt_reason, force=True)
                    self.anti_crawler.post_captcha_stabilize(reason=attempt_reason)

                self._reactivate_review_runtime(hotel_id, source_pool, progress, reason=attempt_reason)

                for _ in range(max_packets):
                    try:
                        packet = page.listen.wait(timeout=wait_per_packet, fit_count=False)
                    except Exception:
                        break

                    if not packet:
                        continue

                    response = getattr(packet, "response", None)
                    body = getattr(response, "body", None) if response else None
                    packets.append(
                        {
                            "url": getattr(packet, "url", ""),
                            "method": getattr(packet, "method", ""),
                            "status": getattr(response, "status", None) if response else None,
                            "body": body,
                        }
                    )

                if packets:
                    break

                if attempt == 0:
                    self._force_open_review_detail_page(
                        hotel_id=hotel_id,
                        source_pool=source_pool,
                        progress=progress,
                        reason="network_capture_empty",
                    )
            except (CaptchaException, CaptchaCooldownException):
                raise
            except Exception as exc:
                logger.debug(
                    f"评论网络包监听异常: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
                    f"attempt={attempt + 1}, error={exc}"
                )
            finally:
                try:
                    page.listen.stop()
                except Exception:
                    logger.debug("停止评论网络监听失败，忽略")

        packet_preview = [packet.get("url", "") for packet in packets[:3]]
        logger.info(
            f"评论网络包捕获完成: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
            f"packet_count={len(packets)}, preview={packet_preview}"
        )
        return packets

    def _request_review_api_page_via_jsonp(
        self,
        *,
        hotel_id: str,
        filter_type: int,
        page_no: int,
        seller_id: str = "",
    ) -> tuple[Any, str]:
        """直接用页面同源 JSONP 方式拉取评论分页数据。"""
        page = self.anti_crawler.get_page()
        request_id = f"review_jsonp_{int(time.time() * 1000)}_{page_no}_{filter_type}"
        payload = {
            "shid": str(hotel_id),
            "page": int(page_no),
            "showContent": 0,
            "sort": 1,
            "rateScore": int(filter_type),
            "t": int(time.time() * 1000),
            "sn": page_no,
        }
        if seller_id:
            payload["sellerId"] = str(seller_id)

        payload_json = json.dumps(payload, ensure_ascii=False)
        request_id_json = json.dumps(request_id, ensure_ascii=False)

        page.run_js(
            f"""
            const requestId = {request_id_json};
            const payload = {payload_json};
            const callbackName = '__codex_jsonp_' + requestId;
            const scriptId = '__codex_jsonp_script_' + requestId;
            const params = new URLSearchParams();
            Object.entries(payload).forEach(([key, value]) => {{
              if (value === undefined || value === null || String(value) === '') return;
              params.set(key, String(value));
            }});
            params.set('callback', callbackName);
            window.__codex_jsonp_results = window.__codex_jsonp_results || {{}};
            window.__codex_jsonp_results[requestId] = {{ done: false, data: null, error: null, url: '' }};
            window[callbackName] = function(resp) {{
              window.__codex_jsonp_results[requestId] = {{
                done: true,
                data: resp,
                error: null,
                url: 'https://hotel.fliggy.com/ajax/getHotelRates.htm?' + params.toString(),
              }};
              try {{ delete window[callbackName]; }} catch (e) {{}}
            }};
            const oldScript = document.getElementById(scriptId);
            if (oldScript) oldScript.remove();
            const script = document.createElement('script');
            script.id = scriptId;
            script.src = 'https://hotel.fliggy.com/ajax/getHotelRates.htm?' + params.toString();
            script.onerror = function() {{
              window.__codex_jsonp_results[requestId] = {{
                done: true,
                data: null,
                error: 'script_error',
                url: script.src,
              }};
              try {{ delete window[callbackName]; }} catch (e) {{}}
            }};
            document.head.appendChild(script);
            return script.src;
            """
        )

        result_obj: Any = None
        request_url = ""
        for _ in range(24):
            time.sleep(0.5)
            raw = page.run_js(
                f"""
                const result = (window.__codex_jsonp_results || {{}})[{request_id_json}] || null;
                return result ? JSON.stringify(result) : '';
                """
            )
            raw_text = str(raw or "").strip()
            if not raw_text:
                continue
            try:
                parsed = json.loads(raw_text)
            except Exception:
                continue
            request_url = str(parsed.get("url") or "")
            if not parsed.get("done"):
                continue
            if parsed.get("error"):
                return None, request_url
            result_obj = parsed.get("data")
            return result_obj, request_url

        return None, request_url

    def _resolve_review_api_params(self, hotel_id: str) -> dict[str, str]:
        """从页面运行时提取评论 API 参数（hid/sellerId）。"""
        page = self.anti_crawler.get_page()
        try:
            params = page.run_js(
                """
                const result = { hid: '', sellerId: '' };
                try {
                  const data = window._hotel_data || window.g_hotel_data || {};
                  const hid = data?.detail?.hotel?.id || data?.hotel?.id || '';
                  if (hid) result.hid = String(hid);
                } catch (e) {}

                const scriptText = [...document.querySelectorAll('script')]
                  .map((s) => s.textContent || '')
                  .join('\\n');

                if (!result.hid) {
                  const hidMatch =
                    scriptText.match(/hotelParams\\s*=\\s*\\{[^}]*\\bhid\\s*:\\s*([0-9]+)/i) ||
                    scriptText.match(/\\bhid\\s*[:=]\\s*['\\"]?([0-9]{3,})['\\"]?/i);
                  if (hidMatch) result.hid = String(hidMatch[1] || '');
                }

                const sellerMatch = scriptText.match(/sellerId\\s*:\\s*['\\"]([^'\\"]*)['\\"]/i);
                if (sellerMatch) result.sellerId = String(sellerMatch[1] || '');
                return result;
                """
            )
        except Exception:
            params = {}

        params_dict = params if isinstance(params, dict) else {}
        hid = str(params_dict.get("hid") or "").strip()
        seller_id = str(params_dict.get("sellerId") or "").strip()

        hotel_id_digits = re.sub(r"\D+", "", str(hotel_id))
        if not re.fullmatch(r"\d{3,}", hid or ""):
            hid = hotel_id_digits
        if not re.fullmatch(r"\d{0,20}", seller_id or ""):
            seller_id = ""

        if not hid:
            hid = str(hotel_id).strip()
        return {"hid": hid, "sellerId": seller_id}

    def _request_review_api_page(
        self,
        *,
        hotel_id: str,
        hid: str,
        seller_id: str,
        rate_score: int,
        page_no: int,
        page_size: int = 20,
    ) -> tuple[str, int, str]:
        """在浏览器上下文内请求评论 API 单页。"""
        page = self.anti_crawler.get_page()
        payload = {
            "hotelId": str(hotel_id),
            "hid": str(hid),
            "sellerId": str(seller_id or ""),
            "rateScore": int(rate_score),
            "pageNo": int(page_no),
            "pageSize": int(page_size),
        }
        payload_json = json.dumps(payload, ensure_ascii=False)
        try:
            result = page.run_js(
                f"""
                const cfg = {payload_json};
                const base = 'https://hotel.fliggy.com/ajax/getReviews.do';
                const templates = [
                  {{ hid: cfg.hid, page: cfg.pageNo, pageSize: cfg.pageSize, rateScore: cfg.rateScore, showContent: 1 }},
                  {{ hid: cfg.hid, sellerId: cfg.sellerId, page: cfg.pageNo, pageSize: cfg.pageSize, rateScore: cfg.rateScore, showContent: 1 }},
                  {{ hid: cfg.hid, sellerId: cfg.sellerId, page: cfg.pageNo, pageSize: cfg.pageSize, rateScore: cfg.rateScore, showContent: 0 }},
                  {{ shid: cfg.hotelId, page: cfg.pageNo, pageSize: cfg.pageSize, rateScore: cfg.rateScore, showContent: 1 }},
                ];
                function toQuery(obj) {{
                  const usp = new URLSearchParams();
                  for (const [k, v] of Object.entries(obj)) {{
                    if (v === undefined || v === null || String(v) === '') continue;
                    usp.set(k, String(v));
                  }}
                  return usp.toString();
                }}
                for (const params of templates) {{
                  const url = `${{base}}?${{toQuery(params)}}`;
                  const xhr = new XMLHttpRequest();
                  try {{
                    xhr.open('GET', url, false);
                    xhr.withCredentials = true;
                    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
                    xhr.send();
                    if (xhr.status >= 200 && xhr.status < 400) {{
                      return {{ body: xhr.responseText || '', status: xhr.status, url }};
                    }}
                  }} catch (e) {{}}
                }}
                return {{ body: '', status: 0, url: '' }};
                """
            )
        except Exception:
            result = {}

        result_dict = result if isinstance(result, dict) else {}
        body = str(result_dict.get("body") or "")
        status_raw = result_dict.get("status") or 0
        try:
            status = int(status_raw)
        except Exception:
            status = 0
        url = str(result_dict.get("url") or "")
        return body, status, url

    def _crawl_reviews_via_api_pagination(
        self,
        *,
        hotel_id: str,
        source_pool: str,
        filter_type: int,
        remaining_quota: int,
        progress: str,
        start_page: int = 1,
    ) -> list[dict]:
        """当前端分页不可用时，直接走评论 API 分页回退。"""
        if remaining_quota <= 0:
            return []

        params = self._resolve_review_api_params(hotel_id)
        hid = params["hid"]
        seller_id = params["sellerId"]
        rate_score = int(filter_type)
        page_size = min(15, max(10, remaining_quota))
        max_pages = max(1, min(60, (remaining_quota + page_size - 1) // page_size + 8))

        logger.info(
            f"评论API分页回退开始: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
            f"hid={hid}, sellerId={'set' if seller_id else 'empty'}, rateScore={rate_score}, "
            f"page_size={page_size}, max_pages={max_pages}"
        )

        if source_pool == "negative" and rate_score in self._jsonp_punished_scores:
            logger.info(
                f"评论API分页已跳过: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
                f"rateScore={rate_score}, reason=punish_cache"
            )
            return []

        # 分页回退阶段必须显式放行评论接口，否则会把我们自己的分页请求也一起拦掉。
        self.anti_crawler.suppress_review_bootstrap_requests(
            False,
            reason="api_pagination_release",
        )
        self.anti_crawler.warm_session(reason="api_pagination", force=True)
        self.anti_crawler.post_captcha_stabilize(reason="api_pagination")
        self._reactivate_review_runtime(
            hotel_id,
            source_pool,
            progress,
            reason="api_pagination",
        )

        collected: list[dict] = []
        empty_streak = 0
        duplicate_streak = 0
        punish_retry_used = False
        for page_no in range(start_page, start_page + max_pages):
            if len(collected) >= remaining_quota:
                break

            payload, request_url = self._request_review_api_page_via_jsonp(
                hotel_id=hotel_id,
                filter_type=rate_score,
                page_no=page_no,
                seller_id=seller_id,
            )
            if payload is None:
                logger.warning(
                    f"评论JSONP分页触发失败: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"page_no={page_no}, rateScore={rate_score}, url={request_url or 'n/a'}"
                )
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue

            body_repr = json.dumps(payload, ensure_ascii=False) if isinstance(payload, dict) else str(payload or "")
            if "punish" in body_repr.lower() or "getpunishpage" in body_repr.lower():
                self._jsonp_punished_scores.add(rate_score)
                logger.warning(
                    f"评论JSONP分页触发风控: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"page_no={page_no}, url={request_url or 'n/a'}"
                )
                break

            parsed_page = self._parse_review_payload(payload, hotel_id, source_pool)
            if not parsed_page:
                logger.info(
                    f"评论JSONP分页页解析为空: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"page_no={page_no}, url={request_url or 'n/a'}"
                )
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue

            empty_streak = 0
            page_added = 0
            for item in parsed_page:
                review_id = item.get("review_id")
                if review_id and review_id in self.crawled_review_ids:
                    continue
                if review_id:
                    self.crawled_review_ids.add(review_id)
                collected.append(item)
                page_added += 1
                if len(collected) >= remaining_quota:
                    break

            logger.info(
                f"评论JSONP分页页处理完成: hotel_id={hotel_id}, source_pool={source_pool}, "
                f"page_no={page_no}, parsed={len(parsed_page)}, added={page_added}, url={request_url or 'n/a'}"
            )
            self._save_review_checkpoint(
                hotel_id,
                {
                    "hotel_id": hotel_id,
                    "stage": source_pool,
                    "mode": "api_pagination",
                    "source_pool": source_pool,
                    "filter_type": filter_type,
                    "api_next_page": page_no + 1,
                    "progress": progress,
                    "current_url": str(getattr(self.anti_crawler.get_page(), "url", "") or ""),
                    "collected_in_stage": len(collected),
                },
            )

            # 降低连续 JSONP 轮询的风控概率。
            self.anti_crawler.random_delay(0.6, 1.4)

            if page_added == 0:
                duplicate_streak += 1
                if duplicate_streak >= 2:
                    break
            else:
                duplicate_streak = 0

        logger.info(
            f"评论API分页回退完成: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
            f"collected={len(collected)}"
        )
        return collected

    def _extract_reviews_from_network(
        self,
        hotel_id: str,
        source_pool: str,
        progress: str,
    ) -> list[dict]:
        """当DOM为空时，通过网络包回退提取评论。"""
        packets = self._capture_review_packets(hotel_id, source_pool, progress)
        if not packets:
            return []

        reviews: list[dict] = []
        for packet in packets:
            body = packet.get("body")
            packet_reviews = self._parse_review_payload(body, hotel_id, source_pool)
            if packet_reviews:
                reviews.extend(packet_reviews)

        deduped: list[dict] = []
        for item in reviews:
            review_id = item.get("review_id")
            if review_id and review_id in self.crawled_review_ids:
                continue
            if review_id:
                self.crawled_review_ids.add(review_id)
            deduped.append(item)

        logger.info(
            f"网络回退提取完成: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
            f"raw={len(reviews)}, deduped={len(deduped)}"
        )
        return deduped

    def _parse_review_payload(self, payload: Any, hotel_id: str, source_pool: str) -> list[dict]:
        """解析网络响应负载，提取评论列表。"""
        if payload is None:
            return []

        parsed: Any = payload
        if isinstance(payload, dict):
            parsed = payload
        if isinstance(payload, bytes):
            parsed = payload.decode("utf-8", errors="ignore")

        if isinstance(parsed, str):
            text = parsed.strip()
            if not text:
                return []
            # JSONP 包装： callback(...)
            if text.endswith(")") and "(" in text and not text.startswith("{") and not text.startswith("["):
                first_brace = text.find("(")
                if first_brace != -1:
                    text = text[first_brace + 1 : -1]
            try:
                parsed = json.loads(text)
            except Exception:
                if "<li" in text and "tb-r-comment" in text:
                    return self._extract_reviews_from_html_snapshot(text, hotel_id, source_pool)
                return []

        items = self._find_review_items(parsed)
        if not items:
            return []

        reviews: list[dict] = []
        for item in items:
            mapped = self._map_network_review_item(item, hotel_id, source_pool)
            if mapped:
                reviews.append(mapped)
        return reviews

    def _find_review_items(self, obj: Any) -> list[dict]:
        """在JSON结构中递归查找评论数组。"""
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict):
                review_keys = {"content", "rateContent", "commentContent", "reviewId", "id", "userNick"}
                if any(review_keys.intersection(set(item.keys())) for item in obj if isinstance(item, dict)):
                    return obj
            for item in obj:
                found = self._find_review_items(item)
                if found:
                    return found
            return []

        if isinstance(obj, dict):
            preferred_keys = [
                "reviews",
                "reviewList",
                "rateList",
                "list",
                "result",
                "data",
            ]
            for key in preferred_keys:
                if key in obj:
                    found = self._find_review_items(obj[key])
                    if found:
                        return found
            for value in obj.values():
                found = self._find_review_items(value)
                if found:
                    return found
        return []

    def _map_network_review_item(self, item: dict, hotel_id: str, source_pool: str) -> Optional[dict]:
        """把网络包评论项映射为统一评论结构。"""
        if not isinstance(item, dict):
            return None

        content = clean_text(
            item.get("content")
            or item.get("rateContent")
            or item.get("commentContent")
            or ""
        )
        if not content:
            return None

        user_nick = clean_text(item.get("userNick") or item.get("nick") or item.get("userName") or "")
        summary = clean_text(item.get("title") or item.get("summary") or "") or None

        review_id_raw = item.get("reviewId") or item.get("id") or item.get("commentId")
        if review_id_raw:
            review_id = f"{hotel_id}_{review_id_raw}"
        else:
            review_id = self._generate_review_id(hotel_id, content, user_nick)

        review_date = parse_date(str(item.get("date") or item.get("createTime") or item.get("gmtCreate") or ""))
        tags = extract_tags(content)

        return {
            "review_id": review_id,
            "hotel_id": hotel_id,
            "user_nick": user_nick or None,
            "content": content,
            "summary": summary,
            "score_clean": None,
            "score_location": None,
            "score_service": None,
            "score_value": None,
            "overall_score": item.get("score") or item.get("rateScore"),
            "tags": tags,
            "review_date": review_date,
            "source_pool": source_pool,
        } | self._build_review_quality_metadata(
            content=content,
            summary=summary,
            source_pool=source_pool,
        )

    def _extract_reviews_from_html_snapshot(self, html: str, hotel_id: str, source_pool: str) -> list[dict]:
        """当 DOM 查询失败时，从页面 HTML 快照中直接提取评论。"""
        if not html or "tb-r-comment" not in html:
            return []
        if BeautifulSoup is None:
            return []

        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return []

        review_nodes = soup.select("li.tb-r-comment")
        if not review_nodes:
            return []

        reviews: list[dict] = []
        skipped_missing_content = 0
        skipped_duplicate = 0
        local_seen: set[str] = set()
        for node in review_nodes:
            content = self._extract_content_from_bs4_node(node)
            if not content:
                skipped_missing_content += 1
                continue

            user_nick = self._get_text_by_selectors_bs4(node, self.NICK_SELECTORS)
            summary = self._get_text_by_selectors_bs4(node, self.SUMMARY_SELECTORS)
            date_text = self._get_text_by_selectors_bs4(node, self.DATE_SELECTORS)
            review_date = parse_date(date_text) if date_text else None
            scores = self._parse_scores_bs4(node)

            review_id = self._generate_review_id(hotel_id, content, user_nick)
            if review_id in local_seen:
                skipped_duplicate += 1
                continue
            local_seen.add(review_id)

            tags = extract_tags(content)
            if summary:
                tags.extend(extract_tags(summary))
            tags = list(set(tags))

            reviews.append(
                {
                    "review_id": review_id,
                    "hotel_id": hotel_id,
                    "user_nick": user_nick,
                    "content": content,
                    "summary": summary,
                    "score_clean": scores.get("clean"),
                    "score_location": scores.get("location"),
                    "score_service": scores.get("service"),
                    "score_value": scores.get("value"),
                    "overall_score": scores.get("overall"),
                    "tags": tags,
                    "review_date": review_date,
                    "source_pool": source_pool,
                } | self._build_review_quality_metadata(
                    content=content,
                    summary=summary,
                    source_pool=source_pool,
                )
            )

        logger.info(
            f"HTML快照提取完成: hotel_id={hotel_id}, source_pool={source_pool}, "
            f"nodes={len(review_nodes)}, parsed={len(reviews)}, "
            f"skipped_missing_content={skipped_missing_content}, skipped_duplicate={skipped_duplicate}"
        )
        return reviews

    def _dedupe_against_crawled_ids(self, reviews: list[dict], hotel_id: str, source_pool: str, progress: str) -> list[dict]:
        """统一按当前会话已采集评论去重，并记录原因。"""
        deduped: list[dict] = []
        skipped = 0
        for item in reviews:
            review_id = item.get("review_id")
            if review_id and review_id in self.crawled_review_ids:
                skipped += 1
                continue
            if review_id:
                self.crawled_review_ids.add(review_id)
            deduped.append(item)

        if skipped:
            logger.info(
                f"评论会话去重完成: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
                f"input={len(reviews)}, deduped={len(deduped)}, skipped_existing={skipped}"
            )
        return deduped

    def _should_attempt_jsonp_pagination(self, source_pool: str, filter_type: int) -> bool:
        """根据当前风控画像决定是否值得继续打在线分页接口。"""
        if filter_type in self.SAFE_JSONP_FILTER_TYPES:
            return True

        logger.info(
            f"跳过高风险JSONP分页: source_pool={source_pool}, filter_type={filter_type}, "
            f"reason=known_high_risk_filter"
        )
        return False

    @staticmethod
    def _get_text_by_selectors_bs4(node, selectors: Sequence[str]) -> Optional[str]:
        """BeautifulSoup 节点按选择器优先级提取文本。"""
        for selector in selectors:
            found = node.select_one(selector)
            if not found:
                continue
            text = clean_text(found.get_text(" ", strip=True))
            if text:
                return text
        return None

    def _extract_content_from_bs4_node(self, node) -> Optional[str]:
        """优先使用精确选择器，失败时回退到评论主体文本抽取。"""
        direct = self._get_text_by_selectors_bs4(node, self.CONTENT_SELECTORS)
        if direct:
            return direct

        body = node.select_one(".tb-r-body, .tb-r-bd, .review-content")
        if not body:
            return None

        body_clone = BeautifulSoup(str(body), "html.parser") if BeautifulSoup is not None else None
        if body_clone is None:
            return None

        for selector in (
            ".starscore",
            ".tb-r-info",
            ".tb-r-photos",
            ".tb-r-photo-viewer",
            ".groupIcon",
            ".tb-r-seller",
            ".review-reply",
        ):
            for item in body_clone.select(selector):
                item.decompose()

        text = clean_text(body_clone.get_text(" ", strip=True))
        return text or None

    @staticmethod
    def _parse_scores_bs4(node) -> dict:
        """从 HTML 快照评论节点中解析评分。"""
        scores: dict[str, Optional[float]] = {}
        score_items = node.select(".starscore li")
        if not score_items:
            return scores

        label_map = {
            "清洁": "clean",
            "地理": "location",
            "服务": "service",
            "性价比": "value",
            "价格": "value",
        }
        ordered_keys = ["clean", "location", "service", "value"]
        ordered_index = 0

        for item in score_items:
            em = item.select_one("em")
            if not em:
                continue
            style = str(em.get("style") or "")
            score = parse_star_score(style)
            if score is None:
                continue

            label_text = clean_text(item.get_text(" ", strip=True))
            mapped_key = None
            for keyword, key in label_map.items():
                if keyword in label_text:
                    mapped_key = key
                    break
            if not mapped_key and ordered_index < len(ordered_keys):
                mapped_key = ordered_keys[ordered_index]
                ordered_index += 1
            if mapped_key and mapped_key not in scores:
                scores[mapped_key] = score

        valid_scores = [v for v in scores.values() if v is not None]
        if valid_scores:
            scores["overall"] = round(sum(valid_scores) / len(valid_scores), 1)
        return scores

    def _log_review_page_diagnostics(
        self,
        *,
        hotel_id: str,
        source_pool: str,
        progress: str,
        reason: str,
    ) -> None:
        page = self.anti_crawler.get_page()
        try:
            html = str(getattr(page, 'html', '') or '')
        except Exception:
            html = ''

        selector_counts: dict[str, int] = {}
        selector_counts_js: dict[str, int] = {}
        for selector in self.REVIEW_LIST_SELECTORS:
            try:
                selector_counts[selector] = len(page.eles(selector))
            except Exception:
                selector_counts[selector] = -1
            selector_counts_js[selector] = self._js_selector_count(page, selector)

        html_snapshot_tb_comment_count = html.count("tb-r-comment")

        list_state = page.run_js(
            """
            const jReview = document.querySelector('#J_ReviewList');
            const firstReviewList = document.querySelector('.review-list');
            const allReviewLists = [...document.querySelectorAll('.review-list')];
            const allCommentItems = document.querySelectorAll('li.tb-r-comment');
            return {
                exists: !!(jReview || firstReviewList),
                jReviewExists: !!jReview,
                jReviewChildCount: jReview ? jReview.children.length : 0,
                jReviewHtmlLen: jReview ? (jReview.innerHTML || '').length : 0,
                reviewListCount: allReviewLists.length,
                firstReviewListChildCount: firstReviewList ? firstReviewList.children.length : 0,
                allCommentCount: allCommentItems.length,
                firstClass: allCommentItems[0] ? String(allCommentItems[0].className || '') : '',
            };
            """
        )

        access_denied = self.anti_crawler.is_access_denied_blocked(log_detected=False)
        captcha_present = self.anti_crawler.check_captcha(log_detected=False)
        logger.warning(
            f"评论页诊断: reason={reason}, hotel_id={hotel_id}, source_pool={source_pool}, "
            f"progress={progress}, url={page.url}, html_len={len(html)}, access_denied={access_denied}, "
            f"captcha_present={captcha_present}, html_snapshot_tb_comment_count={html_snapshot_tb_comment_count}, "
            f"selector_counts={selector_counts}, selector_counts_js={selector_counts_js}, list_state={list_state}"
        )

        # 关键异常场景落地HTML快照，便于离线复盘页面真实结构。
        if reason in {"empty_review_list_final", "empty_review_list_blocked"} and html:
            try:
                diag_dir = settings.log_path / "review_diagnostics"
                diag_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{ts}_{hotel_id}_{source_pool}_{reason}.html"
                (diag_dir / filename).write_text(html, encoding="utf-8", errors="ignore")
            except Exception:
                logger.debug("评论页诊断HTML快照保存失败")

    def _get_review_runtime_state(self) -> dict[str, Any]:
        """读取评论模块运行时状态。"""
        page = self.anti_crawler.get_page()
        try:
            state = page.run_js(
                """
                const container = document.querySelector('#J_ReviewList') || document.querySelector('#J_Reviews') || document.querySelector('.review-list');
                const activeTab = document.querySelector('#hotel-review li.filter-current, #hotel-review li.current, #hotel-review .pi-tab-item.active');
                return {
                    readyState: document.readyState || 'unknown',
                    visibility: document.visibilityState || 'unknown',
                    hasFocus: !!document.hasFocus(),
                    hasKissy: typeof window.KISSY !== 'undefined',
                    hasReviewSection: !!document.querySelector('#hotel-review'),
                    hasReviewContainer: !!container,
                    reviewChildCount: container ? container.children.length : 0,
                    reviewHtmlLen: container ? (container.innerHTML || '').length : 0,
                    activeTabText: activeTab ? (activeTab.innerText || '').trim() : '',
                };
                """
            )
            return state if isinstance(state, dict) else {}
        except Exception as exc:
            logger.debug(f"读取评论运行时状态失败: {exc}")
            return {}

    def _wait_review_module_ready(
        self,
        hotel_id: Optional[str],
        source_pool: Optional[str],
        progress: str,
        reason: str,
    ) -> bool:
        """等待评论模块恢复到可交互状态。"""
        deadline = time.time() + settings.review_module_ready_timeout_seconds
        last_state: dict[str, Any] = {}
        while time.time() < deadline:
            state = self._get_review_runtime_state()
            last_state = state
            ready_state = str(state.get("readyState") or "")
            if (
                state.get("hasReviewSection")
                and state.get("hasReviewContainer")
                and ready_state in {"interactive", "complete"}
                and state.get("visibility") != "hidden"
            ):
                return True
            self.anti_crawler.random_delay(0.2, 0.45)

        logger.warning(
            f"评论模块等待超时: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
            f"reason={reason}, state={last_state}"
        )
        return False

    def _reactivate_review_runtime(
        self,
        hotel_id: Optional[str],
        source_pool: Optional[str],
        progress: str,
        reason: str,
    ) -> bool:
        """在评论交互前恢复焦点、输入和评论模块状态。"""
        self.anti_crawler.ensure_page_foreground(f"review_runtime:{reason}")
        self.anti_crawler.simulate_human_presence(reason=f"review_runtime:{reason}", include_scroll=False)
        self._trigger_review_lazy_load(hotel_id, source_pool, progress, reason=reason)
        ready = self._wait_review_module_ready(hotel_id, source_pool, progress, reason)
        logger.info(
            f"评论模块重激活完成: hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}, "
            f"reason={reason}, ready={ready}"
        )
        return ready

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
        self.anti_crawler.suppress_review_bootstrap_requests(
            True,
            reason="initial_review_navigation",
        )
        try:
            try:
                if not self.anti_crawler.navigate_to(url):
                    last_error = self.anti_crawler.last_error or "navigate_to returned False"
                    if looks_like_recoverable_error(last_error):
                        self._raise_recoverable_review_interruption(
                            hotel_id=hotel_id,
                            action="navigate_to_reviews",
                            stage="review_navigation",
                            progress="0/0",
                            message=f"评论页导航可恢复中断: {last_error}",
                        )
                    return False
            except (CaptchaException, CaptchaCooldownException) as exc:
                self._log_captcha_failure("navigate_to_reviews", hotel_id, None, "0/0", exc)
                raise

            # 等待页面加载
            self.anti_crawler.random_delay(2, 3)
            self.anti_crawler.warm_session(reason="review_navigation", force=False)
            self._ensure_review_page_ready(hotel_id, None, "review_page_navigation", "0/0")

            # 滚动到评论区域
            page = self.anti_crawler.get_page()
            review_section = page.ele('#hotel-review', timeout=5)
            if review_section:
                review_section.scroll.to_see()
                self.anti_crawler.random_delay(1, 2)
                self._ensure_review_page_ready(hotel_id, None, "review_section_scroll", "0/0")

            self.anti_crawler.suppress_review_bootstrap_requests(
                False,
                reason="initial_review_navigation_release",
            )
            self._reactivate_review_runtime(hotel_id, None, "0/0", reason="navigate_to_reviews")
            return True
        finally:
            if self.anti_crawler._blocked_url_patterns:
                self.anti_crawler.suppress_review_bootstrap_requests(
                    False,
                    reason="initial_review_navigation_cleanup",
                )

    def _crawl_positive_pool_manual(
        self,
        hotel_id: str,
        max_count: int,
        save_to_db: bool = False,
    ) -> list[dict]:
        """半自动采集正向评论：程序自动翻页，失败时再提示人工接管。"""
        if max_count <= 0:
            return []

        logger.info(
            f"开始半自动采集正向评论: hotel_id={hotel_id}, target={max_count}, "
            f"manual_page_limit={settings.review_positive_manual_page_limit}"
        )
        existing_positive = len([item for item in self._load_existing_reviews_from_db(hotel_id) if item.get("source_pool") == "positive"])
        checkpoint = self._load_review_checkpoint(hotel_id)
        if checkpoint and checkpoint.get("stage") == "positive":
            logger.info(
                f"检测到正向评论断点恢复提示: hotel_id={hotel_id}, resume_page={checkpoint.get('page_no')}, "
                f"progress={checkpoint.get('progress')}, filter={checkpoint.get('filter_text') or 'unknown'}"
            )
        if not self.navigate_to_reviews(hotel_id):
            raise RuntimeError(f"无法访问酒店 {hotel_id} 的评论页面")
        positive_entry_ready = self._ensure_positive_manual_entry(hotel_id)
        if not positive_entry_ready:
            logger.info(
                f"正向采集当前将使用“全部页 + 高分过滤”策略: hotel_id={hotel_id}, "
                "原因=好评页切换不稳定"
            )

        hotel_review_count = self._get_hotel_review_count_from_db(hotel_id)
        candidate_reviews: list[dict[str, Any]] = []
        collected: list[dict[str, Any]] = []
        quality_stats = self._quality_stat_template()
        last_signature: tuple[str, ...] = tuple()
        unchanged_pages = 0
        max_pages = max(1, settings.review_positive_manual_page_limit)
        current_state: Optional[dict[str, Any]] = None

        for page_no in range(1, max_pages + 1):
            progress = f"{len(collected)}/{max_count}"
            self._update_review_context(
                hotel_id=hotel_id,
                source_pool="positive",
                page_no=page_no,
                progress=progress,
                current_url=str(getattr(self.anti_crawler.get_page(), "url", "") or ""),
            )
            if current_state is None:
                current_state = self._get_manual_page_state(hotel_id)
            before_state = current_state
            self._log_manual_positive_page_state(hotel_id, page_no, before_state)
            logger.info(
                f"人工正向采集状态栏: {self._build_manual_positive_status_line(page_no=page_no, current_state=before_state, total_count=len(collected))}"
            )

            self._ensure_review_page_ready(hotel_id, "positive", "manual_positive_page_ready", progress)
            current_state = self._get_manual_page_state(hotel_id)
            page_signature = current_state.get("signature") or tuple()
            logger.info(
                f"人工正向采集页变化对比: hotel_id={hotel_id}, page_no={page_no}, "
                f"{self._describe_manual_page_transition(before_state, current_state)}"
            )
            if page_signature and page_signature == last_signature:
                unchanged_pages += 1
                logger.warning(
                    f"人工正向采集检测到页面未变化: hotel_id={hotel_id}, page_no={page_no}, progress={progress}, "
                    f"signature={page_signature[:3]}"
                )
                if unchanged_pages >= 2:
                    logger.warning(
                        f"疑似未翻页成功，请检查浏览器是否仍停留在上一页: hotel_id={hotel_id}, "
                        f"page_no={page_no}, progress={progress}, unchanged_pages={unchanged_pages}"
                    )
                manual_action = self._request_manual_positive_takeover(
                    hotel_id=hotel_id,
                    page_no=page_no,
                    current_count=len(collected),
                    target_count=max_count,
                    current_state=current_state,
                )
                if manual_action == "quit":
                    logger.info(f"人工正向采集结束: hotel_id={hotel_id}, page_no={page_no}, current={len(collected)}")
                    break
                if manual_action == "retry":
                    current_state = self._get_manual_page_state(hotel_id)
                continue
            unchanged_pages = 0
            snapshot_path = self._save_manual_positive_snapshot(hotel_id, page_no)
            if snapshot_path:
                logger.info(
                    f"人工正向采集页快照已保存: hotel_id={hotel_id}, page_no={page_no}, path={snapshot_path}"
                )

            page_reviews = self.extract_reviews_from_page(hotel_id, "positive", progress=progress)
            raw_count = len(page_reviews)
            page_reviews = [item for item in page_reviews if (item.get("overall_score") or 0) >= 4.0]
            filtered_count = raw_count - len(page_reviews)
            should_advance = page_no < max_pages
            if filtered_count:
                logger.info(
                    f"人工正向采集评分过滤完成: hotel_id={hotel_id}, page_no={page_no}, "
                    f"raw={raw_count}, kept={len(page_reviews)}, filtered={filtered_count}"
                )
            if not page_reviews:
                logger.warning(
                    f"人工正向采集当前页无新增评论: hotel_id={hotel_id}, page_no={page_no}, progress={progress}"
                )
                last_signature = page_signature
            else:
                candidate_reviews.extend(page_reviews)
                collected, quality_stats = self._apply_quality_selection(
                    candidate_reviews,
                    hotel_review_count=hotel_review_count,
                    source_pool="positive",
                    target_count=max_count,
                )
                self._pool_recovery_buffer = list(collected)
                last_signature = page_signature
                self._log_manual_positive_page_result(
                    hotel_id=hotel_id,
                    page_no=page_no,
                    raw_count=raw_count,
                    kept_count=len(collected),
                    filtered_count=filtered_count,
                    total_count=len(collected),
                    current_state=current_state,
                )
                logger.info(
                    f"人工正向采集质量过滤摘要: hotel_id={hotel_id}, page_no={page_no}, "
                    f"candidate_total={quality_stats['candidate_total']}, accepted_total={quality_stats['accepted_total']}, "
                    f"filtered_quality_total={quality_stats['filtered_quality_total']}, "
                    f"high_quality_count={quality_stats['high_quality_count']}, short_comment_count={quality_stats['short_comment_count']}, "
                    f"quality_relaxed_used={quality_stats['quality_relaxed_used']}, quality_fallback_used={quality_stats['quality_fallback_used']}"
                )
                self._save_review_checkpoint(
                    hotel_id,
                    {
                        "hotel_id": hotel_id,
                        "stage": "positive",
                        "mode": "manual_or_auto_positive",
                        "source_pool": "positive",
                        "page_no": page_no + 1,
                        "progress": f"{len(collected)}/{max_count}",
                        "filter_text": current_state.get("filter_text") or "",
                        "last_signature": list(page_signature[:5]),
                        "current_url": str(getattr(self.anti_crawler.get_page(), "url", "") or ""),
                        "collected_in_stage": len(collected),
                    },
                )
                if len(collected) >= max_count:
                    break

            advance_progress = f"{len(collected)}/{max_count}"
            if not should_advance:
                break
            advanced, next_state, reason = self._auto_advance_positive_page(
                hotel_id=hotel_id,
                page_no=page_no,
                progress=advance_progress,
                current_state=current_state,
            )
            if advanced:
                current_state = next_state
                continue

            logger.warning(
                f"正向自动翻页需人工接管: hotel_id={hotel_id}, page_no={page_no + 1}, progress={advance_progress}, reason={reason}"
            )
            manual_action = self._request_manual_positive_takeover(
                hotel_id=hotel_id,
                page_no=page_no + 1,
                current_count=len(collected),
                target_count=max_count,
                current_state=next_state,
            )
            if manual_action == "quit":
                logger.info(f"人工正向采集结束: hotel_id={hotel_id}, page_no={page_no + 1}, current={len(collected)}")
                break
            if manual_action == "retry":
                retry_advanced, retry_state, retry_reason = self._auto_advance_positive_page(
                    hotel_id=hotel_id,
                    page_no=page_no,
                    progress=advance_progress,
                    current_state=current_state,
                )
                if retry_advanced:
                    current_state = retry_state
                    continue
                logger.warning(
                    f"正向自动翻页重试后仍失败: hotel_id={hotel_id}, page_no={page_no + 1}, "
                    f"progress={advance_progress}, reason={retry_reason}"
                )
                current_state = retry_state
            else:
                current_state = self._get_manual_page_state(hotel_id)

        self._quality_pool_stats["positive"] = quality_stats
        self._clear_review_context()
        return collected[:max_count]

    def filter_reviews(
        self,
        filter_type: int,
        hotel_id: Optional[str] = None,
        source_pool: Optional[str] = None,
        progress: str = "0/0",
    ) -> bool:
        """筛选评论类型
        
        支持多个备用选择器，增强兼容性

        Args:
            filter_type: 筛选类型 (0=全部, 1=好评, 2=差评)

        Returns:
            是否成功
        """
        page = self.anti_crawler.get_page()
        if hotel_id:
            self._ensure_review_page_ready(hotel_id, source_pool, "filter_reviews_precheck", progress)

        # 主选择器映射（多个备用选择器）
        filter_map = {
            0: ['#review-t-1', 'input[value="0"]', '.review-filter-all'],  # 全部
            1: ['#review-t-2', 'input[value="1"]', '.review-filter-good'],  # 好评
            2: ['#review-t-4', 'input[value="2"]', '.review-filter-bad'],  # 差评
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
                    clicked = False
                    # 如果是input，尝试点击对应的label
                    if filter_elem.tag == 'input':
                        input_id = filter_elem.attr('id')
                        if input_id:
                            label = page.ele(f'label[for="{input_id}"]', timeout=1)
                            if label:
                                clicked = self._click_with_fallback(
                                    page,
                                    label,
                                    f'label[for="{input_id}"]',
                                    action="filter_reviews_click_label",
                                    hotel_id=hotel_id,
                                    source_pool=source_pool,
                                    progress=progress,
                                )
                            else:
                                clicked = self._click_with_fallback(
                                    page,
                                    filter_elem,
                                    selector,
                                    action="filter_reviews_click_input",
                                    hotel_id=hotel_id,
                                    source_pool=source_pool,
                                    progress=progress,
                                )
                        else:
                            clicked = self._click_with_fallback(
                                page,
                                filter_elem,
                                selector,
                                action="filter_reviews_click_input_no_id",
                                hotel_id=hotel_id,
                                source_pool=source_pool,
                                progress=progress,
                            )
                    else:
                        clicked = self._click_with_fallback(
                            page,
                            filter_elem,
                            selector,
                            action="filter_reviews_click_direct",
                            hotel_id=hotel_id,
                            source_pool=source_pool,
                            progress=progress,
                        )

                    if not clicked:
                        logger.warning(
                            f"评论筛选点击未成功: type={filter_type}, selector={selector}, "
                            f"hotel_id={hotel_id}, source_pool={source_pool}, progress={progress}"
                        )
                        continue

                    self.anti_crawler.random_delay(1, 2)
                    if hotel_id:
                        self._ensure_review_page_ready(hotel_id, source_pool, "filter_reviews_click", progress)
                        self._wait_review_list_ready(hotel_id, source_pool, "filter_reviews_wait", progress)

                    # 验证是否生效
                    if self._verify_filter_applied(filter_type):
                        logger.debug(f"评论筛选成功: 类型={filter_type}, 选择器={selector}")
                        return True

                    # 一次JS强制重试，处理标签点击被风控脚本吞掉的情况。
                    selector_json = json.dumps(selector, ensure_ascii=False)
                    page.run_js(
                        f"""
                        const selector = {selector_json};
                        const input = document.querySelector(selector);
                        if (!input) return false;
                        const id = input.getAttribute('id');
                        const label = id ? document.querySelector(`label[for="${{id}}"]`) : null;
                        if (label) {{
                          label.click();
                        }} else {{
                          input.click();
                        }}
                        return true;
                        """
                    )
                    self.anti_crawler.random_delay(0.8, 1.6)
                    if self._verify_filter_applied(filter_type):
                        logger.debug(f"评论筛选JS强制重试成功: 类型={filter_type}, 选择器={selector}")
                        return True

            except (CaptchaException, CaptchaCooldownException):
                raise
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
            try:
                selector_json = json.dumps(selector, ensure_ascii=False)
                checked = page.run_js(
                    f"""
                    const selector = {selector_json};
                    const el = document.querySelector(selector);
                    if (!el) return false;
                    return !!(el.checked || el.getAttribute('checked') !== null);
                    """
                )
                if checked:
                    return True
            except Exception:
                elem = page.ele(selector, timeout=1)
                if elem and elem.attr('checked'):
                    return True

        current_filter_text = page.run_js(
            """
            const current = document.querySelector('#hotel-review li.filter-current, #hotel-review li.current');
            return current ? (current.innerText || '').trim() : '';
            """
        )
        current_text = str(current_filter_text or '')
        expected_keywords = {
            self.FILTER_ALL: ("全部",),
            self.FILTER_GOOD: ("好评",),
            self.FILTER_BAD: ("差评",),
        }
        if any(keyword in current_text for keyword in expected_keywords.get(filter_type, ())):
            return True

        logger.warning(f"评论筛选验证失败: type={filter_type}, current_text={current_text}")
        return False

    def extract_reviews_from_page(
        self,
        hotel_id: str,
        source_pool: str,
        progress: str = "0/0",
    ) -> list[dict]:
        """从当前页面提取评论

        Args:
            hotel_id: 酒店ID
            source_pool: 来源池标识

        Returns:
            评论数据列表
        """
        self._wait_review_list_ready(hotel_id, source_pool, "extract_reviews_from_page", progress)
        page = self.anti_crawler.get_page()
        reviews = []

        # 查找所有评论元素
        review_elements = self._get_elements_by_selectors(page, self.REVIEW_LIST_SELECTORS)
        logger.info(f"找到 {len(review_elements)} 条评论")
        if not review_elements:
            js_review_count = self._js_review_item_count(page)
            if js_review_count > 0:
                html_snapshot_reviews = self._extract_reviews_from_html_snapshot(
                    str(getattr(page, "html", "") or ""),
                    hotel_id,
                    source_pool,
                )
                if html_snapshot_reviews:
                    html_snapshot_reviews = self._dedupe_against_crawled_ids(
                        html_snapshot_reviews, hotel_id, source_pool, progress
                    )
                    logger.info(
                        f"DOM选择器未命中但JS检测到评论，HTML快照回退成功: hotel_id={hotel_id}, "
                        f"source_pool={source_pool}, progress={progress}, js_count={js_review_count}, "
                        f"count={len(html_snapshot_reviews)}"
                    )
                    return html_snapshot_reviews

            network_reviews = self._extract_reviews_from_network(hotel_id, source_pool, progress)
            if network_reviews:
                logger.info(
                    f"DOM为空，网络回退成功: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"progress={progress}, count={len(network_reviews)}"
                )
                return network_reviews

            html_snapshot_reviews = self._extract_reviews_from_html_snapshot(
                str(getattr(page, "html", "") or ""),
                hotel_id,
                source_pool,
            )
            if html_snapshot_reviews:
                html_snapshot_reviews = self._dedupe_against_crawled_ids(
                    html_snapshot_reviews, hotel_id, source_pool, progress
                )
                logger.info(
                    f"DOM为空，HTML快照回退成功: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"progress={progress}, count={len(html_snapshot_reviews)}"
                )
                return html_snapshot_reviews

            access_denied = self.anti_crawler.is_access_denied_blocked(log_detected=False)
            captcha_present = self.anti_crawler.check_captcha(log_detected=False)
            if access_denied or captcha_present:
                self._log_review_page_diagnostics(
                    hotel_id=hotel_id,
                    source_pool=source_pool,
                    progress=progress,
                    reason="empty_review_list_blocked",
                )
                self._ensure_review_page_ready(hotel_id, source_pool, "review_list_empty_recovery", progress)
                review_elements = self._get_elements_by_selectors(page, self.REVIEW_LIST_SELECTORS)
                logger.info(
                    f"评论页恢复后再次检测评论列表: hotel_id={hotel_id}, source_pool={source_pool}, "
                    f"progress={progress}, count={len(review_elements)}"
                )

            if not review_elements:
                reopened = self._force_open_review_detail_page(
                    hotel_id=hotel_id,
                    source_pool=source_pool,
                    progress=progress,
                    reason="review_list_empty_final_reopen",
                )
                if reopened:
                    page = self.anti_crawler.get_page()
                    review_elements = self._get_elements_by_selectors(page, self.REVIEW_LIST_SELECTORS)
                    if not review_elements:
                        html_snapshot_reviews = self._extract_reviews_from_html_snapshot(
                            str(getattr(page, "html", "") or ""),
                            hotel_id,
                            source_pool,
                        )
                        if html_snapshot_reviews:
                            html_snapshot_reviews = self._dedupe_against_crawled_ids(
                                html_snapshot_reviews, hotel_id, source_pool, progress
                            )
                            logger.info(
                                f"评论重开后HTML快照回退成功: hotel_id={hotel_id}, source_pool={source_pool}, "
                                f"progress={progress}, count={len(html_snapshot_reviews)}"
                            )
                            return html_snapshot_reviews

            if not review_elements:
                self._log_review_page_diagnostics(
                    hotel_id=hotel_id,
                    source_pool=source_pool,
                    progress=progress,
                    reason="empty_review_list_final",
                )

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

            except (CaptchaException, CaptchaCooldownException):
                raise
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
            for selector in self.NICK_SELECTORS:
                nick_elem = elem.ele(selector, timeout=1)
                if nick_elem:
                    user_nick = clean_text(nick_elem.attr('title') or nick_elem.text)
                    if user_nick:
                        break

            # 评论内容
            content = self._get_text_by_selectors(elem, self.CONTENT_SELECTORS)

            if not content:
                return None

            # 评论摘要
            summary = self._get_text_by_selectors(elem, self.SUMMARY_SELECTORS)

            # 评分解析
            scores = self._parse_scores(elem)

            # 评论日期
            review_date = None
            date_text = self._get_text_by_selectors(elem, self.DATE_SELECTORS)
            if date_text:
                review_date = parse_date(date_text)

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
                'review_date': review_date,
                'source_pool': source_pool,
            } | self._build_review_quality_metadata(
                content=content,
                summary=summary,
                source_pool=source_pool,
            )

        except (CaptchaException, CaptchaCooldownException):
            raise
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

    def load_more_reviews(
        self,
        max_pages: int = 30,
        hotel_id: Optional[str] = None,
        source_pool: Optional[str] = None,
        progress: str = "0/0",
    ) -> int:
        """加载更多评论（翻页）

        Args:
            max_pages: 最大翻页数

        Returns:
            实际翻页数
        """
        page = self.anti_crawler.get_page()
        pages_loaded = 0

        if hotel_id:
            self._ensure_review_page_ready(hotel_id, source_pool, "load_more_reviews_precheck", progress)

        for _ in range(max_pages):
            # 查找下一页按钮
            next_btn = page.ele('.pi-pagination-next:not(.pi-pagination-disabled)', timeout=2)

            if not next_btn:
                if hotel_id:
                    self._log_review_page_diagnostics(
                        hotel_id=hotel_id,
                        source_pool=source_pool or "unknown",
                        progress=progress,
                        reason="pagination_next_missing",
                    )
                logger.debug("没有更多页面")
                break

            # 点击下一页
            next_btn.click()
            self._wait_review_list_ready(hotel_id, source_pool, "load_more_reviews_click_wait", progress)
            pages_loaded += 1

            # 检查验证码
            if hotel_id:
                self._ensure_review_page_ready(hotel_id, source_pool, "load_more_reviews_click", progress)
            elif self.anti_crawler.check_captcha():
                self.anti_crawler.handle_captcha()

        return pages_loaded

    def waterfall_crawl(
        self,
        hotel_id: str,
        max_reviews: Optional[int] = None,
        save_to_db: bool = True,
    ) -> list[dict]:
        """双池采集策略。

        1. negative：差评在线分页主采
        2. positive：人工辅助翻页 + HTML 提取

        Args:
            hotel_id: 酒店ID
            max_reviews: 最大评论数

        Returns:
            所有爬取到的评论
        """
        max_reviews_cap = max_reviews or settings.max_reviews_per_hotel
        all_reviews = []
        total_saved = 0
        self.crawled_review_ids.clear()
        self._pool_recovery_buffer = []
        self._review_detail_fallback_attempted.clear()
        self._jsonp_punished_scores.clear()
        self.last_crawl_summary = {}
        self._quality_pool_stats = {}
        self._manual_takeover_count = 0
        self._manual_timeout_reminder_count = 0
        checkpoint = self._load_review_checkpoint(hotel_id)

        hotel_review_count = self._get_hotel_review_count_from_db(hotel_id)
        quota = self._calculate_review_targets(hotel_review_count, max_reviews_cap=max_reviews_cap)
        target_total = quota["target_total"]
        target_negative = quota["target_negative"]
        target_positive = quota["target_positive"]

        logger.info(
            f"开始瀑布流采集酒店 {hotel_id}，动态目标 total={target_total}, "
            f"negative={target_negative}, positive={target_positive}, review_count={hotel_review_count}"
        )
        logger.info(f"双池策略已启用: positive_manual={self.positive_manual}")
        if checkpoint:
            logger.info(
                f"检测到评论断点，准备恢复: hotel_id={hotel_id}, stage={checkpoint.get('stage')}, "
                f"progress={checkpoint.get('progress')}, checkpoint={self.checkpoints.path_for('reviews', self._review_checkpoint_key(hotel_id))}"
            )

        if save_to_db:
            existing_reviews = [
                self._ensure_review_quality_metadata(item)
                for item in self._load_existing_reviews_from_db(hotel_id)
            ]
            all_reviews.extend(existing_reviews)
            for item in existing_reviews:
                review_id = item.get("review_id")
                if review_id:
                    self.crawled_review_ids.add(review_id)

        existing_negative = len(
            [item for item in all_reviews if str(item.get("source_pool") or "").lower() == "negative"]
        )
        existing_positive = len(
            [item for item in all_reviews if str(item.get("source_pool") or "").lower() == "positive"]
        )
        if existing_negative >= target_negative and existing_positive >= target_positive:
            final_reviews = list(all_reviews)
            self._record_last_crawl_summary(
                hotel_id,
                review_count=hotel_review_count,
                target_total=target_total,
                target_negative=target_negative,
                target_positive=target_positive,
                reviews=final_reviews,
            )
            self.last_crawl_summary.update(
                {
                    "skipped_negative_pool": True,
                    "skipped_positive_pool": True,
                    "remaining_negative_to_fill": 0,
                    "remaining_positive_to_fill": 0,
                }
            )
            logger.info(
                f"酒店 {hotel_id} 两池均已达标，跳过继续采集: "
                f"negative={existing_negative}/{target_negative}, positive={existing_positive}/{target_positive}"
            )
            self._clear_review_checkpoint(hotel_id)
            self._clear_review_context()
            return final_reviews

        # 导航到评论页面
        if not self.navigate_to_reviews(hotel_id):
            raise RuntimeError(f"无法访问酒店 {hotel_id} 的评论页面")

        # 检查总评论数
        total_count = self.get_total_review_count()
        if total_count is None:
            logger.warning(f"无法获取酒店 {hotel_id} 的评论数")
        elif total_count <= 0:
            logger.info(f"酒店 {hotel_id} 无可采集评论，跳过")
            final_reviews = list(all_reviews)
            self._record_last_crawl_summary(
                hotel_id,
                review_count=hotel_review_count,
                target_total=target_total,
                target_negative=target_negative,
                target_positive=target_positive,
                reviews=final_reviews,
            )
            self.last_crawl_summary.update(
                {
                    "skipped_negative_pool": existing_negative >= target_negative,
                    "skipped_positive_pool": existing_positive >= target_positive,
                    "remaining_negative_to_fill": max(target_negative - existing_negative, 0),
                    "remaining_positive_to_fill": max(target_positive - existing_positive, 0),
                }
            )
            self._clear_review_checkpoint(hotel_id)
            self._clear_review_context()
            return final_reviews
        elif total_count < settings.min_reviews_threshold:
            logger.warning(
                f"酒店 {hotel_id} 评论数 {total_count} 低于阈值 {settings.min_reviews_threshold}，"
                "但仍继续采集（该酒店已在采样列表中）"
            )

        def _crawl_and_checkpoint_pool(
            source_pool: str,
            filter_types: list[int],
            max_count: int,
            api_start_page: int = 1,
        ) -> int:
            nonlocal total_saved
            logger.info(f"开始爬取评论池: {source_pool}, 目标 {max_count} 条")
            self._pool_recovery_buffer = []
            try:
                pool_reviews = self._crawl_pool(
                    hotel_id=hotel_id,
                    source_pool=source_pool,
                    filter_types=filter_types,
                    max_count=max_count,
                    api_start_page=api_start_page,
                    hotel_review_count=hotel_review_count,
                )
            except Exception:
                if save_to_db and self._pool_recovery_buffer:
                    partial_saved = self.save_reviews(self._pool_recovery_buffer)
                    total_saved += partial_saved
                    logger.warning(
                        f"评论池异常前已保存缓冲评论: pool={source_pool}, "
                        f"buffered={len(self._pool_recovery_buffer)}, saved={partial_saved}"
                    )
                raise
            all_reviews.extend(pool_reviews)

            saved_count = 0
            if save_to_db and pool_reviews:
                saved_count = self.save_reviews(pool_reviews)
                total_saved += saved_count
            self._pool_recovery_buffer = []

            logger.info(
                f"评论池完成: pool={source_pool}, crawled={len(pool_reviews)}, "
                f"saved={saved_count if save_to_db else 0}, total_collected={len(all_reviews)}"
            )
            return len(pool_reviews)

        # 步骤1: 负面警示池（差评）
        negative_target = min(target_negative, target_total)
        negative_existing = len(
            [item for item in all_reviews if str(item.get("source_pool") or "").lower() == "negative"]
        )
        total_remaining_quota = max(negative_target - negative_existing, 0)
        logger.info("步骤1: 爬取负面警示池")
        try:
            remaining_negative = max(
                min(negative_target - negative_existing, total_remaining_quota),
                0,
            )
            if remaining_negative <= 0:
                logger.info(
                    f"负面池已由历史数据/断点补足，跳过继续采集: "
                    f"hotel_id={hotel_id}, negative_existing={negative_existing}, total_remaining={total_remaining_quota}"
                )
            else:
                negative_api_start = 1
                if checkpoint and checkpoint.get("stage") == "negative":
                    negative_api_start = max(1, int(checkpoint.get("api_next_page") or 1))
                _crawl_and_checkpoint_pool(
                    source_pool="negative",
                    filter_types=[self.FILTER_BAD],
                    max_count=remaining_negative,
                    api_start_page=negative_api_start,
                )
        except (CaptchaException, CaptchaCooldownException) as exc:
            logger.warning(
                f"评论池中断: pool=negative, 已采集={len(all_reviews)}, 已保存={total_saved}, error={exc}"
            )
            raise
        except RecoverableInterruption:
            raise
        except Exception as exc:
            if looks_like_recoverable_error(exc):
                self._raise_recoverable_review_interruption(
                    hotel_id=hotel_id,
                    action="waterfall_negative",
                    stage="negative",
                    progress=f"{len(all_reviews)}/{target_total}",
                    message=f"评论负面池发生可恢复中断: {exc}",
                )
            raise

        current_mix = self._summarize_review_mix(all_reviews)
        actual_negative = int(current_mix["actual_negative"])
        actual_positive = int(current_mix["actual_positive"])
        if actual_negative < target_negative:
            logger.info(
                f"负评软目标未达成但接受短缺: hotel_id={hotel_id}, "
                f"target_negative={target_negative}, actual_negative={actual_negative}"
            )

        remaining_positive = max(target_positive - actual_positive, 0)

        if remaining_positive <= 0:
            self._clear_review_checkpoint(hotel_id)
            final_reviews = list(all_reviews)
            self._record_last_crawl_summary(
                hotel_id,
                review_count=hotel_review_count,
                target_total=target_total,
                target_negative=target_negative,
                target_positive=target_positive,
                reviews=final_reviews,
            )
            self.last_crawl_summary.update(
                {
                    "skipped_negative_pool": negative_existing >= target_negative,
                    "skipped_positive_pool": True,
                    "remaining_negative_to_fill": max(target_negative - actual_negative, 0),
                    "remaining_positive_to_fill": 0,
                }
            )
            return final_reviews

        logger.info(
            f"步骤2: 采集正向评论，剩余正评配额 {remaining_positive}, target_positive={target_positive}, "
            f"actual_negative={actual_negative}"
        )
        if remaining_positive > 0:
            if not self.positive_manual:
                logger.info("正向人工辅助采集已关闭，跳过正向评论池")
            else:
                logger.info("开始人工辅助采集正向评论池")
                try:
                    positive_reviews = self._crawl_positive_pool_manual(
                        hotel_id=hotel_id,
                        max_count=remaining_positive,
                        save_to_db=save_to_db,
                    )
                    all_reviews.extend(positive_reviews)

                    saved_count = 0
                    if save_to_db and positive_reviews:
                        saved_count = self.save_reviews(positive_reviews)
                        total_saved += saved_count
                    self._pool_recovery_buffer = []

                    logger.info(
                        f"评论池完成: pool=positive, crawled={len(positive_reviews)}, "
                        f"saved={saved_count if save_to_db else 0}, total_collected={len(all_reviews)}"
                    )
                except RecoverableInterruption:
                    if save_to_db and self._pool_recovery_buffer:
                        partial_saved = self.save_reviews(self._pool_recovery_buffer)
                        total_saved += partial_saved
                        logger.warning(
                            f"正向评论池异常前已保存缓冲评论: buffered={len(self._pool_recovery_buffer)}, saved={partial_saved}"
                        )
                    raise
                except Exception as exc:
                    if save_to_db and self._pool_recovery_buffer:
                        partial_saved = self.save_reviews(self._pool_recovery_buffer)
                        total_saved += partial_saved
                        logger.warning(
                            f"正向评论池异常前已保存缓冲评论: buffered={len(self._pool_recovery_buffer)}, saved={partial_saved}"
                        )
                    if looks_like_recoverable_error(exc):
                        self._raise_recoverable_review_interruption(
                            hotel_id=hotel_id,
                            action="waterfall_positive",
                            stage="positive",
                            progress=f"{len(all_reviews)}/{target_total}",
                            message=f"评论正向池发生可恢复中断: {exc}",
                        )
                    raise

        final_reviews = list(all_reviews)
        summary = self._record_last_crawl_summary(
            hotel_id,
            review_count=hotel_review_count,
            target_total=target_total,
            target_negative=target_negative,
            target_positive=target_positive,
            reviews=final_reviews,
        )
        summary.update(
            {
                "skipped_negative_pool": negative_existing >= target_negative,
                "skipped_positive_pool": (remaining_positive <= 0) or (not self.positive_manual and remaining_positive > 0),
                "remaining_negative_to_fill": max(target_negative - summary["actual_negative"], 0),
                "remaining_positive_to_fill": max(target_positive - summary["actual_positive"], 0),
            }
        )
        logger.info(
            f"酒店 {hotel_id} 采集完成，共 {len(final_reviews)} 条评论，"
            f"negative={summary['actual_negative']}/{target_negative}, "
            f"positive={summary['actual_positive']}/{target_positive}, "
            f"negative_ratio={summary['actual_negative_ratio']:.2%}, "
            f"high_quality_ratio={summary['high_quality_ratio']:.2%}, "
            f"short_comment_ratio={summary['short_comment_ratio']:.2%}, "
            f"filtered_quality_total={summary['filtered_quality_total']}"
        )
        self._clear_review_checkpoint(hotel_id)
        self._clear_review_context()
        return final_reviews

    def _crawl_pool(
        self,
        hotel_id: str,
        source_pool: str,
        filter_types: list[int],
        max_count: int = 100,
        api_start_page: int = 1,
        hotel_review_count: Optional[int] = None,
    ) -> list[dict]:
        """爬取指定池的评论

        Args:
            hotel_id: 酒店ID
            source_pool: 来源池标识
            filter_types: 筛选类型列表
            max_count: 最大数量

        Returns:
            评论列表
        """
        if max_count <= 0:
            return []

        reviews: list[dict[str, Any]] = []
        candidate_reviews: list[dict[str, Any]] = []
        quality_stats = self._quality_stat_template()
        stagnant_rounds = 0
        stagnant_limit = max(1, settings.review_stagnant_page_limit)

        for filter_type in filter_types:
            if len(reviews) >= max_count:
                break

            progress = f"{len(reviews)}/{max_count}"
            api_fallback_used = False

            # 应用筛选
            try:
                filter_applied = self.filter_reviews(
                    filter_type,
                    hotel_id=hotel_id,
                    source_pool=source_pool,
                    progress=progress,
                )
            except (CaptchaException, CaptchaCooldownException) as exc:
                self._log_captcha_failure("filter_reviews", hotel_id, source_pool, progress, exc)
                raise

            if not filter_applied:
                logger.warning(
                    f"筛选未生效，降级为当前列表继续采集: pool={source_pool}, "
                    f"filter_type={filter_type}, progress={progress}"
                )
            else:
                logger.info(
                    f"筛选已生效: pool={source_pool}, filter_type={filter_type}, progress={progress}"
                )

            self.anti_crawler.random_delay(1, 2)

            # 爬取当前页
            page_reviews = self.extract_reviews_from_page(hotel_id, source_pool, progress=progress)
            if source_pool == "negative" and not filter_applied:
                logger.info(
                    f"负面池筛选未稳定生效，跳过当前页HTML结果: hotel_id={hotel_id}, progress={progress}, "
                    f"raw_page_reviews={len(page_reviews)}"
                )
                page_reviews = []

            if page_reviews:
                candidate_reviews.extend(page_reviews)
                reviews, quality_stats = self._apply_quality_selection(
                    candidate_reviews,
                    hotel_review_count=hotel_review_count,
                    source_pool=source_pool,
                    target_count=max_count,
                )
                self._pool_recovery_buffer = list(reviews)
            logger.info(
                f"当前页采集完成: pool={source_pool}, filter_type={filter_type}, progress={progress}, "
                f"page_reviews={len(page_reviews)}, total_reviews={len(reviews)}, "
                f"candidate_total={quality_stats['candidate_total']}, filtered_quality_total={quality_stats['filtered_quality_total']}, "
                f"high_quality_count={quality_stats['high_quality_count']}, short_comment_count={quality_stats['short_comment_count']}"
            )
            stagnant_rounds = 0 if page_reviews else stagnant_rounds + 1

            # 翻页继续爬取
            while len(reviews) < max_count and stagnant_rounds < stagnant_limit:
                progress = f"{len(reviews)}/{max_count}"
                try:
                    pages = self.load_more_reviews(
                        max_pages=1,
                        hotel_id=hotel_id,
                        source_pool=source_pool,
                        progress=progress,
                    )
                except (CaptchaException, CaptchaCooldownException) as exc:
                    self._log_captcha_failure("load_more_reviews", hotel_id, source_pool, progress, exc)
                    raise

                if pages == 0:
                    if not api_fallback_used:
                        remaining = max_count - len(reviews)
                        api_reviews: list[dict] = []
                        if self._should_attempt_jsonp_pagination(source_pool, filter_type):
                            api_reviews = self._crawl_reviews_via_api_pagination(
                                hotel_id=hotel_id,
                                source_pool=source_pool,
                                filter_type=filter_type,
                                remaining_quota=remaining,
                                progress=progress,
                                start_page=api_start_page,
                            )
                        if api_reviews:
                            candidate_reviews.extend(api_reviews)
                            reviews, quality_stats = self._apply_quality_selection(
                                candidate_reviews,
                                hotel_review_count=hotel_review_count,
                                source_pool=source_pool,
                                target_count=max_count,
                            )
                            self._pool_recovery_buffer = list(reviews)
                            logger.info(
                                f"API分页回退追加完成: pool={source_pool}, filter_type={filter_type}, "
                                f"progress={progress}, api_reviews={len(api_reviews)}, total_reviews={len(reviews)}, "
                                f"candidate_total={quality_stats['candidate_total']}, filtered_quality_total={quality_stats['filtered_quality_total']}"
                            )
                        api_fallback_used = True
                    break

                page_reviews = self.extract_reviews_from_page(hotel_id, source_pool, progress=progress)
                if not page_reviews:
                    stagnant_rounds += 1
                    continue

                candidate_reviews.extend(page_reviews)
                reviews, quality_stats = self._apply_quality_selection(
                    candidate_reviews,
                    hotel_review_count=hotel_review_count,
                    source_pool=source_pool,
                    target_count=max_count,
                )
                self._pool_recovery_buffer = list(reviews)
                stagnant_rounds = 0
                self.anti_crawler.random_delay(1, 2)

        self._quality_pool_stats[source_pool] = quality_stats
        return reviews[:max_count]

    def save_reviews(self, reviews: list[dict]) -> int:
        """保存评论到数据库

        Args:
            reviews: 评论数据列表

        Returns:
            成功保存的数量
        """
        saved_count = 0
        existing_ids: set[str] = set()
        existing_negative_ids: set[str] = set()
        existing_positive_ids: set[str] = set()
        review_columns = {col.name for col in Review.__table__.columns}

        review_ids = {
            review.get('review_id')
            for review in reviews
            if review.get('review_id')
        }

        with session_scope() as session:
            if review_ids:
                existing_rows = session.query(Review.review_id).filter(Review.review_id.in_(review_ids)).all()
                existing_ids = {row[0] for row in existing_rows if row and row[0]}
                negative_rows = session.query(ReviewNegative.review_id).filter(
                    ReviewNegative.review_id.in_(review_ids)
                ).all()
                positive_rows = session.query(ReviewPositive.review_id).filter(
                    ReviewPositive.review_id.in_(review_ids)
                ).all()
                existing_negative_ids = {row[0] for row in negative_rows if row and row[0]}
                existing_positive_ids = {row[0] for row in positive_rows if row and row[0]}

            for review_data in reviews:
                try:
                    review_payload = dict(review_data)

                    # 验证数据
                    validated = ReviewModel(**review_payload)
                    validated_payload = validated.model_dump(
                        exclude_none=True,
                    )
                    review_payload_for_orm = {
                        key: value
                        for key, value in validated_payload.items()
                        if key in review_columns
                    }

                    review_id = validated.review_id
                    source_pool = str(validated.source_pool or "").strip().lower()

                    if review_id not in existing_ids:
                        review = Review(**review_payload_for_orm)
                        session.add(review)
                        session.flush()
                        saved_count += 1
                        existing_ids.add(review_id)

                    if source_pool == "negative" and review_id not in existing_negative_ids:
                        session.add(
                            ReviewNegative(
                                review_id=review_id,
                                hotel_id=validated.hotel_id,
                                user_nick=validated.user_nick,
                                content=validated.content,
                                summary=validated.summary,
                                overall_score=validated.overall_score,
                                review_date=validated.review_date,
                            )
                        )
                        existing_negative_ids.add(review_id)
                    elif source_pool == "positive" and review_id not in existing_positive_ids:
                        session.add(
                            ReviewPositive(
                                review_id=review_id,
                                hotel_id=validated.hotel_id,
                                user_nick=validated.user_nick,
                                content=validated.content,
                                summary=validated.summary,
                                overall_score=validated.overall_score,
                                review_date=validated.review_date,
                            )
                        )
                        existing_positive_ids.add(review_id)

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
        reviews = self.waterfall_crawl(hotel_id, save_to_db=save_to_db)

        if save_to_db:
            logger.info(f"酒店 {hotel_id} 评论采集已在分池阶段完成保存")

        return reviews
