"""全局配置管理。"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """应用配置类，支持从 .env 文件读取配置。"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 数据库配置
    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=5432, alias="DB_PORT")
    db_name: str = Field(default="hotel_reviews", alias="DB_NAME")
    db_user: str = Field(default="postgres", alias="DB_USER")
    db_password: str = Field(default="", alias="DB_PASSWORD")

    # Chrome 调试配置
    chrome_debug_port: int = Field(default=9222, alias="CHROME_DEBUG_PORT")
    chrome_user_data_dir: str = Field(
        default="C:/selenium/automation_profile",
        alias="CHROME_USER_DATA_DIR",
    )

    # 爬虫基础配置
    min_delay: float = Field(default=3.0, alias="MIN_DELAY")
    max_delay: float = Field(default=6.0, alias="MAX_DELAY")
    max_retries: int = Field(default=3, alias="MAX_RETRIES")
    request_timeout: int = Field(default=30, alias="REQUEST_TIMEOUT")

    # 评论采集配置
    # 单酒店评论抓取的最终硬上限；动态配额算出来后仍会再受它兜底约束。
    max_reviews_per_hotel: int = Field(default=300, alias="MAX_REVIEWS_PER_HOTEL")
    # 酒店进入评论采集队列的最低评论数门槛。
    min_reviews_threshold: int = Field(default=200, alias="MIN_REVIEWS_THRESHOLD")
    # 动态总量比例：target_total ≈ review_count * ratio。
    review_total_sample_ratio: float = Field(default=0.20, alias="REVIEW_TOTAL_SAMPLE_RATIO")
    # 动态总量下限：评论再少的酒店，也尽量保留这么多条高质量评论。
    review_total_min_per_hotel: int = Field(default=80, alias="REVIEW_TOTAL_MIN_PER_HOTEL")
    # 动态总量上限：评论再多的酒店，也不会超过这个目标总量。
    review_total_max_per_hotel: int = Field(default=300, alias="REVIEW_TOTAL_MAX_PER_HOTEL")
    # 负评软目标比例：target_negative ≈ target_total * ratio。
    review_negative_target_ratio: float = Field(default=0.25, alias="REVIEW_NEGATIVE_TARGET_RATIO")
    # 负评软目标下限：避免评论池几乎全是正评。
    review_negative_min_per_hotel: int = Field(default=20, alias="REVIEW_NEGATIVE_MIN_PER_HOTEL")
    # 负评软目标上限：避免单酒店被过多差评主导。
    review_negative_max_per_hotel: int = Field(default=80, alias="REVIEW_NEGATIVE_MAX_PER_HOTEL")
    # 兼容旧逻辑保留，内部优先使用 REVIEW_NEGATIVE_MAX_PER_HOTEL。
    review_negative_pool_limit: int = Field(default=100, alias="REVIEW_NEGATIVE_POOL_LIMIT")
    # 是否启用“评论长度/信息量分层 + 动态放宽”规则。
    review_quality_strict_enable: bool = Field(default=True, alias="REVIEW_QUALITY_STRICT_ENABLE")
    # 正评默认最低有效长度；达不到时只有在放宽阶段才可能被保留。
    review_positive_min_effective_len: int = Field(default=12, alias="REVIEW_POSITIVE_MIN_EFFECTIVE_LEN")
    # 负评默认最低有效长度；通常比正评更宽松。
    review_negative_min_effective_len: int = Field(default=10, alias="REVIEW_NEGATIVE_MIN_EFFECTIVE_LEN")
    # 正评兜底放宽时允许的最短有效长度，仍需命中酒店维度词。
    review_positive_short_fallback_min_len: int = Field(
        default=8,
        alias="REVIEW_POSITIVE_SHORT_FALLBACK_MIN_LEN",
    )
    # 负评兜底放宽时允许的最短有效长度，适合“短但具体”的问题评论。
    review_negative_short_fallback_min_len: int = Field(
        default=6,
        alias="REVIEW_NEGATIVE_SHORT_FALLBACK_MIN_LEN",
    )
    # S级评论长度阈值；超过后直接视为高价值长评论。
    review_strict_tier_s_min_len: int = Field(default=40, alias="REVIEW_STRICT_TIER_S_MIN_LEN")
    # 命中多个方面词时，达到该长度也可升为S级。
    review_strict_tier_s_aspect_min_len: int = Field(
        default=25,
        alias="REVIEW_STRICT_TIER_S_ASPECT_MIN_LEN",
    )
    # B级评论长度阈值；12-19左右通常已具备基本证据价值。
    review_tier_b_min_len: int = Field(default=12, alias="REVIEW_TIER_B_MIN_LEN")
    # C级评论长度阈值；只在放宽阶段有限放行。
    review_tier_c_min_len: int = Field(default=8, alias="REVIEW_TIER_C_MIN_LEN")
    # 高评论量酒店允许的短评论占比上限。
    review_short_comment_max_ratio_high: float = Field(
        default=0.10,
        alias="REVIEW_SHORT_COMMENT_MAX_RATIO_HIGH",
    )
    # 中评论量酒店允许的短评论占比上限。
    review_short_comment_max_ratio_mid: float = Field(
        default=0.20,
        alias="REVIEW_SHORT_COMMENT_MAX_RATIO_MID",
    )
    # 低评论量酒店允许的短评论占比上限。
    review_short_comment_max_ratio_low: float = Field(
        default=0.30,
        alias="REVIEW_SHORT_COMMENT_MAX_RATIO_LOW",
    )
    # “短但具体”评论的最终占比上限，防止知识库被碎片化短评淹没。
    review_specific_short_max_ratio: float = Field(
        default=0.15,
        alias="REVIEW_SPECIFIC_SHORT_MAX_RATIO",
    )
    # 放宽触发比例：严格筛选后若保留数低于 target_count * ratio，则进入放宽阶段。
    review_quality_relax_trigger_ratio: float = Field(
        default=0.70,
        alias="REVIEW_QUALITY_RELAX_TRIGGER_RATIO",
    )
    # 正向人工辅助模式最多尝试多少页。
    review_positive_page_limit: int = Field(default=15, alias="REVIEW_POSITIVE_PAGE_LIMIT")
    review_positive_manual_default_enabled: bool = Field(
        default=True,
        alias="REVIEW_POSITIVE_MANUAL_DEFAULT_ENABLED",
    )
    # 正向人工辅助模式的最大页数；页数越大，手工参与成本越高。
    review_positive_manual_page_limit: int = Field(
        default=20,
        alias="REVIEW_POSITIVE_MANUAL_PAGE_LIMIT",
    )
    # 自动翻到下一页后的最小等待秒数，主要用于降低风控。
    review_positive_auto_page_delay_min_seconds: float = Field(
        default=30.0,
        alias="REVIEW_POSITIVE_AUTO_PAGE_DELAY_MIN_SECONDS",
    )
    # 自动翻到下一页后的最大等待秒数。
    review_positive_auto_page_delay_max_seconds: float = Field(
        default=60.0,
        alias="REVIEW_POSITIVE_AUTO_PAGE_DELAY_MAX_SECONDS",
    )
    # 连续多少页没有新增评论后，当前池停止继续翻页。
    review_stagnant_page_limit: int = Field(default=2, alias="REVIEW_STAGNANT_PAGE_LIMIT")

    # 分层采样策略配置
    sampling_policy_enabled: bool = Field(default=False, alias="SAMPLING_POLICY_ENABLED")
    sampling_city_compensation_enabled: bool = Field(
        default=True,
        alias="SAMPLING_CITY_COMPENSATION_ENABLED",
    )
    sampling_sparse_tiers: str = Field(
        default="高档型,奢华型",
        alias="SAMPLING_SPARSE_TIERS",
    )
    sampling_floor_ratio_economy: float = Field(default=0.90, alias="SAMPLING_FLOOR_RATIO_ECONOMY")
    sampling_floor_ratio_comfort: float = Field(default=0.85, alias="SAMPLING_FLOOR_RATIO_COMFORT")
    sampling_floor_ratio_high: float = Field(default=0.70, alias="SAMPLING_FLOOR_RATIO_HIGH")
    sampling_floor_ratio_luxury: float = Field(default=0.60, alias="SAMPLING_FLOOR_RATIO_LUXURY")
    sampling_borrow_cap_ratio: float = Field(default=0.30, alias="SAMPLING_BORROW_CAP_RATIO")
    sampling_donor_guard_ratio: float = Field(default=0.10, alias="SAMPLING_DONOR_GUARD_RATIO")
    sampling_threshold_ladder: str = Field(
        default="200,150,120",
        alias="SAMPLING_THRESHOLD_LADDER",
    )

    # 验证码配置
    captcha_max_retries: int = Field(default=3, alias="CAPTCHA_MAX_RETRIES")
    captcha_cooldown_seconds: int = Field(default=180, alias="CAPTCHA_COOLDOWN_SECONDS")
    captcha_solve_timeout_seconds: int = Field(default=180, alias="CAPTCHA_SOLVE_TIMEOUT_SECONDS")
    captcha_refresh_retry_limit: int = Field(default=5, alias="CAPTCHA_REFRESH_RETRY_LIMIT")
    captcha_click_slide_refresh_threshold: int = Field(
        default=2,
        alias="CAPTCHA_CLICK_SLIDE_REFRESH_THRESHOLD",
    )
    access_denied_max_retries: int = Field(default=2, alias="ACCESS_DENIED_MAX_RETRIES")
    access_denied_backoff_min_seconds: float = Field(default=8.0, alias="ACCESS_DENIED_BACKOFF_MIN_SECONDS")
    access_denied_backoff_max_seconds: float = Field(default=18.0, alias="ACCESS_DENIED_BACKOFF_MAX_SECONDS")
    human_interaction_enabled: bool = Field(default=True, alias="HUMAN_INTERACTION_ENABLED")
    session_warmup_enabled: bool = Field(default=True, alias="SESSION_WARMUP_ENABLED")
    session_idle_rewarm_seconds: int = Field(default=120, alias="SESSION_IDLE_REWARM_SECONDS")
    review_module_ready_timeout_seconds: int = Field(default=12, alias="REVIEW_MODULE_READY_TIMEOUT_SECONDS")
    review_request_capture_attempts: int = Field(default=3, alias="REVIEW_REQUEST_CAPTURE_ATTEMPTS")
    captcha_debug_artifacts_enabled: bool = Field(
        default=False,
        alias="CAPTCHA_DEBUG_ARTIFACTS_ENABLED",
    )
    captcha_debug_artifacts_dir: str = Field(
        default="logs/captcha_artifacts",
        alias="CAPTCHA_DEBUG_ARTIFACTS_DIR",
    )

    # 日志配置
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_dir: str = Field(default="logs", alias="LOG_DIR")

    # 项目路径
    base_dir: Path = Path(__file__).parent.parent

    @property
    def database_url(self) -> str:
        """获取数据库连接 URL。"""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def chrome_address(self) -> str:
        """获取 Chrome 调试地址。"""
        return f"127.0.0.1:{self.chrome_debug_port}"

    @property
    def log_path(self) -> Path:
        """获取日志目录路径。"""
        return self.base_dir / self.log_dir

    @property
    def captcha_debug_artifacts_path(self) -> Path:
        """获取验证码调试产物目录路径。"""
        return self.base_dir / self.captcha_debug_artifacts_dir

    @property
    def sampling_sparse_tier_levels(self) -> list[str]:
        """获取稀缺档位列表。"""
        tiers = [item.strip() for item in self.sampling_sparse_tiers.split(",") if item.strip()]
        if not tiers:
            return ["高档型", "奢华型"]
        return tiers

    @property
    def sampling_threshold_steps(self) -> list[int]:
        """获取阈值阶梯，保证包含基础阈值且按降序去重。"""
        values: list[int] = []
        for item in self.sampling_threshold_ladder.split(","):
            token = item.strip()
            if not token:
                continue
            try:
                parsed = int(token)
            except ValueError:
                continue
            if parsed > 0:
                values.append(parsed)

        if self.min_reviews_threshold > 0:
            values.append(int(self.min_reviews_threshold))

        if not values:
            return [200]

        return sorted(set(values), reverse=True)

# 全局配置实例
settings = Settings()
