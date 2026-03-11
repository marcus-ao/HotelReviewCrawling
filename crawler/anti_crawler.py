"""反爬虫策略模块

使用DrissionPage接管Chrome浏览器，绕过飞猪的自动化检测。
支持滑块验证码自动处理和人工介入机制。
"""
import random
import time
from importlib import import_module
from typing import Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from crawler.exceptions import (
    CaptchaAutoSlideFailed,
    CaptchaCooldownException,
    CaptchaException,
    CaptchaTimeoutException,
)
from utils.logger import get_logger

logger = get_logger("anti_crawler")


class AntiCrawler:
    """反爬虫策略类"""

    def __init__(self):
        self.page: Optional[Any] = None
        self.is_connected = False
        self._captcha_retry_count = 0
        self._captcha_refresh_count = 0

    def init_browser(self) -> Any:
        """初始化浏览器（接管已打开的Chrome）

        需要先手动启动Chrome:
        chrome.exe --remote-debugging-port=9222 --user-data-dir="C:/selenium/automation_profile"

        Returns:
            ChromiumPage实例
        """
        try:
            # 连接到已打开的Chrome浏览器
            drission_page = import_module("DrissionPage")
            chromium_page_cls = getattr(drission_page, "ChromiumPage")
            self.page = chromium_page_cls(addr_or_opts=settings.chrome_address)
            self.is_connected = True
            logger.info(f"成功连接到Chrome浏览器: {settings.chrome_address}")
            return self.page
        except Exception as e:
            logger.error(f"连接Chrome浏览器失败: {e}")
            logger.info("请确保已手动启动Chrome并开启远程调试端口:")
            logger.info(f'chrome.exe --remote-debugging-port={settings.chrome_debug_port} '
                       f'--user-data-dir="{settings.chrome_user_data_dir}"')
            raise

    def get_page(self) -> Any:
        """获取页面实例"""
        if not self.page or not self.is_connected:
            return self.init_browser()
        return self.page

    def random_delay(self, min_delay: Optional[float] = None, max_delay: Optional[float] = None) -> None:
        """随机延迟，模拟人类行为

        Args:
            min_delay: 最小延迟秒数
            max_delay: 最大延迟秒数
        """
        min_d = min_delay or settings.min_delay
        max_d = max_delay or settings.max_delay
        delay = random.uniform(min_d, max_d)
        logger.debug(f"随机延迟 {delay:.2f} 秒")
        time.sleep(delay)

    def check_captcha(self) -> bool:
        """检查是否出现验证码

        Returns:
            是否存在验证码
        """
        page = self.get_page()

        # 检查常见的滑块验证码元素
        captcha_selectors = [
            '#nc_1_n1z',           # 阿里滑块验证码
            '.nc-container',       # 滑块容器
            '.nc_wrapper',         # 滑块包装器
            '#baxia-dialog-content',  # 百度验证码
            '.J_MIDDLEWARE_FRAME_WIDGET',  # 中间件验证
        ]

        for selector in captcha_selectors:
            if page.ele(selector, timeout=1):
                logger.warning(f"检测到验证码: {selector}")
                return True

        return False

    def _reset_captcha_counters(self) -> None:
        """重置验证码重试计数器。"""
        self._captcha_retry_count = 0
        self._captcha_refresh_count = 0

    def _raise_captcha_failure(self, exc: CaptchaException, reason: str) -> None:
        """根据重试上限抛出可重试异常或冷却异常。"""
        self._captcha_retry_count = exc.retry_count
        if exc.should_retry():
            raise exc

        raise CaptchaCooldownException(
            cooldown_seconds=settings.captcha_cooldown_seconds,
            reason=reason,
        )

    def _is_verification_expired(self, attempts_since_refresh: int) -> bool:
        """检查验证页是否过期或卡死。"""
        page = self.get_page()

        expiry_keywords = [
            "验证已过期",
            "验证失效",
            "请刷新",
            "页面已失效",
            "重新验证",
            "点击刷新",
        ]

        try:
            html = page.html or ""
            if any(keyword in html for keyword in expiry_keywords):
                return True
        except Exception:
            logger.debug("读取验证页内容失败，跳过关键字检测")

        # 启发式判断：同一页面多次自动滑块失败，且验证码仍在。
        return attempts_since_refresh >= 2 and self.check_captcha()

    def _refresh_verification_page(self) -> None:
        """刷新验证页以恢复过期状态。"""
        page = self.get_page()
        page.refresh()
        page.wait.load_start()
        self.random_delay(1, 2)

    def handle_captcha(self, auto_retry: bool = True) -> None:
        """处理验证码

        Args:
            auto_retry: 是否自动重试滑块

        Raises:
            CaptchaAutoSlideFailed: 自动滑块失败（可重试）
            CaptchaTimeoutException: 验证码处理超时或验证页过期（可重试）
            CaptchaCooldownException: 达到最大重试次数后进入冷却（终止）
        """
        if not self.check_captcha():
            self._reset_captcha_counters()
            return

        if not auto_retry:
            next_retry_count = self._captcha_retry_count + 1
            self._raise_captcha_failure(
                CaptchaAutoSlideFailed(
                    reason="检测到验证码且自动重试已关闭",
                    retry_count=next_retry_count,
                    max_retries=settings.captcha_max_retries,
                ),
                reason="验证码自动处理被禁用，进入冷却",
            )

        start_time = time.time()
        attempts_since_refresh = 0

        while self.check_captcha():
            elapsed_seconds = int(time.time() - start_time)
            if elapsed_seconds >= settings.captcha_solve_timeout_seconds:
                next_retry_count = self._captcha_retry_count + 1
                self._raise_captcha_failure(
                    CaptchaTimeoutException(
                        timeout_seconds=settings.captcha_solve_timeout_seconds,
                        elapsed_seconds=elapsed_seconds,
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason="验证码处理超时，进入冷却",
                )

            success = self._auto_slide_captcha()
            if success:
                self.random_delay(0.8, 1.5)
                if not self.check_captcha():
                    logger.info("自动滑块验证成功")
                    self._reset_captcha_counters()
                    return

            attempts_since_refresh += 1

            if self._is_verification_expired(attempts_since_refresh):
                if self._captcha_refresh_count < settings.captcha_refresh_retry_limit:
                    self._captcha_refresh_count += 1
                    attempts_since_refresh = 0
                    logger.warning(
                        f"验证页面疑似过期，执行刷新重试 "
                        f"({self._captcha_refresh_count}/{settings.captcha_refresh_retry_limit})"
                    )
                    self._refresh_verification_page()
                    continue

                next_retry_count = self._captcha_retry_count + 1
                self._raise_captcha_failure(
                    CaptchaTimeoutException(
                        timeout_seconds=settings.captcha_solve_timeout_seconds,
                        elapsed_seconds=elapsed_seconds,
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason="验证页面持续过期，超过刷新上限后进入冷却",
                )

            next_retry_count = self._captcha_retry_count + 1
            self._raise_captcha_failure(
                CaptchaAutoSlideFailed(
                    reason="自动滑块未通过验证",
                    retry_count=next_retry_count,
                    max_retries=settings.captcha_max_retries,
                ),
                reason="自动滑块多次失败，进入冷却",
            )

    def _auto_slide_captcha(self) -> bool:
        """自动滑动滑块验证码

        Returns:
            是否成功
        """
        page = self.get_page()

        try:
            # 查找滑块元素
            slider = page.ele('#nc_1_n1z', timeout=3)
            if not slider:
                slider = page.ele('.nc-lang-cnt', timeout=3)

            if not slider:
                logger.debug("未找到滑块元素")
                return False

            # 获取滑块轨道
            track = page.ele('.nc_scale', timeout=3)
            if not track:
                track = page.ele('.nc-container', timeout=3)

            if not track:
                logger.debug("未找到滑块轨道")
                return False

            # 计算滑动距离
            track_width = int(track.rect.get('width', 300))
            if track_width <= 0:
                logger.debug("滑块轨道宽度异常")
                return False

            target_distance = max(track_width - int(round(random.uniform(6, 14))), 50)

            def ease_in_out(progress: float) -> float:
                # 三次平滑曲线：慢起步 -> 中段快 -> 末段减速
                return 3 * progress * progress - 2 * progress * progress * progress

            # 三段式 S 曲线：起步慢 -> 中段加速并超冲 -> 末段回拉修正
            overshoot = 5 + int(round(random.uniform(0, 5)))
            accelerate_target = int(round(ease_in_out(0.35) * target_distance))
            overshoot_target = target_distance + overshoot

            jitter_1 = int(round(random.uniform(-2, 2)))
            jitter_2 = int(round(random.uniform(-2, 2)))
            jitter_3 = int(round(random.uniform(-2, 2)))

            step_1 = max(1, accelerate_target + jitter_1)
            step_2 = max(1, overshoot_target - step_1 + jitter_2)
            step_3 = target_distance - (step_1 + step_2) + jitter_3

            # 对齐最终位移，确保轨迹可控
            total_distance = step_1 + step_2 + step_3
            if total_distance != target_distance:
                step_3 += target_distance - total_distance

            trajectory = [
                (step_1, int(round(random.uniform(-2, 2))), random.uniform(0.05, 0.12)),
                (step_2, int(round(random.uniform(-2, 2))), random.uniform(0.08, 0.2)),
                (step_3, int(round(random.uniform(-2, 2))), random.uniform(0.05, 0.15)),
            ]

            # 模拟人类滑动：加速度变化 + 抖动 + 非恒定停顿 + 超冲回拉
            slider.hover()
            time.sleep(random.uniform(0.15, 0.35))

            # 按住滑块
            page.actions.hold(slider)
            time.sleep(random.uniform(0.12, 0.24))

            for offset_x, offset_y, pause_seconds in trajectory:
                page.actions.move(offset_x, offset_y, duration=random.uniform(0.04, 0.16))
                time.sleep(pause_seconds)

            # 释放滑块
            time.sleep(random.uniform(0.08, 0.16))
            page.actions.release()

            # 等待验证结果
            time.sleep(random.uniform(1.0, 1.8))

            # 检查是否成功
            success_indicators = ['.nc_ok', '.nc-success']
            for indicator in success_indicators:
                if page.ele(indicator, timeout=2):
                    return True

            return False

        except Exception as e:
            logger.debug(f"自动滑块失败: {e}")
            return False

    def navigate_to(self, url: str, wait_load: bool = True) -> bool:
        """导航到指定URL

        Args:
            url: 目标URL
            wait_load: 是否等待页面加载完成

        Returns:
            是否成功
        """
        page = self.get_page()

        try:
            logger.info(f"导航到: {url}")
            page.get(url)

            if wait_load:
                page.wait.load_start()

            # 检查并处理验证码
            self.random_delay(1, 2)
            if self.check_captcha():
                self.handle_captcha()

            return True

        except (CaptchaException, CaptchaCooldownException):
            logger.error("导航期间验证码处理失败")
            raise

        except Exception as e:
            logger.error(f"导航失败: {e}")
            return False

    def scroll_page(self, direction: str = "down", distance: int = 500) -> None:
        """滚动页面

        Args:
            direction: 滚动方向 (up/down)
            distance: 滚动距离（像素）
        """
        page = self.get_page()

        if direction == "down":
            page.scroll.down(distance)
        else:
            page.scroll.up(distance)

        self.random_delay(0.5, 1)

    def scroll_to_bottom(self, step: int = 500, max_scrolls: int = 20) -> None:
        """滚动到页面底部

        Args:
            step: 每次滚动距离
            max_scrolls: 最大滚动次数
        """
        page = self.get_page()

        for i in range(max_scrolls):
            old_height = page.run_js("return document.body.scrollHeight")
            page.scroll.down(step)
            self.random_delay(0.5, 1)
            new_height = page.run_js("return document.body.scrollHeight")

            if new_height == old_height:
                logger.debug(f"已滚动到底部，共滚动 {i + 1} 次")
                break

    def close(self) -> None:
        """关闭浏览器连接（不关闭浏览器本身）"""
        if self.page:
            self.is_connected = False
            logger.info("已断开浏览器连接")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def safe_navigate(self, url: str) -> bool:
        """带重试的安全导航

        Args:
            url: 目标URL

        Returns:
            是否成功
        """
        return self.navigate_to(url)
