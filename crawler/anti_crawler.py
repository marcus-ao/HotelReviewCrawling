"""反爬虫策略模块

使用DrissionPage接管Chrome浏览器，绕过飞猪的自动化检测。
支持滑块验证码自动处理和人工介入机制。
"""
from datetime import datetime
from pathlib import Path
import json
import random
import re
import time
from importlib import import_module
from typing import Any, Optional
from urllib.parse import urlsplit
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
        self._captcha_last_stage: Optional[str] = None
        self._captcha_slide_serial = 0
        self._captcha_refresh_click_stall_count = 0
        self._captcha_click_slide_loop_count = 0
        self._captcha_error_code_streak = 0
        self._captcha_last_error_code: Optional[str] = None

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

    def check_captcha(self, log_detected: bool = True) -> bool:
        """检查是否出现验证码

        Args:
            log_detected: 检测到验证码时是否输出日志

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
                if log_detected:
                    logger.warning(f"检测到验证码: {selector}")
                return True

        return False

    def _reset_captcha_counters(self) -> None:
        """重置验证码重试计数器。"""
        self._captcha_retry_count = 0
        self._captcha_refresh_count = 0
        self._captcha_last_stage = None
        self._captcha_slide_serial = 0
        self._captcha_refresh_click_stall_count = 0
        self._captcha_click_slide_loop_count = 0
        self._captcha_error_code_streak = 0
        self._captcha_last_error_code = None

    def _refresh_prompt_keywords(self) -> list[str]:
        """关键词：表示验证码进入点击重试阶段。"""
        return [
            "点击刷新",
            "请刷新",
            "点击重试",
            "点击框体重试",
            "请重试",
            "重新验证",
            "刷新后重试",
            "验证失败",
            "errorsp",
        ]

    @staticmethod
    def _read_page_html(page: Any) -> str:
        """读取页面 HTML，失败时返回空字符串。"""
        try:
            raw_html = getattr(page, "html", "")
        except Exception:
            return ""
        return raw_html if isinstance(raw_html, str) else str(raw_html or "")

    def _captcha_context_html(self, page: Any) -> str:
        """读取验证码容器上下文 HTML，避免全页关键字误判。"""
        container_selectors = [
            "#baxia-punish",
            "#nocaptcha",
            ".nc-container",
            ".nc_wrapper",
            "#baxia-dialog-content",
            ".J_MIDDLEWARE_FRAME_WIDGET",
        ]

        for selector in container_selectors:
            try:
                container = page.ele(selector, timeout=1)
            except Exception:
                container = None
            if not container:
                continue

            html_part = str(getattr(container, "html", "") or "")
            text_part = str(getattr(container, "text", "") or "")
            context = f"{html_part}\n{text_part}".strip()
            if context:
                return context

        return ""

    def _extract_captcha_error_code(self) -> Optional[str]:
        """提取验证码错误码（如 VP8Ddc）。"""
        if not self.page and not self.is_connected:
            return None

        try:
            page = self.get_page()
        except Exception:
            return None

        html = self._read_page_html(page)
        if not html:
            return None

        patterns = [
            r"error\s*[:=]\s*([A-Za-z0-9_-]{4,})",
            r"errorcode\s*=\s*([A-Za-z0-9_-]{4,})",
        ]
        for pattern in patterns:
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    def _detect_captcha_stage(self) -> str:
        """Detect current captcha stage: refresh_click / slider / captcha_generic / none."""
        page = self.get_page()

        context_html = self._captcha_context_html(page)
        refresh_keywords = [item.lower() for item in self._refresh_prompt_keywords()]
        has_refresh_keyword = bool(context_html) and any(
            keyword in context_html.lower() for keyword in refresh_keywords
        )

        click_stage_selectors = [
            '#nc_1_refresh1',
            '#nc_1__refresh1',
            '.nc-refresh',
            '.nc-container .errloading',
            '.nc_wrapper .errloading',
            '#baxia-punish .errloading',
            '.nc-container .errloading a',
            '.nc_wrapper .errloading a',
            '#baxia-punish .errloading a',
        ]
        for selector in click_stage_selectors:
            try:
                if page.ele(selector, timeout=1):
                    return "refresh_click"
            except Exception:
                continue

        slider_selectors = ['#nc_1_n1z', '.btn_slide', '.nc_iconfont.btn_slide']
        for selector in slider_selectors:
            try:
                if page.ele(selector, timeout=1):
                    return "slider"
            except Exception:
                continue

        if has_refresh_keyword:
            return "refresh_click"

        if self.check_captcha(log_detected=False):
            return "captcha_generic"
        return "none"

    def _save_captcha_debug_artifacts(self, reason: str) -> None:
        """Best-effort save of captcha failure artifacts for local debugging."""
        if not settings.captcha_debug_artifacts_enabled:
            return

        try:
            page = self.get_page()
            artifact_dir = settings.captcha_debug_artifacts_path
            artifact_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            sanitized_reason = re.sub(r"[^A-Za-z0-9_-]+", "_", reason.strip()).strip("_")
            if not sanitized_reason:
                sanitized_reason = "captcha_failure"

            base_name = f"{timestamp}_{sanitized_reason[:80]}"
            screenshot_path = artifact_dir / f"{base_name}.png"
            html_path = artifact_dir / f"{base_name}.html"

            screenshot_saved = False
            screenshot_methods = ["get_screenshot", "screenshot", "save_screenshot"]
            for method_name in screenshot_methods:
                method = getattr(page, method_name, None)
                if not callable(method):
                    continue

                try:
                    method(path=str(screenshot_path))
                    screenshot_saved = True
                    break
                except TypeError:
                    try:
                        method(str(screenshot_path))
                        screenshot_saved = True
                        break
                    except Exception:
                        continue
                except Exception:
                    continue

            html_snapshot = str(getattr(page, "html", "") or "")
            html_path.write_text(html_snapshot, encoding="utf-8")

            logger.info(
                f"已保存验证码调试产物: html={html_path}, screenshot={screenshot_path if screenshot_saved else 'unavailable'}"
            )
        except Exception as exc:
            logger.warning(f"保存验证码调试产物失败: {exc}")

    def _raise_captcha_failure(self, exc: CaptchaException, reason: str) -> None:
        """根据重试上限抛出可重试异常或冷却异常。"""
        self._captcha_retry_count = exc.retry_count
        self._save_captcha_debug_artifacts(reason)
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
            "验证超时",
            "请刷新",
            "页面已失效",
            "重新验证",
            "点击刷新",
            "点击重试",
            "请重试",
            "刷新后重试",
        ]

        try:
            html = page.html or ""
            if any(keyword in html for keyword in expiry_keywords):
                return True
        except Exception:
            logger.debug("读取验证页内容失败，跳过关键字检测")

        # 启发式判断：同一页面多次自动滑块失败，且验证码仍在。
        return attempts_since_refresh >= 2 and self.check_captcha(log_detected=False)

    def _refresh_verification_page(self) -> bool:
        """刷新验证页并验证是否实际生效。"""
        page = self.get_page()
        previous_stage = self._detect_captcha_stage()
        previous_signature = self._page_state_signature()
        current_url = str(getattr(page, "url", "") or "").strip()

        try:
            page.refresh()
            page.wait.load_start()
        except Exception as exc:
            logger.warning(f"页面刷新调用异常: {exc}")
        self.random_delay(0.6, 1.4)

        current_signature = self._page_state_signature()
        current_stage = self._detect_captcha_stage()
        if self._is_refresh_effective(
            previous_signature=previous_signature,
            current_signature=current_signature,
            previous_stage=previous_stage,
            current_stage=current_stage,
        ):
            return True

        logger.warning(
            "页面普通刷新疑似未生效，执行强制硬刷新(CTRL+F5语义) "
            f"(stage: {previous_stage} -> {current_stage})"
        )

        hard_reload_executed = self._hard_reload_ignore_cache()
        hard_signature = self._page_state_signature()
        hard_stage = self._detect_captcha_stage()
        if hard_reload_executed and self._is_refresh_effective(
            previous_signature=previous_signature,
            current_signature=hard_signature,
            previous_stage=previous_stage,
            current_stage=hard_stage,
        ):
            logger.info(
                "强制硬刷新生效 "
                f"(stage: {previous_stage} -> {hard_stage})"
            )
            return True

        logger.warning(
            "页面刷新疑似未生效，执行导航级回退刷新 "
            f"(stage: {previous_stage} -> {hard_stage})"
        )

        try:
            page.run_cdp("Page.stopLoading")
        except Exception:
            logger.debug("执行 Page.stopLoading 失败，继续尝试 URL 级回退")

        if current_url:
            fallback_url = self._append_cache_buster(current_url)
            try:
                page.get(fallback_url)
                page.wait.load_start()
            except Exception as exc:
                logger.warning(f"导航级回退刷新失败: {exc}")
            self.random_delay(0.6, 1.4)

        fallback_signature = self._page_state_signature()
        fallback_stage = self._detect_captcha_stage()
        if self._is_refresh_effective(
            previous_signature=previous_signature,
            current_signature=fallback_signature,
            previous_stage=previous_stage,
            current_stage=fallback_stage,
        ):
            logger.info(
                "导航级回退刷新生效 "
                f"(stage: {previous_stage} -> {fallback_stage})"
            )
            return True

        logger.warning(
            "页面刷新与回退刷新均未检测到状态推进 "
            f"(stage: {previous_stage} -> {fallback_stage})"
        )
        return False

    def _hard_reload_ignore_cache(self) -> bool:
        """执行接近 CTRL+F5 的硬刷新（忽略缓存）。"""
        page = self.get_page()
        hard_reload_executed = False
        current_url = str(getattr(page, "url", "") or "").strip()

        origin = ""
        if current_url:
            try:
                parsed = urlsplit(current_url)
                if parsed.scheme and parsed.netloc:
                    origin = f"{parsed.scheme}://{parsed.netloc}"
            except Exception:
                origin = ""

        try:
            page.run_cdp("Network.enable")
        except Exception:
            logger.debug("启用 Network 域失败，继续尝试硬刷新")

        try:
            page.run_cdp("Network.setCacheDisabled", cacheDisabled=True)
        except Exception:
            logger.debug("设置 cacheDisabled=True 失败，继续尝试硬刷新")

        try:
            page.run_cdp("Network.clearBrowserCache")
        except Exception:
            logger.debug("清理浏览器缓存失败，继续尝试硬刷新")

        try:
            page.run_cdp("Network.setBypassServiceWorker", bypass=True)
        except Exception:
            logger.debug("设置 bypassServiceWorker=True 失败，继续尝试硬刷新")

        if origin:
            try:
                page.run_cdp(
                    "Storage.clearDataForOrigin",
                    origin=origin,
                    storageTypes="cache_storage,service_workers",
                )
            except Exception:
                logger.debug("清理站点缓存存储失败，继续尝试硬刷新")

        try:
            page.run_cdp("Page.reload", ignoreCache=True)
            hard_reload_executed = True
        except TypeError:
            try:
                page.run_cdp("Page.reload", {"ignoreCache": True})
                hard_reload_executed = True
            except Exception as exc:
                logger.debug(f"CDP Page.reload(ignoreCache) 参数回退失败: {exc}")
        except Exception as exc:
            logger.debug(f"CDP Page.reload(ignoreCache) 失败: {exc}")

        if not hard_reload_executed:
            try:
                page.run_js("window.location.reload(true)")
                hard_reload_executed = True
            except Exception as exc:
                logger.warning(f"JS 强制刷新失败: {exc}")

        if hard_reload_executed:
            try:
                page.wait.load_start()
            except Exception:
                logger.debug("硬刷新后等待加载信号失败")
            self.random_delay(0.8, 1.6)

        try:
            page.run_cdp("Network.setCacheDisabled", cacheDisabled=False)
        except Exception:
            logger.debug("恢复 cacheDisabled=False 失败")

        try:
            page.run_cdp("Network.setBypassServiceWorker", bypass=False)
        except Exception:
            logger.debug("恢复 bypassServiceWorker=False 失败")

        return hard_reload_executed

    def _page_state_signature(self) -> str:
        """生成页面状态签名，用于判断刷新是否改变页面状态。"""
        page = self.get_page()
        try:
            html = str(getattr(page, "html", "") or "")
        except Exception:
            return "html_error"

        if not html:
            return "html_empty"

        return f"{len(html)}:{hash(html[:4000])}:{hash(html[-4000:])}"

    @staticmethod
    def _is_refresh_effective(
        previous_signature: str,
        current_signature: str,
        previous_stage: str,
        current_stage: str,
    ) -> bool:
        """判断刷新后是否有状态推进。"""
        if previous_stage == "refresh_click" and current_stage == "refresh_click":
            return False
        if current_stage in ("slider", "none") and current_stage != previous_stage:
            return True
        return previous_signature != current_signature or previous_stage != current_stage

    @staticmethod
    def _append_cache_buster(url: str) -> str:
        """在 URL 上附加时间戳参数，避免刷新命中缓存。"""
        delimiter = "&" if "?" in url else "?"
        return f"{url}{delimiter}_captcha_ts={int(time.time() * 1000)}"

    def _click_selector_center(self, selector: str) -> bool:
        """Best-effort click selector center, prefer native click first."""
        page = self.get_page()
        try:
            target = page.ele(selector, timeout=1)
            if target:
                try:
                    target.click()
                    return True
                except Exception:
                    try:
                        target.click(by_js=True)
                        return True
                    except Exception:
                        pass
        except Exception:
            pass

        selector_json = json.dumps(selector, ensure_ascii=False)

        script = f"""
                const selector = {selector_json};
                const el = document.querySelector(selector);
                if (!el) return {{ ok: false, reason: 'not_found' }};

                const rect = el.getBoundingClientRect();
                if (!rect || rect.width <= 1 || rect.height <= 1) {{
                    return {{ ok: false, reason: 'rect_invalid' }};
                }}

                const cx = rect.left + rect.width / 2;
                const cy = rect.top + rect.height / 2;
                const target = document.elementFromPoint(cx, cy) || el;
                const events = ['mouseover', 'mousedown', 'mouseup', 'click'];

                for (const type of events) {{
                    target.dispatchEvent(new MouseEvent(type, {{
                        view: window,
                        bubbles: true,
                        cancelable: true,
                        clientX: cx,
                        clientY: cy,
                        button: 0,
                    }}));
                }}

                if (typeof el.click === 'function') {{
                    el.click();
                }}

                return {{
                    ok: true,
                    cx: Math.round(cx),
                    cy: Math.round(cy),
                    text: (el.innerText || el.textContent || '').trim(),
                }};
                """

        try:
            result = page.run_js(script)
            if isinstance(result, dict):
                return bool(result.get("ok"))
            return bool(result)
        except Exception:
            return False

    def _did_click_refresh_take_effect(self) -> bool:
        """Verify click-refresh actually transitions captcha stage."""
        for _ in range(8):
            self.random_delay(0.15, 0.3)
            stage = self._detect_captcha_stage()
            if stage in ("slider", "none"):
                return True
        return False

    def _captcha_attempt_interval_delay(self, phase: str = "default") -> None:
        """Delay between adjacent captcha operations.

        Keep intervals short and non-deterministic for repeated verification loops.
        """
        ranges = {
            "click_followup": (0.22, 0.70),
            "slide_followup": (0.35, 1.05),
            "retry_backoff": (0.30, 1.00),
            "after_refresh": (0.45, 1.25),
            "default": (0.28, 0.90),
        }
        low, high = ranges.get(phase, ranges["default"])

        # Add slight adaptive variance when loops repeat.
        loop_factor = 1.0 + min(self._captcha_click_slide_loop_count, 3) * 0.08
        self.random_delay(low, high * loop_factor)

    def _click_verification_refresh(self) -> bool:
        """点击验证码区域内的刷新入口，触发新滑块加载。"""
        page = self.get_page()

        explicit_refresh_selectors = [
            '#nc_1_refresh1',
            '#nc_1__refresh1',
            '.nc-refresh',
            '.nc-container .errloading a',
            '.nc_wrapper .errloading a',
            '#baxia-punish .errloading a',
        ]

        refresh_text_keywords = self._refresh_prompt_keywords()

        page_html = self._read_page_html(page)
        context_html = self._captcha_context_html(page)
        prompt_detected = any(keyword in page_html for keyword in refresh_text_keywords) or any(
            keyword in context_html for keyword in refresh_text_keywords
        )

        for selector in explicit_refresh_selectors:
            try:
                if self._click_selector_center(selector):
                    if self._did_click_refresh_take_effect():
                        logger.info(f"点击验证刷新区域: {selector}")
                        self.random_delay(0.8, 1.5)
                        return True
                    logger.debug(f"点击后未触发状态切换，继续尝试其他区域: {selector}")
            except Exception:
                continue

        if prompt_detected:
            rectangle_retry_selectors = [
                '#nc_1_n1t',
                '.nc-container .nc_scale',
                '.nc_wrapper .nc_scale',
                '.nc-container .nc-lang-cnt',
                '.nc_wrapper .nc-lang-cnt',
                '.nc-container .errloading',
                '.nc_wrapper .errloading',
                '#baxia-punish .errloading',
            ]
            for selector in rectangle_retry_selectors:
                try:
                    if self._click_selector_center(selector):
                        if self._did_click_refresh_take_effect():
                            logger.info(f"点击验证重试框体区域: {selector}")
                            self.random_delay(0.8, 1.5)
                            return True
                        logger.debug(f"点击框体后未触发状态切换，继续尝试: {selector}")
                except Exception:
                    continue

        text_refresh_selectors = [
            '.nc-container .errloading',
            '.nc_wrapper .errloading',
            '#baxia-punish .errloading',
            '.nc-container .nc-lang-cnt',
            '.nc_wrapper .nc-lang-cnt',
            '.nc-container .nc_scale',
            '.nc_wrapper .nc_scale',
        ]
        for selector in text_refresh_selectors:
            try:
                refresh_ele = page.ele(selector, timeout=1)
                if not refresh_ele:
                    continue

                text_content = str(getattr(refresh_ele, 'text', '') or '')
                if not any(keyword in text_content for keyword in refresh_text_keywords):
                    continue

                try:
                    refresh_ele.click(by_js=True)
                except Exception:
                    refresh_ele.click()

                if self._did_click_refresh_take_effect():
                    logger.info(f"点击验证刷新文本区域: {selector}")
                    self.random_delay(0.8, 1.5)
                    return True
                logger.debug(f"文本区域点击后未触发状态切换: {selector}")
            except Exception:
                continue

        try:
            clicked = page.run_js(
                """
                const root = document.querySelector('.nc-container, .nc_wrapper, #baxia-dialog-content, .J_MIDDLEWARE_FRAME_WIDGET') || document;
                const keywords = ['点击刷新', '请刷新', '点击重试', '点击框体重试', '请重试', '重新验证', '刷新后重试', '验证失败'];
                const candidates = root.querySelectorAll('a, button, span, div');
                for (const el of candidates) {
                    const text = (el.innerText || '').trim();
                    if (!text) continue;
                    if (keywords.some(k => text.includes(k))) {
                        el.click();
                        return text;
                    }
                }
                return '';
                """
            )
            if clicked:
                if self._did_click_refresh_take_effect():
                    logger.info(f"点击验证刷新文本区域: {clicked}")
                    self.random_delay(0.8, 1.5)
                    return True
                logger.debug(f"JS文本点击后未触发状态切换: {clicked}")
        except Exception:
            pass

        return False

    def handle_captcha(self, auto_retry: bool = True) -> None:
        """处理验证码

        Args:
            auto_retry: 是否自动重试滑块

        Raises:
            CaptchaAutoSlideFailed: 自动滑块失败（可重试）
            CaptchaTimeoutException: 验证码处理超时或验证页过期（可重试）
            CaptchaCooldownException: 达到最大重试次数后进入冷却（终止）
        """
        if not self.check_captcha(log_detected=False):
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

        session_start_time = time.time()
        no_progress_start_time = session_start_time
        attempts_since_refresh = 0
        total_actions = 0
        max_total_actions = max(
            settings.captcha_max_retries * max(settings.captcha_refresh_retry_limit + 2, 4),
            12,
        )
        max_refresh_click_stall = 2
        max_click_slide_loops = max(1, settings.captcha_click_slide_refresh_threshold)

        while self.check_captcha(log_detected=False):
            now = time.time()
            session_elapsed_seconds = int(now - session_start_time)
            no_progress_elapsed_seconds = int(now - no_progress_start_time)

            if no_progress_elapsed_seconds >= settings.captcha_solve_timeout_seconds:
                next_retry_count = self._captcha_retry_count + 1
                self._raise_captcha_failure(
                    CaptchaTimeoutException(
                        timeout_seconds=settings.captcha_solve_timeout_seconds,
                        elapsed_seconds=no_progress_elapsed_seconds,
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason=(
                        "验证码处理无进展超时，进入冷却 "
                        f"(本轮会话耗时: {session_elapsed_seconds}s)"
                    ),
                )

            if total_actions >= max_total_actions:
                next_retry_count = self._captcha_retry_count + 1
                self._raise_captcha_failure(
                    CaptchaTimeoutException(
                        timeout_seconds=settings.captcha_solve_timeout_seconds,
                        elapsed_seconds=session_elapsed_seconds,
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason="验证码阶段切换过多，超过处理上限后进入冷却",
                )

            stage = self._detect_captcha_stage()

            if stage == "refresh_click":
                error_code = self._extract_captcha_error_code()
                if error_code:
                    if error_code == self._captcha_last_error_code:
                        self._captcha_error_code_streak += 1
                    else:
                        self._captcha_last_error_code = error_code
                        self._captcha_error_code_streak = 1

                    if self._captcha_error_code_streak >= 3:
                        next_retry_count = self._captcha_retry_count + 1
                        self._raise_captcha_failure(
                            CaptchaTimeoutException(
                                timeout_seconds=settings.captcha_solve_timeout_seconds,
                                elapsed_seconds=session_elapsed_seconds,
                                retry_count=next_retry_count,
                                max_retries=settings.captcha_max_retries,
                            ),
                            reason=(
                                f"验证码错误码 {error_code} 持续出现，疑似陷入验证死循环，进入冷却"
                            ),
                        )
                else:
                    self._captcha_last_error_code = None
                    self._captcha_error_code_streak = 0
            else:
                self._captcha_last_error_code = None
                self._captcha_error_code_streak = 0

            if self._captcha_last_stage and stage != self._captcha_last_stage:
                attempts_since_refresh = 0
                if stage == "refresh_click":
                    self._captcha_retry_count = 0
                logger.debug(f"验证码阶段切换: {self._captcha_last_stage} -> {stage}")
                no_progress_start_time = time.time()
            self._captcha_last_stage = stage

            if stage != "refresh_click":
                self._captcha_refresh_click_stall_count = 0

            if stage == "refresh_click":
                total_actions += 1
                if self._click_verification_refresh():
                    attempts_since_refresh = 0
                    self._captcha_retry_count = 0
                    self._captcha_refresh_click_stall_count += 1
                    no_progress_start_time = time.time()

                    if self._captcha_refresh_click_stall_count >= max_refresh_click_stall:
                        if self._captcha_refresh_count < settings.captcha_refresh_retry_limit:
                            self._captcha_refresh_count += 1
                            logger.warning(
                                f"点击重试阶段连续停滞，执行页面刷新重试 "
                                f"({self._captcha_refresh_count}/{settings.captcha_refresh_retry_limit})"
                            )
                            refresh_ok = self._refresh_verification_page()
                            self._captcha_last_stage = None
                            if refresh_ok:
                                self._captcha_refresh_click_stall_count = 0
                                self._captcha_click_slide_loop_count = 0
                                self._captcha_retry_count = 0
                                attempts_since_refresh = 0
                                no_progress_start_time = time.time()
                            else:
                                self._captcha_refresh_click_stall_count = max_refresh_click_stall
                            self._captcha_attempt_interval_delay("after_refresh")
                            continue

                        next_retry_count = self._captcha_retry_count + 1
                        self._raise_captcha_failure(
                            CaptchaTimeoutException(
                                timeout_seconds=settings.captcha_solve_timeout_seconds,
                                elapsed_seconds=session_elapsed_seconds,
                                retry_count=next_retry_count,
                                max_retries=settings.captcha_max_retries,
                            ),
                            reason="点击重试阶段持续停滞，超过刷新上限后进入冷却",
                        )

                    logger.info("点击重试成功，等待新滑块阶段")
                    self._captcha_attempt_interval_delay("click_followup")
                    continue

                if self._captcha_refresh_count < settings.captcha_refresh_retry_limit:
                    self._captcha_refresh_count += 1
                    attempts_since_refresh = 0
                    logger.warning(
                        f"点击重试失败，执行页面刷新重试 "
                        f"({self._captcha_refresh_count}/{settings.captcha_refresh_retry_limit})"
                    )
                    refresh_ok = self._refresh_verification_page()
                    if refresh_ok:
                        no_progress_start_time = time.time()
                    continue

                next_retry_count = self._captcha_retry_count + 1
                self._raise_captcha_failure(
                    CaptchaTimeoutException(
                        timeout_seconds=settings.captcha_solve_timeout_seconds,
                        elapsed_seconds=session_elapsed_seconds,
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason="点击重试阶段持续失败，超过刷新上限后进入冷却",
                )

            success = self._auto_slide_captcha()
            total_actions += 1
            self._captcha_attempt_interval_delay("slide_followup")
            attempts_since_refresh += 1

            captcha_still_present = self.check_captcha(log_detected=False)
            if self._is_verification_expired(attempts_since_refresh):
                clicked_refresh_zone = self._click_verification_refresh()
                if clicked_refresh_zone:
                    attempts_since_refresh = 0
                    self._captcha_retry_count = 0
                    self._captcha_click_slide_loop_count = 0
                    no_progress_start_time = time.time()
                    logger.info("过期后点击重试成功，继续进入滑块阶段")
                    self._captcha_attempt_interval_delay("click_followup")
                    continue

                if self._captcha_refresh_count < settings.captcha_refresh_retry_limit:
                    self._captcha_refresh_count += 1
                    attempts_since_refresh = 0
                    logger.warning(
                        f"验证页面疑似过期，执行刷新重试 "
                        f"({self._captcha_refresh_count}/{settings.captcha_refresh_retry_limit})"
                    )

                    refresh_ok = self._refresh_verification_page()
                    if refresh_ok:
                        no_progress_start_time = time.time()

                    continue

                next_retry_count = self._captcha_retry_count + 1
                self._raise_captcha_failure(
                    CaptchaTimeoutException(
                        timeout_seconds=settings.captcha_solve_timeout_seconds,
                        elapsed_seconds=session_elapsed_seconds,
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason="验证页面持续过期，超过刷新上限后进入冷却",
                )

            if not captcha_still_present:
                if success:
                    logger.info("自动滑块验证成功")
                else:
                    logger.info("验证码元素已消失，判定验证通过")
                self._reset_captcha_counters()
                return

            post_stage = self._detect_captcha_stage()
            if post_stage == "refresh_click":
                self._captcha_last_stage = post_stage
                self._captcha_click_slide_loop_count += 1

                if self._captcha_click_slide_loop_count >= max_click_slide_loops:
                    if self._captcha_refresh_count < settings.captcha_refresh_retry_limit:
                        self._captcha_refresh_count += 1
                        logger.warning(
                            f"点击->滑块循环连续失败，执行页面刷新重试 "
                            f"({self._captcha_refresh_count}/{settings.captcha_refresh_retry_limit})"
                        )
                        refresh_ok = self._refresh_verification_page()
                        self._captcha_last_stage = None
                        if refresh_ok:
                            self._captcha_click_slide_loop_count = 0
                            self._captcha_refresh_click_stall_count = 0
                            self._captcha_retry_count = 0
                            attempts_since_refresh = 0
                            no_progress_start_time = time.time()
                        else:
                            self._captcha_click_slide_loop_count = max_click_slide_loops
                        self._captcha_attempt_interval_delay("after_refresh")
                        continue

                    next_retry_count = self._captcha_retry_count + 1
                    self._raise_captcha_failure(
                        CaptchaTimeoutException(
                            timeout_seconds=settings.captcha_solve_timeout_seconds,
                            elapsed_seconds=session_elapsed_seconds,
                            retry_count=next_retry_count,
                            max_retries=settings.captcha_max_retries,
                        ),
                        reason="点击->滑块循环持续失败，超过刷新上限后进入冷却",
                    )

                logger.info("滑块后进入点击重试阶段，优先点击重试")
                continue

            self._captcha_click_slide_loop_count = 0

            next_retry_count = self._captcha_retry_count + 1
            if next_retry_count >= settings.captcha_max_retries:
                self._raise_captcha_failure(
                    CaptchaAutoSlideFailed(
                        reason="自动滑块未通过验证",
                        retry_count=next_retry_count,
                        max_retries=settings.captcha_max_retries,
                    ),
                    reason="自动滑块多次失败，进入冷却",
                )

            self._captcha_retry_count = next_retry_count
            logger.warning(
                f"自动滑块未通过验证，准备重试 "
                f"({next_retry_count + 1}/{settings.captcha_max_retries})"
            )
            self._captcha_attempt_interval_delay("retry_backoff")

    def _auto_slide_captcha(self) -> bool:
        """自动滑动滑块验证码

        Returns:
            是否成功
        """
        page = self.get_page()

        try:
            self._captcha_slide_serial += 1
            motion_profiles = [
                {
                    "name": "steady",
                    "speed_scale": 1.00,
                    "pause_scale": 1.00,
                    "pivot": 0.35,
                    "overshoot_bias": 0,
                },
                {
                    "name": "cautious",
                    "speed_scale": 0.86,
                    "pause_scale": 1.22,
                    "pivot": 0.30,
                    "overshoot_bias": -1,
                },
                {
                    "name": "decisive",
                    "speed_scale": 1.18,
                    "pause_scale": 0.82,
                    "pivot": 0.40,
                    "overshoot_bias": 1,
                },
                {
                    "name": "micro_corrective",
                    "speed_scale": 0.94,
                    "pause_scale": 1.10,
                    "pivot": 0.33,
                    "overshoot_bias": 0,
                },
            ]
            profile = motion_profiles[(self._captcha_slide_serial - 1) % len(motion_profiles)]

            # 查找滑块元素
            slider = page.ele('#nc_1_n1z', timeout=3)
            if not slider:
                slider = page.ele('.btn_slide', timeout=3)
            if not slider:
                slider = page.ele('.nc_iconfont.btn_slide', timeout=3)

            if not slider:
                logger.debug("未找到滑块元素")
                return False

            # 获取滑块轨道
            track = page.ele('#nc_1_n1t', timeout=3)
            if not track:
                track = page.ele('.nc_scale', timeout=3)
            if not track:
                track = page.ele('.nc-container', timeout=3)

            if not track:
                logger.debug("未找到滑块轨道")
                return False

            # 计算滑动距离
            track_rect = track.rect if isinstance(track.rect, dict) else {}
            slider_rect = slider.rect if isinstance(getattr(slider, 'rect', None), dict) else {}

            track_width = int(track_rect.get('width', 300))
            slider_width = int(slider_rect.get('width', 40))
            if track_width <= 0:
                logger.debug("滑块轨道宽度异常")
                return False
            if slider_width <= 0:
                slider_width = 40

            target_distance = max(
                track_width - slider_width - int(round(random.uniform(2, 8))),
                40,
            )

            def ease_in_out(progress: float) -> float:
                # 三次平滑曲线：慢起步 -> 中段快 -> 末段减速
                return 3 * progress * progress - 2 * progress * progress * progress

            def clamp(value: float, minimum: float, maximum: float) -> float:
                return max(minimum, min(value, maximum))

            # 三段式 S 曲线：起步慢 -> 中段加速并超冲 -> 末段回拉修正
            overshoot = 5 + profile["overshoot_bias"] + int(round(random.uniform(0, 5)))
            if overshoot < 3:
                overshoot = 3

            accelerate_target = int(round(ease_in_out(profile["pivot"]) * target_distance))
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
            time.sleep(clamp(random.uniform(0.15, 0.35) * profile["pause_scale"], 0.1, 0.6))

            # 按住滑块
            page.actions.hold(slider)
            time.sleep(clamp(random.uniform(0.12, 0.24) * profile["pause_scale"], 0.08, 0.5))

            for offset_x, offset_y, pause_seconds in trajectory:
                move_duration = clamp(
                    random.uniform(0.04, 0.16) / profile["speed_scale"],
                    0.03,
                    0.24,
                )
                page.actions.move(offset_x, offset_y, duration=move_duration)
                time.sleep(clamp(pause_seconds * profile["pause_scale"], 0.03, 0.30))

            # 释放滑块
            time.sleep(clamp(random.uniform(0.08, 0.16) * profile["pause_scale"], 0.05, 0.28))
            page.actions.release()

            # 等待验证结果
            time.sleep(clamp(random.uniform(1.0, 1.8) * profile["pause_scale"], 0.7, 2.4))

            logger.debug(
                f"滑块轨迹配置: serial={self._captcha_slide_serial}, "
                f"profile={profile['name']}, speed={profile['speed_scale']:.2f}, "
                f"pause={profile['pause_scale']:.2f}"
            )

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
