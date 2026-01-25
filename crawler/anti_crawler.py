"""反爬虫策略模块

使用DrissionPage接管Chrome浏览器，绕过飞猪的自动化检测。
支持滑块验证码自动处理和人工介入机制。
"""
import random
import time
from typing import Optional

from DrissionPage import ChromiumPage, ChromiumOptions
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from utils.logger import get_logger

logger = get_logger("anti_crawler")


class AntiCrawler:
    """反爬虫策略类"""

    def __init__(self):
        self.page: Optional[ChromiumPage] = None
        self.is_connected = False

    def init_browser(self) -> ChromiumPage:
        """初始化浏览器（接管已打开的Chrome）

        需要先手动启动Chrome:
        chrome.exe --remote-debugging-port=9222 --user-data-dir="C:/selenium/automation_profile"

        Returns:
            ChromiumPage实例
        """
        try:
            # 连接到已打开的Chrome浏览器
            self.page = ChromiumPage(addr_or_opts=settings.chrome_address)
            self.is_connected = True
            logger.info(f"成功连接到Chrome浏览器: {settings.chrome_address}")
            return self.page
        except Exception as e:
            logger.error(f"连接Chrome浏览器失败: {e}")
            logger.info("请确保已手动启动Chrome并开启远程调试端口:")
            logger.info(f'chrome.exe --remote-debugging-port={settings.chrome_debug_port} '
                       f'--user-data-dir="{settings.chrome_user_data_dir}"')
            raise

    def get_page(self) -> ChromiumPage:
        """获取页面实例"""
        if not self.page or not self.is_connected:
            return self.init_browser()
        return self.page

    def random_delay(self, min_delay: float = None, max_delay: float = None) -> None:
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

    def handle_captcha(self, auto_retry: bool = True) -> bool:
        """处理验证码

        Args:
            auto_retry: 是否自动重试滑块

        Returns:
            是否成功处理
        """
        if not self.check_captcha():
            return True

        page = self.get_page()

        if auto_retry:
            # 尝试自动滑动
            success = self._auto_slide_captcha()
            if success:
                logger.info("自动滑块验证成功")
                return True

        # 自动处理失败，等待人工介入
        logger.warning("=" * 50)
        logger.warning("需要人工处理验证码！")
        logger.warning("请在浏览器中手动完成验证，然后按Enter继续...")
        logger.warning("=" * 50)

        input("按Enter键继续...")

        # 验证是否成功
        self.random_delay(1, 2)
        if not self.check_captcha():
            logger.info("人工验证成功")
            return True

        logger.error("验证码处理失败")
        return False

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
            track_width = track.rect.get('width', 300)

            # 模拟人类滑动：先快后慢，带有轻微抖动
            slider.hover()
            time.sleep(0.3)

            # 按住滑块
            page.actions.hold(slider)
            time.sleep(0.2)

            # 分段滑动
            current_x = 0
            while current_x < track_width:
                # 随机步长
                step = random.randint(20, 50)
                if current_x + step > track_width:
                    step = track_width - current_x

                # 添加轻微的Y轴抖动
                y_offset = random.randint(-2, 2)

                page.actions.move(step, y_offset, duration=random.uniform(0.05, 0.15))
                current_x += step

                # 随机暂停
                if random.random() < 0.3:
                    time.sleep(random.uniform(0.05, 0.1))

            # 释放滑块
            time.sleep(0.1)
            page.actions.release()

            # 等待验证结果
            time.sleep(2)

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
