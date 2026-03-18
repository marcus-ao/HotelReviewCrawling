"""Pytest unit tests for anti-captcha primitives.

This suite is fully mock-based:
- no live browser
- no real DrissionPage dependency
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

from config.settings import Settings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_module_from_path(module_name: str, relative_path: str, injected_modules=None):
    module_path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module spec for {module_path}")

    module = importlib.util.module_from_spec(spec)
    if injected_modules:
        with patch.dict(sys.modules, injected_modules, clear=False):
            spec.loader.exec_module(module)
    else:
        spec.loader.exec_module(module)
    return module


def _load_exceptions_module():
    return _load_module_from_path(
        "crawler_exceptions_under_test",
        "crawler/exceptions.py",
    )


def _load_anti_crawler_module():
    exceptions_module = _load_exceptions_module()

    fake_crawler_pkg = ModuleType("crawler")
    fake_crawler_pkg.__path__ = []
    setattr(fake_crawler_pkg, "exceptions", exceptions_module)

    fake_drission = ModuleType("DrissionPage")
    setattr(fake_drission, "ChromiumPage", type("ChromiumPage", (), {}))
    setattr(fake_drission, "ChromiumOptions", type("ChromiumOptions", (), {}))

    fake_logger = Mock()
    fake_logger.bind.return_value = fake_logger

    fake_logger_module = ModuleType("utils.logger")
    setattr(fake_logger_module, "get_logger", Mock(return_value=fake_logger))

    fake_utils_pkg = ModuleType("utils")
    fake_utils_pkg.__path__ = []
    setattr(fake_utils_pkg, "logger", fake_logger_module)

    return _load_module_from_path(
        "anti_crawler_under_test",
        "crawler/anti_crawler.py",
        injected_modules={
            "DrissionPage": fake_drission,
            "crawler": fake_crawler_pkg,
            "crawler.exceptions": exceptions_module,
            "utils": fake_utils_pkg,
            "utils.logger": fake_logger_module,
        },
    )


def test_retry_ceiling_for_captcha_exception():
    exceptions_module = _load_exceptions_module()

    retryable = exceptions_module.CaptchaException("captcha failed", retry_count=2, max_retries=3)
    exhausted = exceptions_module.CaptchaException("captcha failed", retry_count=3, max_retries=3)

    assert retryable.should_retry() is True
    assert exhausted.should_retry() is False
    assert "重试 3/3" in str(exhausted)


def test_cooldown_trigger_after_retry_exhaustion():
    exceptions_module = _load_exceptions_module()

    max_retries = 3
    retry_count = 0
    cooldown_error = None

    while True:
        exc = exceptions_module.CaptchaException(
            "captcha failed",
            retry_count=retry_count,
            max_retries=max_retries,
        )
        if not exc.should_retry():
            cooldown_error = exceptions_module.CaptchaCooldownException(
                cooldown_seconds=180,
                reason="captcha failures exceeded ceiling",
            )
            break
        retry_count += 1

    assert retry_count == max_retries
    assert cooldown_error is not None
    assert cooldown_error.should_retry() is False
    assert "冷却时长: 180s" in str(cooldown_error)


def test_timeout_exception_contains_context_values():
    exceptions_module = _load_exceptions_module()

    exc = exceptions_module.CaptchaTimeoutException(
        timeout_seconds=120,
        elapsed_seconds=133,
        retry_count=1,
        max_retries=3,
    )

    assert exc.timeout_seconds == 120
    assert exc.elapsed_seconds == 133
    assert "已等待 133s" in str(exc)
    assert "超时设置 120s" in str(exc)
    assert "重试 1/3" in str(exc)


def test_check_captcha_detection_flow_hits_known_selector():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    page = Mock()

    def ele_side_effect(selector, timeout=1):
        if selector == ".nc-container":
            return object()
        return None

    page.ele.side_effect = ele_side_effect
    anti.get_page = Mock(return_value=page)

    assert anti.check_captcha() is True
    queried_selectors = [call.args[0] for call in page.ele.call_args_list]
    assert '#baxia-dialog-content' in queried_selectors
    assert '.nc-container' in queried_selectors


def test_check_captcha_detection_flow_returns_false_when_absent():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    page = Mock()
    page.ele.return_value = None
    anti.get_page = Mock(return_value=page)

    assert anti.check_captcha() is False
    queried_selectors = [call.args[0] for call in page.ele.call_args_list]
    assert '#baxia-dialog-content' in queried_selectors
    assert '#nc_1_n1z' in queried_selectors


def test_auto_slide_generates_segmented_movement_flow():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    slider = Mock()
    slider.rect = {"width": 30}
    track = Mock()
    track.rect = {"width": 90}

    page = Mock()
    page.actions = Mock()

    def ele_side_effect(selector, timeout=1):
        if selector in ("#nc_1_n1z", ".btn_slide", ".nc_iconfont.btn_slide"):
            return slider
        if selector in ("#nc_1_n1t", ".nc_scale", ".nc-container"):
            return track
        if selector == ".nc_ok":
            return object()
        return None

    page.ele.side_effect = ele_side_effect
    anti.get_page = Mock(return_value=page)

    with patch.object(anti_module.time, "sleep", return_value=None), patch.object(
        anti_module.random,
        "randint",
        side_effect=[30, 0, 30, 0, 30, 0],
    ), patch.object(anti_module.random, "uniform", return_value=0.1), patch.object(
        anti_module.random,
        "random",
        return_value=1.0,
    ):
        assert anti._auto_slide_captcha() is True

    page.actions.hold.assert_called_once_with(slider)
    page.actions.release.assert_called_once()
    assert page.actions.move.call_count == 3
    total_moved = sum(call.args[0] for call in page.actions.move.call_args_list)
    assert total_moved == 60


def test_auto_slide_uses_variable_speed_profiles_across_cycles():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    slider = Mock()
    slider.rect = {"width": 30}
    track = Mock()
    track.rect = {"width": 90}

    page = Mock()
    page.actions = Mock()

    def ele_side_effect(selector, timeout=1):
        if selector in ("#nc_1_n1z", ".btn_slide", ".nc_iconfont.btn_slide"):
            return slider
        if selector in ("#nc_1_n1t", ".nc_scale", ".nc-container"):
            return track
        if selector == ".nc_ok":
            return object()
        return None

    page.ele.side_effect = ele_side_effect
    anti.get_page = Mock(return_value=page)

    def uniform_mid(low, high):
        return (low + high) / 2

    with patch.object(anti_module.time, "sleep", return_value=None), patch.object(
        anti_module.random,
        "uniform",
        side_effect=uniform_mid,
    ):
        assert anti._auto_slide_captcha() is True
        assert anti._auto_slide_captcha() is True

    assert page.actions.move.call_count == 6
    first_cycle_duration = page.actions.move.call_args_list[0].kwargs["duration"]
    second_cycle_duration = page.actions.move.call_args_list[3].kwargs["duration"]
    assert first_cycle_duration != second_cycle_duration


def test_handle_captcha_retries_in_process_until_success():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    anti.check_captcha = Mock(side_effect=[True, True, True, True, False])
    anti._detect_captcha_stage = Mock(return_value="slider")
    anti._auto_slide_captcha = Mock(side_effect=[False, True])
    anti._is_verification_expired = Mock(return_value=False)
    anti.random_delay = Mock()

    anti.handle_captcha(auto_retry=True)

    assert anti._auto_slide_captcha.call_count == 2
    assert anti._captcha_retry_count == 0
    assert anti._captcha_refresh_count == 0


def test_handle_captcha_passes_when_captcha_disappears_without_indicator():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    anti.check_captcha = Mock(side_effect=[True, True, False])
    anti._detect_captcha_stage = Mock(return_value="slider")
    anti._auto_slide_captcha = Mock(return_value=False)
    anti._is_verification_expired = Mock(return_value=False)
    anti.random_delay = Mock()

    anti.handle_captcha(auto_retry=True)

    anti._auto_slide_captcha.assert_called_once()
    assert anti._captcha_retry_count == 0


def test_captcha_settings_default_loading():
    fields = Settings.model_fields
    assert fields["captcha_max_retries"].default == 3
    assert fields["captcha_cooldown_seconds"].default == 180
    assert fields["captcha_solve_timeout_seconds"].default == 180
    assert fields["captcha_refresh_retry_limit"].default == 5
    assert fields["captcha_click_slide_refresh_threshold"].default == 2
    assert fields["access_denied_max_retries"].default == 2
    assert fields["access_denied_backoff_min_seconds"].default == 8.0
    assert fields["access_denied_backoff_max_seconds"].default == 18.0
    assert fields["human_interaction_enabled"].default is True
    assert fields["session_warmup_enabled"].default is True
    assert fields["session_idle_rewarm_seconds"].default == 120
    assert fields["review_module_ready_timeout_seconds"].default == 12
    assert fields["review_request_capture_attempts"].default == 3

    cfg = Settings()
    assert isinstance(cfg.captcha_max_retries, int)
    assert isinstance(cfg.captcha_cooldown_seconds, int)
    assert isinstance(cfg.captcha_solve_timeout_seconds, int)
    assert isinstance(cfg.captcha_refresh_retry_limit, int)
    assert isinstance(cfg.captcha_click_slide_refresh_threshold, int)
    assert isinstance(cfg.access_denied_max_retries, int)


def test_post_captcha_stabilize_restores_focus_and_presence():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    page = Mock()
    page.run_js = Mock(return_value="complete")
    anti.get_page = Mock(return_value=page)
    anti.ensure_page_foreground = Mock()
    anti.simulate_human_presence = Mock()
    anti.random_delay = Mock()

    anti.post_captcha_stabilize("unit_test")

    anti.ensure_page_foreground.assert_called_once_with("unit_test")
    anti.simulate_human_presence.assert_called_once_with(reason="unit_test_presence", include_scroll=False)


def test_warm_session_opens_homepage_when_cold_profile_not_on_fliggy():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    page = Mock()
    page.url = "about:blank"
    page.wait.load_start = Mock()
    anti.get_page = Mock(return_value=page)
    anti.ensure_page_foreground = Mock()
    anti.simulate_human_presence = Mock()
    anti.random_delay = Mock()

    with patch.object(anti_module, "settings", Mock(
        session_warmup_enabled=True,
        session_idle_rewarm_seconds=120,
        human_interaction_enabled=True,
    )):
        anti.warm_session("unit_test", force=True)

    page.get.assert_called_once_with("https://www.fliggy.com/")
    anti.ensure_page_foreground.assert_called_once_with("unit_test")
