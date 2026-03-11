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
    assert page.ele.call_count == 2
    assert page.ele.call_args_list[0].args[0] == "#nc_1_n1z"
    assert page.ele.call_args_list[1].args[0] == ".nc-container"


def test_check_captcha_detection_flow_returns_false_when_absent():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    page = Mock()
    page.ele.return_value = None
    anti.get_page = Mock(return_value=page)

    assert anti.check_captcha() is False
    assert page.ele.call_count == 5


def test_auto_slide_generates_segmented_movement_flow():
    anti_module = _load_anti_crawler_module()
    anti = anti_module.AntiCrawler()

    slider = Mock()
    track = Mock()
    track.rect = {"width": 90}

    page = Mock()
    page.actions = Mock()

    def ele_side_effect(selector, timeout=1):
        if selector in ("#nc_1_n1z", ".nc-lang-cnt"):
            return slider
        if selector in (".nc_scale", ".nc-container"):
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
    assert total_moved == 90


def test_captcha_settings_default_loading():
    fields = Settings.model_fields
    assert fields["captcha_max_retries"].default == 3
    assert fields["captcha_cooldown_seconds"].default == 180
    assert fields["captcha_solve_timeout_seconds"].default == 120
    assert fields["captcha_refresh_retry_limit"].default == 2

    cfg = Settings()
    assert isinstance(cfg.captcha_max_retries, int)
    assert isinstance(cfg.captcha_cooldown_seconds, int)
    assert isinstance(cfg.captcha_solve_timeout_seconds, int)
    assert isinstance(cfg.captcha_refresh_retry_limit, int)
