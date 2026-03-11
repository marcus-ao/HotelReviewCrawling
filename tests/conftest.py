"""Pytest test-only dependency stubs.

Provide a lightweight DrissionPage stub so unit tests can import crawler modules
without requiring the real browser automation package.
"""

import sys
import types


def _install_drissionpage_stub() -> None:
    if "DrissionPage" in sys.modules:
        return

    drission_stub = types.ModuleType("DrissionPage")

    class ChromiumPage:  # pragma: no cover - test import stub
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class ChromiumOptions:  # pragma: no cover - test import stub
        pass

    setattr(drission_stub, "ChromiumPage", ChromiumPage)
    setattr(drission_stub, "ChromiumOptions", ChromiumOptions)
    sys.modules["DrissionPage"] = drission_stub


_install_drissionpage_stub()
