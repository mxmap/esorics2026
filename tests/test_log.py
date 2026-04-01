"""Tests for logging module."""

import logging

from municipality_email.log import _InterceptHandler, setup


class TestSetup:
    def test_default_setup(self):
        setup(verbose=False)
        # Should not raise

    def test_verbose_setup(self):
        setup(verbose=True)
        # Should not raise

    def test_suppresses_noisy_loggers(self):
        setup()
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("httpcore").level == logging.WARNING
        assert logging.getLogger("dns").level == logging.WARNING
        assert logging.getLogger("stamina").level == logging.WARNING


class TestInterceptHandler:
    def test_emit(self):
        handler = _InterceptHandler()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        # Should not raise
        handler.emit(record)

    def test_emit_unknown_level(self):
        handler = _InterceptHandler()
        record = logging.LogRecord(
            name="test",
            level=99,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        record.levelname = "NONEXISTENT_LEVEL"
        # Should fall back to numeric level without raising
        handler.emit(record)
