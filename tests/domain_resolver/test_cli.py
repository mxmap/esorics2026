"""Tests for CLI module."""

import pytest
import typer
from typer.testing import CliRunner

from mail_municipalities.cli import _get_config, _resolve_app, app

runner = CliRunner()


class TestGetConfig:
    def test_ch(self):
        from mail_municipalities.domain_resolver.countries.switzerland import SwitzerlandConfig

        config = _get_config("ch")
        assert isinstance(config, SwitzerlandConfig)

    def test_de(self):
        from mail_municipalities.domain_resolver.countries.germany import GermanyConfig

        config = _get_config("de")
        assert isinstance(config, GermanyConfig)

    def test_at(self):
        from mail_municipalities.domain_resolver.countries.austria import AustriaConfig

        config = _get_config("at")
        assert isinstance(config, AustriaConfig)

    def test_invalid(self):
        with pytest.raises(typer.Exit):
            _get_config("xx")


class TestMultiCommandApp:
    def test_no_args(self):
        result = runner.invoke(app, ["resolve"])
        assert result.exit_code != 0

    def test_classify_requires_country(self):
        result = runner.invoke(app, ["classify"])
        assert result.exit_code != 0

    def test_invalid_country(self):
        result = runner.invoke(app, ["resolve", "xx"])
        assert result.exit_code != 0


class TestResolveEntryPoint:
    """Test the standalone resolve app (used by [project.scripts])."""

    def test_no_args(self):
        result = runner.invoke(_resolve_app, [])
        assert result.exit_code != 0

    def test_invalid_country(self):
        result = runner.invoke(_resolve_app, ["xx"])
        assert result.exit_code != 0

    def test_help(self):
        result = runner.invoke(_resolve_app, ["--help"])
        assert result.exit_code == 0
        assert "country" in result.output.lower()
