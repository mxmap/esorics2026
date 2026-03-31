"""Tests for CLI module."""

import pytest
import typer
from typer.testing import CliRunner

from municipality_email.cli import _get_config, _resolve_app, app

runner = CliRunner()


class TestGetConfig:
    def test_ch(self):
        from municipality_email.countries.switzerland import SwitzerlandConfig

        config = _get_config("ch")
        assert isinstance(config, SwitzerlandConfig)

    def test_de(self):
        from municipality_email.countries.germany import GermanyConfig

        config = _get_config("de")
        assert isinstance(config, GermanyConfig)

    def test_at(self):
        from municipality_email.countries.austria import AustriaConfig

        config = _get_config("at")
        assert isinstance(config, AustriaConfig)

    def test_invalid(self):
        with pytest.raises(typer.Exit):
            _get_config("xx")


class TestMultiCommandApp:
    def test_no_args(self):
        result = runner.invoke(app, ["resolve"])
        assert result.exit_code != 0

    def test_classify_not_implemented(self):
        result = runner.invoke(app, ["classify", "ch"])
        assert result.exit_code == 1
        assert "not yet implemented" in result.output.lower()

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
