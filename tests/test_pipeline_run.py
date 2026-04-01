"""Tests for the full pipeline run function."""

import json
from unittest.mock import AsyncMock, patch

from municipality_email.countries.germany import GermanyConfig
from municipality_email.pipeline import run_pipeline
from municipality_email.schemas import Country, DomainCandidate, MunicipalityRecord


def _make_record(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="001", name="Test", region="Region", country=Country.DE)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


class TestRunPipeline:
    async def test_dry_run(self, tmp_path, capsys):
        config = GermanyConfig()

        async def mock_collect(data_dir):
            return [
                _make_record(
                    code="001",
                    name="Teststadt",
                    candidates=[DomainCandidate(domain="test.de", source="livenson")],
                ),
            ]

        config.collect_candidates = mock_collect

        await run_pipeline(
            config,
            data_dir=tmp_path,
            output_dir=tmp_path / "out",
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "DRY RUN" in out

    async def test_full_run_mocked(self, tmp_path):
        config = GermanyConfig()

        async def mock_collect(data_dir):
            return [
                _make_record(
                    code="001",
                    name="Teststadt",
                    candidates=[DomainCandidate(domain="test.de", source="livenson")],
                ),
            ]

        config.collect_candidates = mock_collect

        # Mock all HTTP and DNS
        with (
            patch(
                "municipality_email.pipeline.phase_dns_prefilter",
                new_callable=AsyncMock,
                return_value={"test.de": True},
            ),
            patch(
                "municipality_email.pipeline.phase_validate",
                new_callable=AsyncMock,
                return_value={"test.de": (True, None, False)},
            ),
            patch(
                "municipality_email.pipeline.phase_scrape",
                new_callable=AsyncMock,
                return_value={"test.de": ({"test.de"}, None, True)},
            ),
            patch(
                "municipality_email.pipeline.phase_mx",
                new_callable=AsyncMock,
                return_value={"test.de": True},
            ),
        ):
            await run_pipeline(
                config,
                data_dir=tmp_path,
                output_dir=tmp_path / "out",
            )

        # Verify output was written
        assert (tmp_path / "out" / "de.json").exists()
        data = json.loads((tmp_path / "out" / "de.json").read_text())
        assert data["total"] == 1


class TestCliEntryPoints:
    def test_resolve_entry_point(self):
        """Test that the resolve entry point function exists."""
        from municipality_email.cli import resolve

        assert callable(resolve)

    def test_classify_entry_point(self):
        """Test that the classify entry point function exists."""
        from municipality_email.cli import classify

        assert callable(classify)
