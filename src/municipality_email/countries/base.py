"""Abstract base class for country-specific configuration."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from municipality_email.schemas import Country, MunicipalityRecord


class CountryConfig(ABC):
    country: Country
    code_field: str  # "bfs" / "ags" / "gkz"
    tlds: list[str]
    government_tlds: list[str]
    skip_domains: set[str]
    subpages: list[str]
    concurrency: int

    @abstractmethod
    async def collect_candidates(self, data_dir: Path) -> list[MunicipalityRecord]:
        """Fetch all municipalities from canonical + supplementary sources.

        Returns MunicipalityRecord with candidates populated.
        """

    @abstractmethod
    def guess_domains(self, name: str, region: str) -> list[str]:
        """Generate plausible domain candidates from municipality name."""

    @abstractmethod
    def domain_matches_name(self, name: str, domain: str) -> bool:
        """Check if domain is plausible for this municipality name."""

    @abstractmethod
    def slugify_name(self, name: str) -> set[str]:
        """Generate slug variants for name matching."""

    def pick_best_email(self, emails: set[str], name: str, static_domains: set[str]) -> list[str]:
        """Pick and sort email domains by preference.

        Default: government TLD > name match > alphabetical.
        """
        gov = sorted(d for d in emails if any(d.endswith(t) for t in self.government_tlds))
        name_match = sorted(
            d for d in emails if self.domain_matches_name(name, d) and d not in gov
        )
        rest = sorted(d for d in emails if d not in gov and d not in name_match)
        return gov + name_match + rest

    def regional_suffixes(self, region: str) -> list[str]:
        """Return regional domain suffixes. Default: empty."""
        return []
