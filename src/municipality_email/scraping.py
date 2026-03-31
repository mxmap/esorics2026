"""Web scraping, email extraction, and domain utilities."""

from __future__ import annotations

import json
import re
import ssl
import warnings
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
from email_validator import EmailNotValidError, validate_email
from loguru import logger

# ── Constants ────────────────────────────────────────────────────────

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
TYPO3_RE = re.compile(r"linkTo_UnCryptMailto\((?:['\"]|%27|%22)([^'\"]+?)(?:['\"]|%27|%22)")
SPARQL_URL = "https://query.wikidata.org/sparql"

_ASSET_EXTENSIONS = frozenset(
    {
        "png",
        "jpg",
        "jpeg",
        "gif",
        "svg",
        "webp",
        "ico",
        "bmp",
        "tiff",
        "css",
        "js",
        "woff",
        "woff2",
        "ttf",
        "eot",
        "map",
        "pdf",
        "zip",
        "xml",
        "json",
    }
)


# ── URL / domain helpers ────────────────────────────────────────────


def url_to_domain(url: str | None) -> str | None:
    """Extract the base domain from a URL, stripping www."""
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = parsed.hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host if host else None


def _is_valid_email(email: str) -> bool:
    """Validate an email address using email-validator (no DNS)."""
    try:
        validate_email(email, check_deliverability=False)
    except EmailNotValidError:
        return False
    domain = email.split("@")[1]
    tld = domain.rsplit(".", 1)[-1].lower()
    return tld not in _ASSET_EXTENSIONS


# ── TYPO3 decryption ────────────────────────────────────────────────


def decrypt_typo3(encoded: str, offset: int = 2) -> str:
    """Decrypt TYPO3 linkTo_UnCryptMailto Caesar cipher.

    TYPO3 encrypts mailto: links with a Caesar shift on three ASCII ranges:
      0x2B-0x3A (+,-./0123456789:)
      0x40-0x5A (@A-Z)
      0x61-0x7A (a-z)
    """
    ranges = [(0x2B, 0x3A), (0x40, 0x5A), (0x61, 0x7A)]
    result = []
    for c in encoded:
        code = ord(c)
        decrypted = False
        for start, end in ranges:
            if start <= code <= end:
                size = end - start + 1
                n = start + (code - start + offset) % size
                result.append(chr(n))
                decrypted = True
                break
        if not decrypted:
            result.append(c)
    return "".join(result)


# ── Email extraction ────────────────────────────────────────────────


def extract_email_domains(html: str, skip_domains: set[str]) -> set[str]:
    """Extract email domains from HTML, including TYPO3-obfuscated emails."""
    domains: set[str] = set()

    def _add(email: str) -> None:
        if not _is_valid_email(email):
            return
        domain = email.split("@")[1].lower().rstrip("\\/.")
        if domain and domain not in skip_domains:
            domains.add(domain)

    # Simple @ in body
    for email in EMAIL_RE.findall(html):
        _add(email)

    # mailto: links
    for email in re.findall(r'mailto:([^">\s?]+)', html):
        if "@" in email:
            _add(email)

    # TYPO3 obfuscated emails
    for encoded in TYPO3_RE.findall(html):
        for offset in range(-25, 26):
            decoded = decrypt_typo3(encoded, offset)
            decoded = decoded.replace("mailto:", "")
            if "@" in decoded and EMAIL_RE.search(decoded):
                _add(decoded)
                break

    # (at) / [at] variants
    for m in re.finditer(r"[\[(][Aa][Tt][\])]", html):
        before = html[max(0, m.start() - 64) : m.start()]
        after = html[m.end() : m.end() + 253]
        local_match = re.search(r"([\w.-]+)\s*$", before)
        domain_match = re.match(r"\s*([\w.-]+\.\w+)", after)
        if local_match and domain_match:
            _add(f"{local_match.group(1)}@{domain_match.group(1)}")

    return domains


# ── URL building ────────────────────────────────────────────────────


def build_urls(domain: str, subpages: list[str]) -> list[str]:
    """Build candidate URLs to scrape, trying www. prefix first."""
    domain = domain.strip()
    if domain.startswith(("http://", "https://")):
        parsed = urlparse(domain)
        domain = parsed.hostname or domain
    if domain.startswith("www."):
        bare = domain[4:]
    else:
        bare = domain

    bases = [f"https://www.{bare}", f"https://{bare}"]
    urls = []
    for base in bases:
        urls.append(base + "/")
        for path in subpages:
            urls.append(base + path)
    return urls


# ── SSL helpers ─────────────────────────────────────────────────────


def _is_ssl_error(exc: BaseException) -> bool:
    """Check if an exception (or any in its chain) is an SSL verification error."""
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        if "CERTIFICATE_VERIFY_FAILED" in str(current):
            return True
        current = current.__cause__ if current.__cause__ is not current else None
    return False


async def _fetch_insecure(url: str) -> httpx.Response:
    """Fetch a URL with SSL verification disabled (single request)."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        async with httpx.AsyncClient(verify=False) as insecure_client:
            return await insecure_client.get(url, follow_redirects=True, timeout=15)


# ── HEAD validation (Phase 2) ──────────────────────────────────────


async def validate_domain_accessibility(
    client: httpx.AsyncClient,
    domain: str,
) -> tuple[bool, str | None, bool]:
    """Check if a domain is accessible via HEAD request.

    Returns (accessible, redirect_target, ssl_failed).
    """
    for prefix in [f"https://www.{domain}", f"https://{domain}"]:
        try:
            r = await client.head(prefix, follow_redirects=True, timeout=10)
            if r.status_code < 400:
                final = url_to_domain(str(r.url))
                redirect = final if (final and final != domain) else None
                return True, redirect, False
        except httpx.ConnectError as exc:
            if _is_ssl_error(exc):
                # Try insecure
                try:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
                        async with httpx.AsyncClient(verify=False) as ic:
                            r = await ic.head(prefix, follow_redirects=True, timeout=10)
                    if r.status_code < 400:
                        final = url_to_domain(str(r.url))
                        redirect = final if (final and final != domain) else None
                        return True, redirect, True
                except Exception:
                    continue
        except Exception:
            continue
    return False, None, False


# ── Scraping ────────────────────────────────────────────────────────


def _process_scrape_response(
    r: httpx.Response,
    domain: str,
    all_domains: set[str],
    redirect_domain: str | None,
    skip_domains: set[str],
) -> tuple[set[str], str | None]:
    """Extract emails and detect redirects from a scrape response."""
    if r.status_code != 200:
        return all_domains, redirect_domain

    if redirect_domain is None:
        final_domain = url_to_domain(str(r.url))
        if final_domain and final_domain != domain:
            redirect_domain = final_domain
            logger.info("Redirect detected: {} -> {}", domain, redirect_domain)

    domains = extract_email_domains(r.text, skip_domains)
    all_domains |= domains
    return all_domains, redirect_domain


async def scrape_email_domains(
    client: httpx.AsyncClient,
    domain: str,
    subpages: list[str],
    skip_domains: set[str],
    *,
    exhaustive: bool = True,
    ssl_failed: bool = False,
) -> tuple[set[str], str | None, bool]:
    """Scrape a municipality website for email domains.

    Returns (email_domains_found, redirect_target_or_None, accessible).
    """
    if not domain:
        return set(), None, False

    all_domains: set[str] = set()
    redirect_domain: str | None = None
    accessible = False
    consecutive_failures = 0
    max_consecutive_failures = 3
    urls = build_urls(domain, subpages)

    for url in urls:
        if consecutive_failures >= max_consecutive_failures:
            logger.debug(
                "Giving up on {} after {} consecutive failures",
                domain,
                consecutive_failures,
            )
            break

        if ssl_failed:
            try:
                r = await _fetch_insecure(url)
            except Exception as exc:
                consecutive_failures += 1
                logger.debug("Insecure fetch {} failed: {}", url, exc)
                continue
        else:
            try:
                r = await client.get(url, follow_redirects=True, timeout=15)
            except httpx.ConnectError as exc:
                if _is_ssl_error(exc):
                    ssl_failed = True
                    logger.info("SSL error on {}, retrying without verification", url)
                    try:
                        r = await _fetch_insecure(url)
                    except Exception:
                        consecutive_failures += 1
                        continue
                else:
                    consecutive_failures += 1
                    logger.debug("Scrape {} failed: {}", url, exc)
                    continue
            except Exception as exc:
                consecutive_failures += 1
                logger.debug("Scrape {} failed: {}", url, exc)
                continue

        consecutive_failures = 0
        accessible = True

        all_domains, redirect_domain = _process_scrape_response(
            r, domain, all_domains, redirect_domain, skip_domains
        )
        if all_domains and not exhaustive:
            return all_domains, redirect_domain, accessible

    return all_domains, redirect_domain, accessible


# ── Website mismatch detection ──────────────────────────────────────


def _slugify_name(name: str) -> set[str]:
    """Generate slug variants for a municipality name (umlaut/accent handling)."""
    raw = name.lower().strip()
    raw = re.sub(r"\s*\(.*?\)\s*", "", raw)

    # German umlaut transliteration
    de = raw.replace("\u00fc", "ue").replace("\u00e4", "ae").replace("\u00f6", "oe")
    # French accent removal
    fr = raw
    for a, b in [
        ("\u00e9", "e"),
        ("\u00e8", "e"),
        ("\u00ea", "e"),
        ("\u00eb", "e"),
        ("\u00e0", "a"),
        ("\u00e2", "a"),
        ("\u00f4", "o"),
        ("\u00ee", "i"),
        ("\u00f9", "u"),
        ("\u00fb", "u"),
        ("\u00e7", "c"),
        ("\u00ef", "i"),
    ]:
        fr = fr.replace(a, b)

    def slugify(s):
        s = re.sub(r"['\u2019`]", "", s)
        s = re.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")

    return {slugify(de), slugify(fr), slugify(raw)} - {""}


def detect_website_mismatch(name: str, website_domain: str) -> bool:
    """Detect if a website domain doesn't match the municipality name.

    Returns True if the domain appears unrelated to the municipality name.
    """
    if not name or not website_domain:
        return False

    domain_lower = website_domain.lower()
    slugs = _slugify_name(name)

    # Handle common prefixes
    prefixes = ["stadt-", "gemeinde-", "commune-de-", "comune-di-", "markt-"]
    domain_stripped = domain_lower
    for prefix in prefixes:
        if domain_stripped.startswith(prefix):
            domain_stripped = domain_stripped[len(prefix) :]
            break

    # Remove TLD for matching
    domain_base = domain_stripped.rsplit(".", 1)[0] if "." in domain_stripped else domain_stripped
    # Strip canton subdomain: e.g. teufen.ar.ch -> teufen
    parts = domain_base.split(".")
    domain_base_first = parts[0] if parts else domain_base

    for slug in slugs:
        if slug in domain_lower:
            return False
        if slug in domain_stripped:
            return False
        if slug == domain_base_first:
            return False

    # Check if any word from the name (4+ chars) appears in the domain
    raw = name.lower().strip()
    raw = re.sub(r"\s*\(.*?\)\s*", "", raw)
    de = raw.replace("\u00fc", "ue").replace("\u00e4", "ae").replace("\u00f6", "oe")
    fr = raw
    for a, b in [
        ("\u00e9", "e"),
        ("\u00e8", "e"),
        ("\u00ea", "e"),
        ("\u00eb", "e"),
        ("\u00e0", "a"),
        ("\u00e2", "a"),
        ("\u00f4", "o"),
        ("\u00ee", "i"),
        ("\u00f9", "u"),
        ("\u00fb", "u"),
        ("\u00e7", "c"),
        ("\u00ef", "i"),
    ]:
        fr = fr.replace(a, b)

    for variant in [raw, de, fr]:
        words = re.findall(r"[a-z]{4,}", variant)
        for word in words:
            if word in domain_lower:
                return False

    return True


# ── Overrides ───────────────────────────────────────────────────────


def load_overrides(overrides_path: Path) -> dict[str, dict[str, str]]:
    """Load manual overrides from JSON file."""
    if not overrides_path.exists():
        return {}
    with open(overrides_path, encoding="utf-8") as f:
        return json.load(f)


# ── Scrape cache ────────────────────────────────────────────────────


def load_scrape_cache(
    path: Path, ttl_days: int = 7
) -> dict[str, tuple[set[str], str | None, bool]]:
    """Load scrape cache, filtering expired entries by TTL."""
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        logger.warning("Ignoring corrupt scrape cache at {}", path)
        return {}

    now = datetime.now(tz=timezone.utc)
    result: dict[str, tuple[set[str], str | None, bool]] = {}
    expired = 0
    for domain, entry in raw.items():
        ts = entry.get("timestamp")
        if ts:
            try:
                entry_time = datetime.fromisoformat(ts)
                age_days = (now - entry_time).total_seconds() / 86400
                if age_days > ttl_days:
                    expired += 1
                    continue
            except (ValueError, TypeError):
                pass
        result[domain] = (
            set(entry.get("emails", [])),
            entry.get("redirect"),
            entry.get("accessible", False),
        )

    if expired:
        logger.info("Scrape cache: {} expired entries filtered", expired)
    return result


def save_scrape_cache(path: Path, data: dict[str, tuple[set[str], str | None, bool]]) -> None:
    """Persist scrape cache to JSON (atomic write)."""
    raw = {
        domain: {
            "emails": sorted(emails),
            "redirect": redirect,
            "accessible": accessible,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        for domain, (emails, redirect, accessible) in data.items()
    }
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False)
    tmp.replace(path)
