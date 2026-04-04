"""Multi-resolver DNS queries with fallback."""

from __future__ import annotations

import asyncio

import dns.asyncresolver
import dns.resolver
from loguru import logger

_resolvers: list[dns.asyncresolver.Resolver] | None = None
_dns_semaphore = asyncio.Semaphore(50)


def make_resolvers() -> list[dns.asyncresolver.Resolver]:
    """Create async resolvers: system default, Quad9, Cloudflare."""
    cache = dns.resolver.Cache()
    resolvers = []
    for nameservers in [None, ["9.9.9.9", "149.112.112.112"], ["1.1.1.1", "1.0.0.1"]]:
        r = dns.asyncresolver.Resolver()
        if nameservers:
            r.nameservers = nameservers
        r.timeout = 5
        r.lifetime = 8
        r.cache = cache
        resolvers.append(r)
    return resolvers


def get_resolvers() -> list[dns.asyncresolver.Resolver]:
    global _resolvers
    if _resolvers is None:
        _resolvers = make_resolvers()
    return _resolvers


def reset_resolvers() -> None:
    """Reset cached resolvers (useful for testing)."""
    global _resolvers
    _resolvers = None


async def resolve_robust(qname: str, rdtype: str) -> dns.resolver.Answer | None:
    """DNS query with multi-resolver fallback."""
    resolvers = get_resolvers()
    for i, resolver in enumerate(resolvers):
        try:
            return await resolver.resolve(qname, rdtype)
        except dns.resolver.NXDOMAIN:
            return None
        except dns.resolver.NoAnswer:
            logger.trace("DNS {}/{}: NoAnswer (terminal)", qname, rdtype)
            return None
        except dns.resolver.NoNameservers:
            logger.trace("DNS {}/{}: NoNameservers on resolver {}", qname, rdtype, i)
        except Exception:
            logger.trace("DNS {}/{}: error on resolver {}", qname, rdtype, i)
    logger.warning("DNS {}/{}: all resolvers exhausted", qname, rdtype)
    return None


async def lookup_a(domain: str) -> bool:
    """Check if domain resolves (A or AAAA record)."""
    async with _dns_semaphore:
        a_result, aaaa_result = await asyncio.gather(
            resolve_robust(domain, "A"),
            resolve_robust(domain, "AAAA"),
        )
        return a_result is not None or aaaa_result is not None


async def lookup_mx(domain: str) -> list[str]:
    """Return sorted list of MX exchange hostnames for a domain."""
    async with _dns_semaphore:
        answer = await resolve_robust(domain, "MX")
    if answer is None:
        return []
    return sorted(str(r.exchange).rstrip(".").lower() for r in answer)