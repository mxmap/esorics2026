# Security Scanner

Kotlin/Docker-based email security scanner that tests DANE/DNSSEC and email authentication (SPF, DMARC) for municipality email domains.

## Usage

Invoked via the project CLI:

```bash
uv run scan ch    # scan Swiss municipalities
uv run scan de    # scan German municipalities
uv run scan at    # scan Austrian municipalities
uv run scan ch -v # verbose (streams Docker output)
```

## Prerequisites

- Docker (with `docker compose` v2 or `docker-compose` v1)
- Domain resolver output in `output/domains/domains_{cc}.json`

> [!IMPORTANT]
> **DANE/TLSA scanning requires unrestricted outbound port 25 (SMTP).** The `gotls` tool connects to MX servers on port 25 to validate TLS certificates against TLSA records. Most residential ISPs and macOS/Docker Desktop environments block outbound SMTP, causing all DANE checks to fail while SPF/DMARC results remain correct (DNS-only).
>
> Verify port 25 access:
> ```bash
> nc -z -w5 mta-gw.infomaniak.ch 25 && echo "open" || echo "blocked"
> ```

## Architecture

- **scanner/** — Kotlin app that resolves MX records and runs TLS, DANE, and DSS probes against each domain
- **evaluator/** — Kotlin app that aggregates raw scan results into per-domain security assessments
- **docker-compose.yaml** — Orchestrates both containers with shared volumes

The Python wrapper in `src/mail_municipalities/security_analysis/runner.py` handles input/output transformation and Docker invocation.

## Dependencies

Both containers are built from `eclipse-temurin:11-jdk` (Java 11). The scanner container bundles several external tools:

| Dependency | Version | Purpose | Source |
|---|---|---|---|
| Kotlin | 1.7.10 | Scanner & evaluator application language | JetBrains |
| Gradle | 7.1 | Build system | [gradle.org](https://gradle.org) |
| Go | 1.21 | Builds gotls for DANE validation | [go.dev](https://go.dev) |
| gotls | latest | STARTTLS + DANE/TLSA certificate validation | [github.com/shuque/gotls](https://github.com/shuque/gotls) |
| testssl.sh | 3.0 | TLS configuration testing via OpenSSL | [github.com/testssl/testssl.sh](https://github.com/testssl/testssl.sh) |
| dss | pre-built binary (linux/amd64) | SPF, DKIM, DMARC record validation | [github.com/globalcyberalliance/domain-security-scanner](https://github.com/globalcyberalliance/domain-security-scanner) |

### Kotlin libraries (scanner)

| Library | Purpose |
|---|---|
| kotlinx-coroutines-core 1.6.4 | Concurrent scanning |
| kotlinx-serialization-json 1.4.0 | JSON I/O |
| kotlinx-datetime 0.4.0 | Timestamps |
| OkHttp 4.10.0 | HTTP client |
| Guava 31.1 | Rate limiting |
| HikariCP 5.0.1 | Connection pooling |
| SLF4J 1.7.36 | Logging |

### Go libraries (gotls)

| Library | Purpose |
|---|---|
| github.com/miekg/dns | DNS resolution |
| github.com/shuque/dane | DANE/TLSA validation |

## Configuration

Default scan parameters are auto-generated in `.env` on first run. See `.env.template` for all available options (DNS rate limits, timeouts, thread counts).
