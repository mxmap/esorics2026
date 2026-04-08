"""Default .env configuration for the security scanner Docker containers."""

from __future__ import annotations

DEFAULTS: dict[str, str] = {
    "LOG_LEVEL": "INFO",
    "DNS_NAMESERVER": "8.8.8.8",
    "DNS_QUERIES_PER_SECOND": "10.0",
    "DNS_THREAD_MULTIPLIER": "4",
    "TESTSSL_STARTTLS_SLEEP": "60",
    "TESTSSL_CONNECT_TIMEOUT": "30s",
    "TESTSSL_OPENSSL_TIMEOUT": "30s",
    "SCAN_THREAD_MULTIPLIER": "4",
    "DANE_RETRIES": "5",
    "DANE_TRIES_PER_SECOND": "10.0",
    "DANE_TIMEOUT": "300",
    "DANE_EHLO_NAME": "localhost",
    "TLS_RETRIES": "5",
    "TLS_TRIES_PER_SECOND": "10.0",
    "TLS_TIMEOUT": "300",
    "DSS_TRIES_PER_SECOND": "10.0",
    "DATASET_DELAY_MINUTES": "0",
    "INPUT_FILE": "",
    "REST_USERNAME": "admin",
    "REST_PASSWORD": "admin",
}
