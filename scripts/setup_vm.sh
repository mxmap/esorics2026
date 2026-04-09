#!/usr/bin/env bash
# Install dependencies and configure the security scanner on a fresh Ubuntu 22.04+ VM.
#
# Prerequisites:
#   - Ubuntu 22.04 or 24.04 VM with sudo access
#   - Outbound port 25 (SMTP) must be open for DANE/TLSA validation
#   - Easiest is to allow all egress traffic in security groups
#
# Usage:
#   chmod +x scripts/setup_vm.sh
#   ./scripts/setup_vm.sh
#
# After setup completes, run scans manually:
#   uv run scan ch -v    # Switzerland
#   uv run scan de -v    # Germany
#   uv run scan at -v    # Austria

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

# ── Helpers ───────────────────────────────────────────────────────────

log()  { echo -e "\n\033[1;34m▸ $*\033[0m"; }
ok()   { echo -e "\033[1;32m✓ $*\033[0m"; }
warn() { echo -e "\033[1;33m⚠ $*\033[0m"; }

# ── Install ───────────────────────────────────────────────────────────

install_docker() {
    if command -v docker &>/dev/null && docker compose version &>/dev/null; then
        ok "Docker already installed ($(docker --version))"
        return
    fi

    log "Installing Docker"
    sudo apt-get update -qq
    sudo apt-get install -y -qq ca-certificates curl gnupg

    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
        sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update -qq
    sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin

    sudo usermod -aG docker "$USER"
    ok "Docker installed"
}

install_uv() {
    if command -v uv &>/dev/null; then
        ok "uv already installed ($(uv --version))"
        return
    fi

    log "Installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
    ok "uv installed ($(uv --version))"
}

install_deps() {
    log "Installing system packages"
    sudo apt-get update -qq
    sudo apt-get install -y -qq git netcat-openbsd

    install_docker
    install_uv

    log "Installing Python dependencies"
    uv sync

    ok "All dependencies installed"
}

# ── Configure ─────────────────────────────────────────────────────────

check_port25() {
    log "Checking outbound port 25 (SMTP) access"
    if nc -z -w5 mta-gw.infomaniak.ch 25 2>/dev/null; then
        ok "Port 25 is open — DANE scanning will work"
    else
        warn "Port 25 appears blocked — DANE results will be empty (SPF/DKIM/DMARC unaffected)"
    fi
}

configure_env() {
    local env_file="$REPO_DIR/src/security_test/.env"
    if [ -f "$env_file" ]; then
        ok "Using existing .env"
        return
    fi

    log "Generating .env for security scanner"
    local ehlo_name
    ehlo_name=$(hostname -f 2>/dev/null || hostname)

    cat > "$env_file" <<EOF
LOG_LEVEL=INFO
DNS_NAMESERVER=8.8.8.8
DNS_QUERIES_PER_SECOND=10.0
DNS_THREAD_MULTIPLIER=4
TESTSSL_STARTTLS_SLEEP=60
TESTSSL_CONNECT_TIMEOUT=30s
TESTSSL_OPENSSL_TIMEOUT=30s
SCAN_THREAD_MULTIPLIER=4
DANE_RETRIES=5
DANE_TRIES_PER_SECOND=10.0
DANE_TIMEOUT=300
DANE_EHLO_NAME=${ehlo_name}
TLS_RETRIES=5
TLS_TRIES_PER_SECOND=10.0
TLS_TIMEOUT=300
DSS_TRIES_PER_SECOND=10.0
DATASET_DELAY_MINUTES=0
INPUT_FILE=
EOF
    ok "Generated .env with DANE_EHLO_NAME=${ehlo_name}"
}

# ── Main ──────────────────────────────────────────────────────────────

main() {
    echo "================================================================"
    echo "  Municipality Email Security Scanner — Setup"
    echo "================================================================"

    install_deps
    check_port25
    configure_env

    echo ""
    echo "================================================================"
    ok "Setup complete"
    echo ""
    echo "  If Docker was just installed, activate the group first:"
    echo "    newgrp docker"
    echo ""
    echo "  Then run scans:"
    echo "    uv run scan ch -v    # Switzerland (~2,100 municipalities)"
    echo "    uv run scan de -v    # Germany (~11,100 municipalities)"
    echo "    uv run scan at -v    # Austria (~2,100 municipalities)"
    echo ""
    echo "  Results will be written to output/security/security_{cc}.json"
    echo "================================================================"
}

main "$@"
