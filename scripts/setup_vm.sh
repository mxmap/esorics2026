#!/usr/bin/env bash
# Setup and run the security scanner on a fresh Ubuntu 22.04+ VM.
#
# Prerequisites:
#   - Ubuntu 22.04 or 24.04 VM with root/sudo access
#   - Outbound port 25 (SMTP) must be open for DANE/TLSA validation
#   - Domain resolver output already generated (output/domains/domains_{cc}.json)
#
# Usage:
#   # Clone the repo first, then:
#   chmod +x scripts/setup_vm.sh
#   ./scripts/setup_vm.sh          # install dependencies + scan all countries
#   ./scripts/setup_vm.sh --scan   # skip install, just run scans
#   ./scripts/setup_vm.sh ch       # install + scan Switzerland only
#   ./scripts/setup_vm.sh --scan de at  # scan Germany and Austria only

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

# ── Helpers ───────────────────────────────────────────────────────────

log()  { echo -e "\n\033[1;34m▸ $*\033[0m"; }
ok()   { echo -e "\033[1;32m✓ $*\033[0m"; }
warn() { echo -e "\033[1;33m⚠ $*\033[0m"; }
fail() { echo -e "\033[1;31m✗ $*\033[0m" >&2; exit 1; }

check_port25() {
    log "Checking outbound port 25 (SMTP) access"
    if nc -z -w5 mta-gw.infomaniak.ch 25 2>/dev/null; then
        ok "Port 25 is open — DANE scanning will work"
    else
        warn "Port 25 appears blocked — DANE results will be empty (SPF/DKIM/DMARC unaffected)"
    fi
}

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

    # Allow current user to run Docker without sudo
    sudo usermod -aG docker "$USER"
    ok "Docker installed — you may need to log out and back in for group membership to take effect"
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

# ── Scan ──────────────────────────────────────────────────────────────

delete_existing_scans() {
    log "Deleting existing scan results"
    rm -f "$REPO_DIR/output/security/security_"*.json
    ok "Existing scans deleted"
}

configure_env() {
    local env_file="$REPO_DIR/src/security_test/.env"
    if [ -f "$env_file" ]; then
        return
    fi

    log "Generating .env for security scanner"
    # Use the VM's hostname for EHLO (important for DANE — some servers reject 'localhost')
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

run_scan() {
    local cc="$1"
    local domains_file="$REPO_DIR/output/domains/domains_${cc}.json"

    if [ ! -f "$domains_file" ]; then
        warn "Skipping ${cc}: ${domains_file} not found (run 'uv run resolve ${cc}' first)"
        return 1
    fi

    log "Scanning ${cc^^} municipalities"
    uv run scan "$cc" -v
    ok "Scan complete for ${cc^^} — results in output/security/security_${cc}.json"
}

# ── Main ──────────────────────────────────────────────────────────────

main() {
    local skip_install=false
    local countries=()

    # Parse arguments
    for arg in "$@"; do
        case "$arg" in
            --scan) skip_install=true ;;
            ch|de|at) countries+=("$arg") ;;
            *) fail "Unknown argument: $arg (expected --scan, ch, de, or at)" ;;
        esac
    done

    # Default to all countries
    if [ ${#countries[@]} -eq 0 ]; then
        countries=(ch de at)
    fi

    echo "================================================================"
    echo "  Municipality Email Security Scanner"
    echo "  Countries: ${countries[*]}"
    echo "================================================================"

    if [ "$skip_install" = false ]; then
        install_deps
    fi

    check_port25
    configure_env
    delete_existing_scans

    local failed=0
    for cc in "${countries[@]}"; do
        run_scan "$cc" || ((failed++))
    done

    echo ""
    echo "================================================================"
    if [ "$failed" -eq 0 ]; then
        ok "All scans completed successfully"
    else
        warn "${failed} country scan(s) skipped (missing domain files)"
    fi
    echo "================================================================"
}

main "$@"