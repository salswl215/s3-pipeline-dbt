#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/config.env"; set +a
