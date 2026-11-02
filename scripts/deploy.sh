#!/usr/bin/env bash
# 노트북에서 실행: 설정 검사 → VM으로 compose/ 동기화 → 기동
# 사용: scripts/deploy.sh [--no-lint]
set -euo pipefail

HOST="${MON_HOST:-monitoring@172.30.0.30}"
REMOTE_DIR="~/monitoring"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_DIR="$REPO_DIR/compose"

lint() {
  echo "== promtool check config / rules"
  docker run --rm -v "$COMPOSE_DIR/prometheus:/etc/prometheus:ro" --entrypoint promtool \
    prom/prometheus:v3.15.1 check config /etc/prometheus/prometheus.yml
  echo "== amtool check-config"
  docker run --rm -v "$COMPOSE_DIR/alertmanager:/etc/alertmanager:ro" --entrypoint amtool \
    prom/alertmanager:v0.34.2 check-config /etc/alertmanager/alertmanager.yml
}

[[ "${1:-}" == "--no-lint" ]] || lint

echo "== rsync → $HOST:$REMOTE_DIR"
ssh "$HOST" "mkdir -p $REMOTE_DIR/compose $REMOTE_DIR/scripts"
# .env · secrets/ 는 VM에만 있는 파일이라 덮어쓰지도 지우지도 않음
rsync -az --delete --exclude '.env' --exclude 'secrets/' \
  "$COMPOSE_DIR/" "$HOST:$REMOTE_DIR/compose/"
rsync -az "$REPO_DIR/scripts/" "$HOST:$REMOTE_DIR/scripts/"

echo "== docker compose up -d"
ssh "$HOST" "cd $REMOTE_DIR/compose && docker compose up -d --remove-orphans && docker compose ps"
