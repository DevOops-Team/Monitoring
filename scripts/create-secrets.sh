#!/usr/bin/env bash
# 사람이 자기 터미널에서 실행한다. 입력은 화면에 안 보이고(read -s) 레포·셸 기록에 남지 않는다.
# 사용: KUBECONFIG=<admin.conf> scripts/create-secrets.sh
# 만드는 것: Secret alertmanager-secrets(slack_webhook_url · healthchecks_ping_url),
#           Secret grafana-secrets(admin-user · admin-password · pg-ro-password)
# 값이 바뀌면 다시 실행하면 덮어쓴다. 그 뒤 해당 Pod를 재시작: kubectl -n monitoring rollout restart sts/alertmanager deploy/grafana
set -euo pipefail

NS=monitoring
echo "대상: $(kubectl config current-context) $(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
kubectl get ns "$NS" > /dev/null 2>&1 || kubectl create ns "$NS"

tmp="$(mktemp -d)"; chmod 700 "$tmp"; trap 'rm -rf "$tmp"' EXIT

# ask <파일이름> <안내문> [비워도 되면 기본값]
ask() {
  local v
  read -rsp "$2: " v; echo
  if [[ -z "$v" ]]; then
    [[ $# -ge 3 ]] || { echo "빈 값은 안 된다" >&2; exit 1; }
    v="$3"; echo "  (비워 둠 → '$3'로 저장. 나중에 다시 실행해 채운다)"
  fi
  printf '%s' "$v" > "$tmp/$1"
}

ask slack_webhook_url     "Slack Incoming Webhook URL (앱 Alertmanager, #alerts-gpu)"
ask healthchecks_ping_url "healthchecks.io ping URL (체크 alertmanager-watchdog)"
ask admin-password        "Grafana admin 비밀번호 (새로 정함)"
ask pg-ro-password        "PostgreSQL grafana_ro 비밀번호 (현수 DM, 없으면 Enter)" unset
printf 'admin' > "$tmp/admin-user"

kubectl -n "$NS" create secret generic alertmanager-secrets \
  --from-file="$tmp/slack_webhook_url" --from-file="$tmp/healthchecks_ping_url" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create secret generic grafana-secrets \
  --from-file="$tmp/admin-user" --from-file="$tmp/admin-password" --from-file="$tmp/pg-ro-password" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "완료. 확인: kubectl -n $NS get secret alertmanager-secrets grafana-secrets"
