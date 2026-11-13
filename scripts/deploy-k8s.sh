#!/usr/bin/env bash
# 노트북에서 실행: 설정 검사 → 모니터링 클러스터(또는 kind)에 deploy/ 적용
# 사용: KUBECONFIG=<admin.conf> scripts/deploy-k8s.sh [--lint-only] [--kind] [--agents-kind]
#   --lint-only    검사만 하고 끝 (클러스터 접속 없음)
#   --kind         로컬 kind 클러스터에 저장소 스택(deploy/base · values)을 적용 (더미 Secret을 만들어 구조만 검증)
#   --agents-kind  로컬 kind 클러스터에 수집 층(deploy/agents/overlays/kind)만 적용. 데이터는 실제 모니터링 클러스터로 간다
# 수집 층의 관리 클러스터 · EKS 적용은 이 스크립트가 아니라 ArgoCD(K8s 레포 Application)가 한다
# 환경변수 CHART_CACHE=<폴더> 를 주면 그 안의 <차트>-<버전>.tgz를 쓴다(GitHub 다운로드가 불안정할 때)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
D="$REPO_DIR/deploy"
NS=monitoring
EXPECTED_SERVER="https://172.30.0.30:6443"   # 모니터링 클러스터 API VIP
# shellcheck disable=SC1091
. "$D/versions.env"

MODE=cluster
LINT_ONLY=false
for a in "$@"; do
  case "$a" in
    --kind) MODE=kind ;;
    --agents-kind) MODE=agents-kind ;;
    --lint-only) LINT_ONLY=true ;;
    *) echo "모르는 옵션: $a" >&2; exit 2 ;;
  esac
done

# 차트 위치: 캐시에 tgz가 있으면 그것, 없으면 레포에서 버전 고정으로
chart() {
  local name="${1##*/}" tgz
  tgz="${CHART_CACHE:-/nonexistent}/$name-$2.tgz"
  if [[ -f "$tgz" ]]; then echo "$tgz"; else echo "$1 --version $2"; fi
}

lint() {
  local tmp; tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' RETURN
  echo "== promtool check rules (Mimir ruler가 읽을 룰)"
  docker run --rm -v "$D/base/mimir:/mimir:ro" --entrypoint promtool "$PROMETHEUS_IMAGE" \
    check rules /mimir/rules/self.yml /mimir/rules/recovery.yml /mimir/rules/records.yml
  echo "== promtool test rules (룰 단위 테스트)"
  docker run --rm -v "$D/base/mimir:/mimir:ro" --entrypoint promtool "$PROMETHEUS_IMAGE" \
    test rules /mimir/tests/recovery_test.yml
  echo "== promtool check config --agent (Prometheus Agent)"
  # promtool은 credentials_file 존재까지 본다. Pod 안에만 있는 ServiceAccount 토큰 자리에 빈 파일을 마운트
  : > "$tmp/token"
  docker run --rm -v "$D/base/prometheus-agent:/etc/prometheus:ro" \
    -v "$tmp/token:/var/run/secrets/kubernetes.io/serviceaccount/token:ro" \
    --entrypoint promtool "$PROMETHEUS_IMAGE" check config --agent /etc/prometheus/prometheus.yml
  echo "== Grafana 대시보드 JSON (문법 · uid · title, uid 중복)"
  jq -e '.uid and .title' "$D"/base/grafana/dashboards/*/*.json > /dev/null
  [[ -z "$(jq -r '.uid' "$D"/base/grafana/dashboards/*/*.json | sort | uniq -d)" ]] || { echo "대시보드 uid 중복" >&2; return 1; }
  echo "== kustomize 렌더 (deploy/base)"
  kubectl kustomize "$D/base" > "$tmp/base.yaml"
  echo "   리소스 $(grep -c '^kind:' "$tmp/base.yaml")개"
  echo "== 수집 층 (deploy/agents): promtool + 렌더"
  docker run --rm -v "$D/agents/base/prometheus-agent:/etc/prometheus:ro" \
    -v "$tmp/token:/var/run/secrets/kubernetes.io/serviceaccount/token:ro" \
    -e CLUSTER_NAME=lint -e SITE=lint \
    --entrypoint promtool "$PROMETHEUS_IMAGE" check config --agent /etc/prometheus/prometheus.yml
  docker run --rm -v "$D/agents/base/fluent-bit/fluent-bit.yaml:/fluent-bit/etc/conf/fluent-bit.yaml:ro" \
    "$FLUENT_BIT_IMAGE" -c /fluent-bit/etc/conf/fluent-bit.yaml --dry-run
  docker run --rm -v "$D/agents/base/otel-gateway/config.yaml:/etc/otel/config.yaml:ro" \
    -e CLUSTER_NAME=lint -e SITE=lint \
    "$OTEL_COLLECTOR_IMAGE" validate --config=/etc/otel/config.yaml
  for o in "$D"/agents/overlays/*/; do
    kubectl kustomize "$o" > "$tmp/agents-$(basename "$o").yaml"
    echo "   overlays/$(basename "$o"): 리소스 $(grep -c '^kind:' "$tmp/agents-$(basename "$o").yaml")개"
  done
  echo "== helm template (values 문법)"
  # shellcheck disable=SC2046
  helm template local-path-provisioner $(chart containeroo/local-path-provisioner "$LOCAL_PATH_CHART") -f "$D/values/local-path.yaml" > /dev/null
  # shellcheck disable=SC2046
  helm template node-exporter $(chart prometheus-community/prometheus-node-exporter "$NODE_EXPORTER_CHART") -n "$NS" -f "$D/values/node-exporter.yaml" > /dev/null
  # shellcheck disable=SC2046
  helm template kube-state-metrics $(chart prometheus-community/kube-state-metrics "$KSM_CHART") -n "$NS" -f "$D/values/kube-state-metrics.yaml" > /dev/null
  # shellcheck disable=SC2046
  helm template grafana $(chart grafana-community/grafana "$GRAFANA_CHART") -n "$NS" -f "$D/values/grafana.yaml" > /dev/null
  # shellcheck disable=SC2046
  helm template alertmanager $(chart prometheus-community/alertmanager "$ALERTMANAGER_CHART") -n "$NS" \
    -f "$D/values/alertmanager.yaml" -s templates/configmap.yaml \
    | awk '/^  alertmanager.yml: \|/{f=1;next} f&&/^    /{sub(/^    /,"");print;next} f&&!/^ *$/{f=0}' > "$tmp/alertmanager.yml"
  [[ -s "$tmp/alertmanager.yml" ]] || { echo "alertmanager.yml 추출 실패" >&2; return 1; }
  for f in loki tempo blackbox; do
    [[ -f "$D/values/$f.yaml" ]] || continue
    case "$f" in
      loki)     c="$(chart grafana-community/loki "$LOKI_CHART")" ;;
      tempo)    c="$(chart grafana-community/tempo "$TEMPO_CHART")" ;;
      blackbox) c="$(chart prometheus-community/prometheus-blackbox-exporter "$BLACKBOX_CHART")" ;;
    esac
    # shellcheck disable=SC2086
    helm template "$f" $c -n "$NS" -f "$D/values/$f.yaml" > /dev/null
  done
  echo "== amtool check-config (차트가 만든 alertmanager.yml)"
  docker run --rm -v "$tmp:/cfg:ro" --entrypoint amtool "$ALERTMANAGER_IMAGE" check-config /cfg/alertmanager.yml
}

guard() {
  local server ctx
  server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  ctx="$(kubectl config current-context)"
  echo "== 대상: context=$ctx server=$server (mode=$MODE)"
  if [[ "$MODE" == cluster && "$server" != "$EXPECTED_SERVER" ]]; then
    echo "API 서버가 $EXPECTED_SERVER 가 아니다. KUBECONFIG를 확인하거나 --kind 를 써라" >&2; exit 1
  fi
  if [[ "$MODE" == kind || "$MODE" == agents-kind ]] && [[ "$ctx" != kind-* ]]; then
    echo "--kind / --agents-kind 인데 현재 컨텍스트가 kind-* 가 아니다" >&2; exit 1
  fi
}

ensure_secrets() {
  local missing=()
  for s in alertmanager-secrets grafana-secrets; do
    kubectl -n "$NS" get secret "$s" > /dev/null 2>&1 || missing+=("$s")
  done
  [[ ${#missing[@]} -eq 0 ]] && return
  if [[ "$MODE" == kind ]]; then
    echo "== kind: 더미 Secret 생성 (${missing[*]})"
    kubectl -n "$NS" create secret generic alertmanager-secrets \
      --from-literal=slack_webhook_url=http://127.0.0.1:9/slack \
      --from-literal=healthchecks_ping_url=http://127.0.0.1:9/ping --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n "$NS" create secret generic grafana-secrets \
      --from-literal=admin-user=admin --from-literal=admin-password=kind-only \
      --from-literal=pg-ro-password=unset --dry-run=client -o yaml | kubectl apply -f -
  else
    echo "Secret 없음: ${missing[*]} → 먼저 scripts/create-secrets.sh 를 실행하라" >&2; exit 1
  fi
}

apply() {
  echo "== StorageClass (local-path)"
  if [[ "$MODE" == kind ]]; then
    # kind는 같은 provisioner(rancher.io/local-path)를 standard라는 이름으로 이미 갖고 있다.
    # 차트를 또 깔면 충돌하므로, 매니페스트가 쓰는 이름(local-path)의 StorageClass만 하나 더 만든다
    kubectl apply -f - <<'SC'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-path
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
SC
  else
    # shellcheck disable=SC2046
    helm upgrade --install local-path-provisioner $(chart containeroo/local-path-provisioner "$LOCAL_PATH_CHART") \
      -n local-path-storage --create-namespace -f "$D/values/local-path.yaml" --wait --timeout 5m
  fi
  echo "== 자체 매니페스트 (namespace · Mimir · Prometheus Agent)"
  kubectl apply -k "$D/base"
  # kind에는 LoadBalancer에 IP를 주는 것이 없어 helm --wait가 끝나지 않는다 → kind에서만 ClusterIP로
  local svc_override=()
  [[ "$MODE" == kind ]] && svc_override=(--set service.type=ClusterIP)
  echo "== 차트 (node-exporter · kube-state-metrics)"
  # shellcheck disable=SC2046
  helm upgrade --install node-exporter $(chart prometheus-community/prometheus-node-exporter "$NODE_EXPORTER_CHART") \
    -n "$NS" -f "$D/values/node-exporter.yaml" --wait --timeout 5m
  # shellcheck disable=SC2046
  helm upgrade --install kube-state-metrics $(chart prometheus-community/kube-state-metrics "$KSM_CHART") \
    -n "$NS" -f "$D/values/kube-state-metrics.yaml" --wait --timeout 5m
  for f in loki tempo blackbox; do
    [[ -f "$D/values/$f.yaml" ]] || continue
    case "$f" in
      loki)     c="$(chart grafana-community/loki "$LOKI_CHART")" ;;
      tempo)    c="$(chart grafana-community/tempo "$TEMPO_CHART")" ;;
      blackbox) c="$(chart prometheus-community/prometheus-blackbox-exporter "$BLACKBOX_CHART")" ;;
    esac
    echo "== 차트 ($f)"
    # shellcheck disable=SC2086
    helm upgrade --install "$f" $c -n "$NS" -f "$D/values/$f.yaml" --wait --timeout 8m
  done
  # 여기까지는 Secret 없이 뜬다(수집 · 저장은 이미 시작). Alertmanager · Grafana는 Secret이 있어야 한다
  ensure_secrets
  echo "== 차트 (Alertmanager · Grafana)"
  # shellcheck disable=SC2046
  helm upgrade --install alertmanager $(chart prometheus-community/alertmanager "$ALERTMANAGER_CHART") \
    -n "$NS" -f "$D/values/alertmanager.yaml" ${svc_override[@]+"${svc_override[@]}"} --wait --timeout 5m
  # shellcheck disable=SC2046
  helm upgrade --install grafana $(chart grafana-community/grafana "$GRAFANA_CHART") \
    -n "$NS" -f "$D/values/grafana.yaml" ${svc_override[@]+"${svc_override[@]}"} --wait --timeout 8m
  echo "== 롤아웃 대기"
  kubectl -n "$NS" rollout status statefulset/mimir --timeout=5m
  kubectl -n "$NS" rollout status statefulset/prometheus-agent --timeout=5m
  kubectl -n "$NS" get pods,pvc,svc
}

apply_agents_kind() {
  echo "== 수집 층 → kind (overlays/kind)"
  kubectl apply -k "$D/agents/overlays/kind"
  for d in kube-state-metrics prometheus-agent otel-gateway; do
    kubectl -n "$NS" rollout status deployment/"$d" --timeout=3m
  done
  kubectl -n "$NS" rollout status daemonset/fluent-bit --timeout=3m
  kubectl -n "$NS" rollout status daemonset/node-exporter --timeout=3m
  kubectl -n "$NS" get pods
}

lint
$LINT_ONLY && { echo "== lint만 하고 끝"; exit 0; }
guard
if [[ "$MODE" == agents-kind ]]; then apply_agents_kind; else apply; fi
