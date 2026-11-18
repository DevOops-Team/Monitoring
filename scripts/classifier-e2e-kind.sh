#!/usr/bin/env bash
# 분류기 종단 검증: 노트북 kind에서 웹훅 → 판정 → Kafka gpu.fault까지 본다 (docs/classifier.md, deploy/README.md 「분류기」)
# 사용: scripts/classifier-e2e-kind.sh [--delete]
#   --delete  검증용 kind 클러스터를 지우고 끝
#
# 무엇이 진짜이고 무엇이 임시인가
#   - Kafka: kind 안의 임시 브로커 1대. 실제 브로커(.40~.42)에는 쓰지 않는다
#   - Mimir: 실제 모니터링 클러스터(172.30.0.35). kind의 수집 층(overlays/kind)이 cluster="kind-test"로 지표 · 로그를 보내고,
#            분류기는 그것을 읽어 job_id · collector_healthy를 채운다 (deploy-k8s.sh --agents-kind와 같은 방식)
#   - kubeconfig: 따로 둔다. 평소 쓰는 ~/.kube/config의 현재 컨텍스트를 바꾸지 않는다
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=clf-e2e
NODE="$NAME-worker"                     # Xid가 났다고 할 노드. 작업 e2e00001의 Pod만 여기 둔다
OTHER_NODE="$NAME-worker2"              # 나머지 가짜 Pod. 한 노드에 작업이 둘이면 Xid의 job_id를 특정할 수 없다(의도한 동작)
CLUSTER=kind-test                       # deploy/agents/overlays/kind의 CLUSTER_NAME
MIMIR=http://172.30.0.35:9009
PORT=18080
WORK="${TMPDIR:-/tmp}/classifier-e2e"
export KUBECONFIG="$WORK/kubeconfig"
PY=classifier/.venv/bin/python
GPU=GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7
K="kubectl -n monitoring"

if [[ "${1:-}" == "--delete" ]]; then
  kind delete cluster --name "$NAME"; rm -rf "${WORK:?}"; exit 0
fi
[[ -x "$PY" ]] || { echo "classifier/.venv 가 없다: python3.12 -m venv classifier/.venv && classifier/.venv/bin/pip install -r classifier/requirements-dev.txt" >&2; exit 1; }
mkdir -p "$WORK"

step() { echo; echo "== $*"; }
fail() { echo "   실패: $*" >&2; exit 1; }
ago() { python3 -c "import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=int(sys.argv[1]))).strftime('%Y-%m-%dT%H:%M:%SZ'))" "$1"; }
mimir_count() { curl -s -G "$MIMIR/prometheus/api/v1/query" --data-urlencode "query=$1" | python3 -c "import json,sys; r=json.load(sys.stdin)['data']['result']; print(int(float(r[0]['value'][1])) if r else 0)"; }

# 웹훅 1건을 보내고 응답이 기대와 같은지 본다. 인자: 설명, 기대 응답, 라벨 JSON, 신호가 난 지 몇 초 전
send() {
  local got
  got=$(curl -s -X POST "localhost:$PORT/alerts" -H 'Content-Type: application/json' \
    -d "{\"alerts\":[{\"status\":\"firing\",\"labels\":$3,\"annotations\":{\"event_ts\":\"$(ago "$4")\"}}]}")
  [[ "$got" == "$2" ]] && echo "   ok   $1 → $got" || fail "$1: 기대 $2, 실제 $got"
}
xid_labels() { echo "{\"alertname\":\"GpuXid\",\"pipeline\":\"failover\",\"signal\":\"xid\",\"source\":\"dmesg\",\"cluster\":\"$CLUSTER\",\"node\":\"$NODE\",\"gpu_uuid\":\"$GPU\",\"xid\":\"79\"}"; }
pod_labels() { echo "{\"alertname\":\"TrainerExited\",\"pipeline\":\"failover\",\"signal\":\"pod_exit\",\"cluster\":\"$CLUSTER\",\"node\":\"$2\",\"pod\":\"$1\",\"exit_code\":\"137\"}"; }

forward() {
  if [[ -n "${PF:-}" ]]; then kill "$PF" 2>/dev/null || true; wait "$PF" 2>/dev/null || true; fi
  $K port-forward svc/classifier "$PORT:8080" > "$WORK/port-forward.log" 2>&1 & PF=$!
  for _ in $(seq 1 40); do curl -s -o /dev/null "localhost:$PORT/healthz" && return; sleep 0.5; done
  fail "port-forward"
}
trap '[[ -n "${PF:-}" ]] && kill "$PF" 2>/dev/null || true' EXIT

step "kind 클러스터 ($NAME)"
if kind get clusters 2>/dev/null | grep -qx "$NAME"; then
  kind export kubeconfig --name "$NAME" --kubeconfig "$KUBECONFIG" > /dev/null
else
  kind create cluster --name "$NAME" --kubeconfig "$KUBECONFIG" --config deploy/classifier/overlays/kind/kind-cluster.yaml
fi
# kind 노드에는 /var/log/journal이 없어 Fluent Bit(systemd 입력)가 뜨지 못한다. 수집 경로 판정(up{job="fluent-bit"})에 필요하다
for n in $(kind get nodes --name "$NAME"); do
  docker exec "$n" sh -c 'mkdir -p /var/log/journal && journalctl --flush'
done

step "분류기 이미지 빌드 → kind"
docker build -q -f classifier/Dockerfile -t classifier:kind \
  --build-arg RULE_VERSION="$(git log -1 --format=%h -- classifier/classifier/rules.yaml)" . > /dev/null
# kind load docker-image는 Docker의 containerd 이미지 저장소에서 실패하는 경우가 있어 파일로 넘긴다
docker save classifier:kind -o "$WORK/classifier.tar"
kind load image-archive "$WORK/classifier.tar" --name "$NAME"

step "수집 층 (deploy/agents/overlays/kind) — 지표가 실제 Mimir로 간다"
scripts/deploy-k8s.sh --agents-kind > "$WORK/agents.log" 2>&1 || { tail -20 "$WORK/agents.log"; fail "수집 층 적용"; }

step "분류기 + 임시 Kafka (deploy/classifier/overlays/kind)"
kubectl apply -k deploy/classifier/overlays/kind > /dev/null
$K rollout status deploy/kafka --timeout=300s > /dev/null
$K wait --for=condition=complete job/kafka-topics --timeout=180s > /dev/null
$K rollout restart deploy/classifier > /dev/null        # 다시 돌릴 때도 방금 빌드한 이미지로
$K rollout status deploy/classifier --timeout=180s > /dev/null

step "작업 라벨이 붙은 가짜 학습 Pod 3개"
kubectl create namespace training --dry-run=client -o yaml | kubectl apply -f - > /dev/null
pod_name() { printf 'job-%s-g%04d-w-0-0' "$1" "$2"; }
for spec in "e2e00001:3:$NODE" "e2e00002:1:$OTHER_NODE" "e2e00003:2:$OTHER_NODE"; do
  IFS=: read -r job gen node <<< "$spec"
  kubectl -n training run "$(pod_name "$job" "$gen")" --image=busybox:1.37.0 --restart=Never \
    --labels="devoops/job-id=$job,devoops/generation=$gen" --overrides="{\"spec\":{\"nodeName\":\"$node\"}}" \
    --dry-run=client -o yaml -- sleep 86400 | kubectl apply -f - > /dev/null
done
echo "   Mimir에 라벨과 수집 경로 지표가 보일 때까지 기다린다 (최대 3분)"
for _ in $(seq 1 36); do
  labels=$(mimir_count "count(kube_pod_labels{cluster=\"$CLUSTER\",label_devoops_job_id=~\"e2e0000[123]\"})")
  ups=$(mimir_count "count(up{cluster=\"$CLUSTER\",job=~\"prometheus-agent|otel-gateway\"} == 1 or up{cluster=\"$CLUSTER\",job=\"fluent-bit\",node=\"$NODE\"} == 1)")
  [[ "$labels" == 3 && "$ups" == 3 ]] && break
  sleep 5
done
[[ "$labels" == 3 && "$ups" == 3 ]] || fail "Mimir: 작업 라벨 $labels/3, 수집 경로 $ups/3"

forward
# 이 실행 전에 쌓여 있던 gpu.fault 수. 스크립트를 여러 번 돌려도 이번 실행분만 센다
offset() { $K exec deploy/kafka -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic gpu.fault 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'; }
before=$(offset)
# 중복 제거 창(±45초)에 앞 실행이 걸리지 않게 GPU마다 다른 대상을 쓴다
GPU="GPU-$(python3 -c 'import secrets; print(secrets.token_hex(4))')-d5ea-c752-ddf3-c4453e44d3f7"

step "1. Xid 79 → 이벤트 1건, 재전송은 중복"
send "Xid 79" '{"published":1}' "$(xid_labels)" 20
send "같은 알람 재전송" '{"duplicate":1}' "$(xid_labels)" 20

step "2. 분류기를 재시작해도 같은 사건을 또 내지 않는다 (Kafka에서 기억 복원)"
$K rollout restart deploy/classifier > /dev/null
$K rollout status deploy/classifier --timeout=180s > /dev/null
forward
send "재시작 뒤 같은 알람" '{"duplicate":1}' "$(xid_labels)" 20

step "3. 의도된 종료: job.fenced 뒤의 Pod 종료는 버린다"
echo "{\"event_id\":\"job-e2e00002-g0001-fenced\",\"schema_version\":1,\"type\":\"job.fenced\",\"occurred_at\":\"$(ago 50)\",\"source\":\"job-watcher\",\"job_id\":\"e2e00002\",\"generation\":1,\"payload\":{}}" \
  | $K exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic job.lifecycle
sleep 3
send "fenced된 세대의 Pod 종료" '{"intended":1}' "$(pod_labels "$(pod_name e2e00002 1)" "$OTHER_NODE")" 40

step "4. 기록이 없는 Pod 종료는 장애로 낸다"
send "Pod 종료(종료 코드 137)" '{"published":1}' "$(pod_labels "$(pod_name e2e00003 2)" "$OTHER_NODE")" 40

step "5. 원인 사건(1번의 Xid)을 낸 세대의 Pod 종료는 따로 내지 않는다"
send "Xid가 난 작업의 Pod 종료" '{"absorbed":1}' "$(pod_labels "$(pod_name e2e00001 3)" "$NODE")" 40

step "6. Kafka gpu.fault에 쌓인 이번 실행분이 v1 스키마를 통과하는가"
after=$(offset)
[[ $((after - before)) == 2 ]] || fail "이번 실행에서 낸 메시지 $((after - before))건 (기대 2건)"
$K exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gpu.fault \
  --from-beginning --timeout-ms 6000 --property print.headers=true 2>/dev/null > "$WORK/gpu.fault.tsv" || true
"$PY" - "$WORK/gpu.fault.tsv" "$GPU" <<'PY'
import json, sys
sys.path.insert(0, "classifier")
from classifier.publish import validate

path, gpu = sys.argv[1:]
rows = [line.rstrip("\n").split("\t") for line in open(path, encoding="utf-8") if line.strip()]
mine = [(h, json.loads(v)) for h, v in rows if json.loads(v)["payload"]["job_id"] in ("e2e00001", "e2e00003")]
mine = sorted(mine, key=lambda x: x[1]["occurred_at"])[-2:]     # 파티션 순서로 읽히므로 시각으로 이번 실행분 2건을 고른다
got = {}
for headers, env in mine:
    e = env["payload"]
    validate(e)
    assert env["event_id"] == e["event_id"] and env["source"] == "classifier" and headers.startswith("rule_version:")
    got[e["job_id"]] = (env["type"], e["category"], e["suggested_action"], e["generation"], e["collector_healthy"])
    print(f"   ok   {env['type']:15} job={e['job_id']} g{e['generation']} {e['category']}/{e['suggested_action']} "
          f"collector_healthy={e['collector_healthy']} {headers}")
assert got == {"e2e00001": ("fault.detected", "hw", "isolate_migrate", 3, True),
               "e2e00003": ("fault.detected", "app", "restart_in_place", 2, True)}, got
PY

step "7. 지표"
curl -s "localhost:$PORT/metrics" | grep -E '^classifier_(lifecycle_healthy|alerts_total)' | sed 's/^/   /'
curl -s "localhost:$PORT/metrics" | grep -q '^classifier_lifecycle_healthy 1.0' || fail "classifier_lifecycle_healthy가 1이 아니다"

echo
echo "통과. 클러스터는 남겨 둔다 → 지우기: scripts/classifier-e2e-kind.sh --delete"
