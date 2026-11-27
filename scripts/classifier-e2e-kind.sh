#!/usr/bin/env bash
# 분류기 종단 검증: 노트북 kind에서 웹훅 → 판정 → Kafka gpu.fault까지 본다 (docs/classifier.md, deploy/README.md 「분류기」)
# 사용: scripts/classifier-e2e-kind.sh [--delete]
#   --delete  검증용 kind 클러스터를 지우고 끝
#
# 무엇이 진짜이고 무엇이 임시인가
#   - Kafka: kind 안의 임시 브로커 1대. 실제 브로커(.40~.42)에는 쓰지 않는다
#   - Mimir: 실제 모니터링 클러스터(172.30.0.35). kind의 수집 층(deploy/kind/agents)이 cluster="kind-test"로 지표 · 로그를 보내고,
#            분류기는 그것을 읽어 job_id · collector_healthy를 채운다 (deploy-k8s.sh --agents-kind와 같은 방식)
#   - kubeconfig: 따로 둔다. 평소 쓰는 ~/.kube/config의 현재 컨텍스트를 바꾸지 않는다
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=clf-e2e
NODE="$NAME-worker"                     # Xid가 났다고 할 노드. 작업 e2e00001의 Pod만 여기 둔다
OTHER_NODE="$NAME-worker2"              # 나머지 가짜 Pod. 한 노드에 작업이 둘이면 Xid의 job_id를 특정할 수 없다(의도한 동작)
LOST_NODE="$NAME-worker3"               # 9단계에서 멈추고 지울 GPU 노드(taint로 다른 Pod는 안 올라감)
CLUSTER=kind-test                       # deploy/kind/agents의 CLUSTER_NAME
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
  got=$(curl -s -X POST "localhost:$PORT/alerts" -H 'Content-Type: application/json' -d "$(alert_body "$3" "$4")")
  [[ "$got" == "$2" ]] && echo "   ok   $1 → $got" || fail "$1: 기대 $2, 실제 $got"
}
alert_body() { echo "{\"alerts\":[{\"status\":\"firing\",\"labels\":$1,\"annotations\":{\"event_ts\":\"$(ago "$2")\"}}]}"; }
leader() { $K get lease classifier -o jsonpath='{.spec.holderIdentity}'; }
is_leader() { kubectl get --raw "/api/v1/namespaces/monitoring/pods/$1:8080/proxy/metrics" 2>/dev/null | grep -q '^classifier_leader 1.0'; }
xid_labels() { echo "{\"alertname\":\"GpuXid\",\"pipeline\":\"failover\",\"signal\":\"xid\",\"source\":\"dmesg\",\"cluster\":\"$CLUSTER\",\"node\":\"$NODE\",\"gpu_uuid\":\"$GPU\",\"xid\":\"79\"}"; }
pod_labels() { echo "{\"alertname\":\"TrainerExited\",\"pipeline\":\"failover\",\"signal\":\"pod_exit\",\"cluster\":\"$CLUSTER\",\"node\":\"$2\",\"pod\":\"$1\",\"reason\":\"Error\"}"; }

# 인자가 없으면 Service로, 있으면 그 Pod 하나로 붙는다(예: forward pod/이름)
# 포트가 이미 쓰이고 있으면 멈춘다. 실제 분류기로 가는 port-forward가 떠 있으면 healthz는 응답하므로,
# 확인 없이 진행하면 시험 알람이 실제 gpu.fault로 나간다(11/24 18:15 실제로 2건 나감)
forward() {
  if [[ -n "${PF:-}" ]]; then kill "$PF" 2>/dev/null || true; wait "$PF" 2>/dev/null || true; fi
  if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN > /dev/null 2>&1; then
    fail "포트 $PORT 를 다른 프로세스가 쓰고 있다(실제 분류기 port-forward일 수 있음): $(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN | tail -n +2 | head -1)"
  fi
  $K port-forward "${1:-svc/classifier}" "$PORT:8080" > "$WORK/port-forward.log" 2>&1 & PF=$!
  for _ in $(seq 1 40); do
    if curl -s -o /dev/null "localhost:$PORT/healthz"; then
      kill -0 "$PF" 2>/dev/null || fail "port-forward가 죽었다($(tail -1 "$WORK/port-forward.log")) — 다른 곳이 응답하고 있다"
      return
    fi
    sleep 0.5
  done
  fail "port-forward"
}
trap '[[ -n "${PF:-}" ]] && kill "$PF" 2>/dev/null || true' EXIT

step "kind 클러스터 ($NAME)"
if kind get clusters 2>/dev/null | grep -qx "$NAME"; then
  kind export kubeconfig --name "$NAME" --kubeconfig "$KUBECONFIG" > /dev/null
  for n in $(kind get nodes --name "$NAME"); do docker start "$n" > /dev/null; done   # 앞 실행의 9단계가 멈춘 노드
  kubectl wait --for=condition=ready node --all --timeout=180s > /dev/null
else
  kind create cluster --name "$NAME" --kubeconfig "$KUBECONFIG" --config deploy/kind/classifier/kind-cluster.yaml
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

step "수집 층 (deploy/kind/agents) — 지표가 실제 Mimir로 간다"
scripts/deploy-k8s.sh --agents-kind > "$WORK/agents.log" 2>&1 || { tail -20 "$WORK/agents.log"; fail "수집 층 적용"; }

step "분류기 + 임시 Kafka (deploy/kind/classifier)"
kubectl apply -k deploy/kind/classifier > /dev/null
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
echo "   Mimir에 라벨과 수집 경로 지표가 보일 때까지 기다린다 (최대 4분)"
# 라벨은 1분 전에도 있었어야 한다: 아래에서 「40초 전에 난 신호」를 보내고, 분류기는 그 시각의 라벨로 작업을 찾는다
for _ in $(seq 1 48); do
  labels=$(mimir_count "count(kube_pod_labels{cluster=\"$CLUSTER\",label_devoops_job_id=~\"e2e0000[123]\"} offset 1m)")
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

step "3. 대장이 멈추면 대기 쪽이 넘겨받고, 같은 사건을 또 내지 않는다"
old=$(leader)
new=$($K get pods -l app.kubernetes.io/name=classifier -o name | grep -v "$old" | head -1); new=${new#pod/}
[[ -n "$old" && -n "$new" ]] || fail "대장 · 대기를 찾지 못함"
forward "pod/$new"
send "대기 쪽으로 보낸 알람(대장에게 넘겨줌)" '{"duplicate":1}' "$(xid_labels)" 20
# 프로세스를 얼린다 = 노드가 멈춘 것과 같다. 정상 종료가 아니라 Lease를 비우지 못하므로 대기 쪽은 만료(15초)를 기다린다
old_node=$($K get pod "$old" -o jsonpath='{.spec.nodeName}')
docker exec "$old_node" pkill -STOP -f 'uvicorn classifier.app'
start=$SECONDS
for _ in $(seq 1 60); do is_leader "$new" && break; sleep 1; done
is_leader "$new" || fail "60초 안에 넘겨받지 못함"
echo "   ok   넘겨받음 $old → $new ($((SECONDS - start))초)"
send "넘겨받은 뒤 같은 알람" '{"duplicate":1}' "$(xid_labels)" 20
# 깨어난 옛 대장은 Lease가 남의 것이 된 것을 보고 스스로 끝나 대기 쪽으로 다시 뜬다
docker exec "$old_node" pkill -CONT -f 'uvicorn classifier.app'
for _ in $(seq 1 30); do
  [[ "$($K get pod "$old" -o jsonpath='{.status.containerStatuses[0].restartCount}')" -ge 1 ]] && break; sleep 1
done
[[ "$($K get pod "$old" -o jsonpath='{.status.containerStatuses[0].restartCount}')" -ge 1 ]] || fail "옛 대장이 내려놓지 않음"
$K wait --for=condition=ready "pod/$old" --timeout=60s > /dev/null
is_leader "$old" && fail "옛 대장이 다시 뜬 뒤에도 대장이라고 한다"
[[ "$(leader)" == "$new" ]] || fail "Lease 주인이 $new 가 아니다: $(leader)"
echo "   ok   옛 대장은 재시작해 대기가 됨"
forward

step "4. 의도된 종료: job.fenced 뒤의 Pod 종료는 버린다"
echo "{\"event_id\":\"job-e2e00002-g0001-fenced\",\"schema_version\":1,\"type\":\"job.fenced\",\"occurred_at\":\"$(ago 50)\",\"source\":\"job-watcher\",\"job_id\":\"e2e00002\",\"generation\":1,\"payload\":{}}" \
  | $K exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic job.lifecycle
sleep 3
send "fenced된 세대의 Pod 종료" '{"intended":1}' "$(pod_labels "$(pod_name e2e00002 1)" "$OTHER_NODE")" 40

step "5. 기록이 없는 Pod 종료는 장애로 낸다"
send "Pod 종료(종료 코드 137)" '{"published":1}' "$(pod_labels "$(pod_name e2e00003 2)" "$OTHER_NODE")" 40

step "6. 원인 사건(1번의 Xid)을 낸 세대의 Pod 종료는 따로 내지 않는다"
send "Xid가 난 작업의 Pod 종료" '{"absorbed":1}' "$(pod_labels "$(pod_name e2e00001 3)" "$NODE")" 40

step "7. Kafka gpu.fault에 쌓인 이번 실행분이 v1 스키마를 통과하는가"
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

step "8. 지표 (대장 Pod. 대기 쪽은 job.lifecycle을 읽지 않아 classifier_lifecycle_healthy가 0이다)"
kubectl get --raw "/api/v1/namespaces/monitoring/pods/$(leader):8080/proxy/metrics" > "$WORK/metrics.txt"
grep -E '^classifier_(leader|lifecycle_healthy|alerts_total)' "$WORK/metrics.txt" | sed 's/^/   /'
grep -q '^classifier_lifecycle_healthy 1.0' "$WORK/metrics.txt" || fail "classifier_lifecycle_healthy가 1이 아니다"

step "9. GPU 노드 소실: 노드를 멈추고 지우면 룰이 잡고, 그 노드 작업의 isolate_migrate 1건이 나간다"
# 가짜 GPU(Node status의 extended resource) + 작업 e2e00004 Pod. 지우기 전에 Mimir에 Ready · GPU · Running이 보여야 한다
kubectl patch node "$LOST_NODE" --subresource=status --type=merge -p '{"status":{"capacity":{"nvidia.com/gpu":"1"}}}' > /dev/null
kubectl -n training delete pod "$(pod_name e2e00004 1)" --ignore-not-found --wait=false > /dev/null
kubectl -n training run "$(pod_name e2e00004 1)" --image=busybox:1.37.0 --restart=Never \
  --labels="devoops/job-id=e2e00004,devoops/generation=1" --dry-run=client -o yaml \
  --overrides="{\"spec\":{\"nodeName\":\"$LOST_NODE\",\"tolerations\":[{\"operator\":\"Exists\"}]}}" -- sleep 86400 | kubectl apply -f - > /dev/null
kubectl -n training wait --for=condition=ready "pod/$(pod_name e2e00004 1)" --timeout=120s > /dev/null
echo "   Mimir에 GPU 노드 · Running Pod가 보일 때까지 기다린다 (최대 4분)"
lost_ready="kube_node_status_condition{job=\"kube-state-metrics\",cluster=\"$CLUSTER\",node=\"$LOST_NODE\",condition=\"Ready\",status=\"true\"}"
for _ in $(seq 1 48); do
  gpu=$(mimir_count "count(kube_node_status_allocatable{cluster=\"$CLUSTER\",node=\"$LOST_NODE\",resource=\"nvidia_com_gpu\"} > 0 and on (node) $lost_ready == 1)")
  run=$(mimir_count "count(kube_pod_labels{cluster=\"$CLUSTER\",label_devoops_job_id=\"e2e00004\"} and on (namespace, pod) kube_pod_status_phase{cluster=\"$CLUSTER\",phase=\"Running\"} == 1)")
  [[ "$gpu" == 1 && "$run" == 1 ]] && break
  sleep 5
done
[[ "$gpu" == 1 && "$run" == 1 ]] || fail "Mimir: GPU 노드 $gpu/1, Running Pod $run/1"
sleep 20                                # 그 시각 직전에도 Running이었어야 분류기가 작업을 찾는다
docker stop "$LOST_NODE" > /dev/null
kubectl delete node "$LOST_NODE" --wait=false > /dev/null
echo "   $LOST_NODE 멈춤 + Node 삭제. 룰 식이 이 노드를 잡을 때까지 기다린다 (최대 3분, for 1m은 Mimir ruler 몫이라 여기선 식만)"
# 실제 룰 식에서 대상 클러스터만 kind-test로 바꾼다. 실제 ruler는 eks-a · b · c만 보므로 kind는 실제 알람이 되지 않는다
rule_expr=$("$PY" -c '
import sys, yaml
r = yaml.safe_load(open("deploy/monitoring-cluster/storage/mimir/rules/failover.yml"))
e = next(x for g in r["groups"] for x in g["rules"] if x.get("alert") == "FailoverGpuNodeLost")["expr"]
print(e.replace("cluster=~\"eks-(a|b|c)\"", "cluster=\"" + sys.argv[1] + "\""))' "$CLUSTER")
for _ in $(seq 1 36); do
  caught=$(mimir_count "count(($rule_expr) and on (node) label_replace(vector(1), \"node\", \"$LOST_NODE\", \"\", \"\"))" 2>/dev/null || echo 0)
  [[ "$caught" == 1 ]] && break
  sleep 5
done
[[ "$caught" == 1 ]] || fail "룰 식이 $LOST_NODE 를 잡지 못함"
echo "   ok   룰 식이 $LOST_NODE 를 잡음"
# event_ts = 룰 annotation과 같은 식(마지막 Ready 샘플 시각)
last_ready=$(curl -s -G "$MIMIR/prometheus/api/v1/query" \
  --data-urlencode "query=max_over_time((timestamp($lost_ready) and $lost_ready == 1)[15m:15s])" \
  | python3 -c "import json,sys,time; r=json.load(sys.stdin)['data']['result']; print(int(time.time()-float(r[0]['value'][1])))")
lost_labels="{\"alertname\":\"FailoverGpuNodeLost\",\"pipeline\":\"failover\",\"signal\":\"node_lost\",\"cluster\":\"$CLUSTER\",\"node\":\"$LOST_NODE\"}"
before9=$(offset)
send "노드 소실(마지막 Ready ${last_ready}초 전)" '{"published":1}' "$lost_labels" "$last_ready"
send "같은 알람 재전송" '{"duplicate":1}' "$lost_labels" "$last_ready"
[[ $(($(offset) - before9)) == 1 ]] || fail "9단계에서 낸 메시지 $(($(offset) - before9))건 (기대 1건)"
$K exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gpu.fault \
  --from-beginning --timeout-ms 6000 2>/dev/null > "$WORK/gpu.fault.9.jsonl" || true
"$PY" - "$WORK/gpu.fault.9.jsonl" "$LOST_NODE" <<'PY'
import json, sys
sys.path.insert(0, "classifier")
from classifier.publish import validate

path, node = sys.argv[1:]
env = sorted((json.loads(l) for l in open(path, encoding="utf-8") if l.strip()
              and json.loads(l)["payload"]["job_id"] == "e2e00004"), key=lambda x: x["occurred_at"])[-1]
e = env["payload"]
validate(e)
got = (env["type"], e["source"], e["category"], e["suggested_action"], e["node"], e["generation"], e["collector_healthy"])
print(f"   ok   {got}")
assert got == ("fault.detected", "node", "infra", "isolate_migrate", node, 1, True), got
PY

echo
echo "통과. 클러스터는 남겨 둔다 → 지우기: scripts/classifier-e2e-kind.sh --delete"
