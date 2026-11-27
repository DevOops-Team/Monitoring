#!/usr/bin/env bash
# Loki 알람 룰 검증 (GPU-218): 로컬 Loki 컨테이너에 deploy/monitoring-cluster/storage/loki/rules를 그대로 올리고 합성 커널 로그 줄을 넣어
#   ① 울려야 할 줄만 알람이 되는지 ② 라벨 · annotation이 분류기 계약(docs/classifier.md 2절)과 맞는지 본다.
# 사용: scripts/loki-rules-e2e.sh
#
# 실제 클러스터 · 실제 Loki(172.30.0.35)는 건드리지 않는다. 알람을 보낼 곳도 없는 주소다(분류기로 안 간다).
# promtool 같은 Loki 룰 단위 테스트 도구가 없어 실제 ruler를 띄워 본다. 룰 문법이 틀리면 알람이 안 생겨 여기서 실패한다.
# 한계: 넣는 줄은 NVIDIA 문서 형식으로 만든 합성 줄이다. 실제 드라이버 줄과 같은지는 EKS에서 확인해야 한다
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=grafana/loki:3.7.9                # deploy/monitoring-cluster/values/loki.yaml과 같은 버전
NAME=loki-rules-e2e
PORT=13100
WORK=$(mktemp -d)
trap 'docker rm -f "$NAME" > /dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

cat > "$WORK/loki.yaml" <<'EOF'
auth_enabled: false
server: {http_listen_port: 3100}
common:
  path_prefix: /tmp/loki
  replication_factor: 1
  ring: {kvstore: {store: inmemory}}
  storage: {filesystem: {chunks_directory: /tmp/loki/chunks, rules_directory: /tmp/loki/rules}}
schema_config:
  configs:
    - {from: "2024-01-01", store: tsdb, object_store: filesystem, schema: v13, index: {prefix: index_, period: 24h}}
limits_config: {allow_structured_metadata: true}
ruler:
  storage: {type: local, local: {directory: /etc/loki/rules}}
  rule_path: /tmp/loki/rules-temp
  alertmanager_url: http://127.0.0.1:9
  enable_api: true
EOF

docker rm -f "$NAME" > /dev/null 2>&1 || true
# 운영과 같은 자리(/etc/loki/rules/fake = 테넌트 fake)에 룰 디렉터리를 통째로 마운트한다
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:3100" \
  -v "$WORK/loki.yaml:/etc/loki/local-config.yaml:ro" -v "$PWD/deploy/monitoring-cluster/storage/loki/rules:/etc/loki/rules/fake:ro" \
  "$IMAGE" -config.file=/etc/loki/local-config.yaml > /dev/null
for _ in $(seq 1 60); do curl -s -m 2 "localhost:$PORT/ready" | grep -q '^ready' && break; sleep 2; done
curl -s -m 2 "localhost:$PORT/ready" | grep -q '^ready' || { docker logs "$NAME" 2>&1 | tail -20; echo "Loki가 뜨지 않았다" >&2; exit 1; }

# 운영과 같은 모양으로 넣는다: 인덱스 라벨 = service_name · k8s_cluster_name, 노드 이름은 구조화 메타데이터
push() {   # 인자: 클러스터, 노드, 줄
  local body
  body=$(python3 -c 'import json,sys,time; c,n,l=sys.argv[1:]; print(json.dumps({"streams":[{"stream":{"service_name":"kernel","k8s_cluster_name":c},"values":[[str(time.time_ns()),l,{"k8s_node_name":n}]]}]}))' "$@")
  curl -sf -o /dev/null -H 'Content-Type: application/json' "localhost:$PORT/loki/api/v1/push" -d "$body"
}
NODE_A=ip-172-28-67-31.us-east-2.compute.internal
NODE_B=ip-172-28-80-9.us-east-2.compute.internal
LINE_79='NVRM: Xid (PCI:0000:00:1e): 79, pid=1234, name=python, GPU has fallen off the bus.'
LINE_31='NVRM: Xid (PCI:0000:00:1e): 31, pid=28461, name=python3, Ch 00000008, intr 10000000. MMU Fault: ENGINE GRAPHICS GPCCLIENT_PBDMA0 faulted @ 0x0_00000000. Fault is of type FAULT_PDE ACCESS_TYPE_WRITE'
push eks-a "$NODE_A" "$LINE_79"                                  # 울린다
push eks-b "$NODE_B" "$LINE_31"                                  # 울린다
push eks-a "$NODE_A" 'zram0: system has too much memory (15785MB), limit is 800MB, ignoring.'   # Xid 줄이 아님
push eks-a "$NODE_A" 'NVRM: Xid handling changed, see documentation'                             # 문구만 같고 번호가 없음
push monitoring monitoring1 "$LINE_79"                           # EKS가 아님

echo "알람을 기다린다(평가 주기 15초)"
for _ in $(seq 1 12); do
  sleep 10
  curl -s "localhost:$PORT/prometheus/api/v1/alerts" > "$WORK/alerts.json"
  [[ $(python3 -c 'import json,sys; print(sum(a["state"]=="firing" for a in json.load(open(sys.argv[1]))["data"]["alerts"]))' "$WORK/alerts.json") -ge 2 ]] && break
done

# 분류기 파서(classifier/classifier/signals.py, 표준 라이브러리만 씀)에 그대로 넣어 본다
PYTHONPATH=classifier python3 - "$WORK/alerts.json" "$NODE_A" "$NODE_B" "$LINE_79" "$LINE_31" <<'EOF'
import json, sys
from datetime import datetime, timedelta, timezone
from classifier.signals import parse_alert

path, node_a, node_b, line_79, line_31 = sys.argv[1:]
alerts = [a for a in json.load(open(path))["data"]["alerts"] if a["state"] == "firing"]
got = {}
for a in alerts:
    s = parse_alert({"status": "firing", "labels": a["labels"], "annotations": a["annotations"]})
    assert s.alert == "FailoverXidKernel" and (s.signal, s.source) == ("xid", "dmesg"), a["labels"]
    assert s.gpu_uuid is None, "gpu_uuid는 분류기가 찾는다"
    assert datetime.now(timezone.utc) - s.ts < timedelta(minutes=5), f"event_ts가 줄 시각이 아니다: {s.ts}"
    got[(s.cluster, s.node, s.xid)] = s.log_line
want = {("eks-a", node_a, 79): line_79, ("eks-b", node_b, 31): line_31}
assert got == want, f"\n기대 {want}\n실제 {got}"
print(f"ok: 알람 {len(alerts)}건(Xid 79 · 31), 나머지 줄 3개는 안 울림, 분류기 파싱 통과")
EOF
