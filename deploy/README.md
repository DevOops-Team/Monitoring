# 모니터링 스택 (k8s) — 런북

모니터링 클러스터(monitoring1~3, `docs/monitoring-cluster.md`)에 올리는 스택. `compose/`(VM 1대)를 대체한다. 이 문서가 MON-STD-22 "모니터링 클러스터 복구 런북"이다. 부 담당이 이 절차만으로 다시 올릴 수 있어야 한다.

ADR-18 1단계 상태: 저장소는 **복제 1 · PVC filesystem**. 3노드 복제 · S3 · ArgoCD · Fluent Bit/OTel 게이트웨이 · 분류기는 아직 없다.

## 구성

| 구성물 | 방법 | 복제 | PVC | 뜬 노드 (11/09) | 클러스터 안 주소 | 밖 주소 (서비스 VIP) |
|---|---|---|---|---|---|---|
| StorageClass `local-path` | Helm `containeroo/local-path-provisioner` | 1 | - | - | - | - |
| Mimir 3.2.4 (지표 저장 · 룰 평가) | `base/mimir/` | 1 | 10Gi | monitoring3 | `mimir.monitoring.svc:9009` | `172.30.0.35:9009` |
| Prometheus Agent v3.15.1 (스크레이프 → Mimir) | `base/prometheus-agent/` | 1 | 2Gi (WAL) | monitoring1 | `prometheus-agent:9090` | - |
| Loki 3.7.9 | Helm `grafana-community/loki` | 1 | 10Gi | monitoring2 | `loki.monitoring.svc:3100` | `172.30.0.35:3100` |
| Tempo 3.1.1 | Helm `grafana-community/tempo` | 1 | 5Gi | monitoring1 | `tempo.monitoring.svc:3200`(조회) | `172.30.0.35:4317` · `:4318` (OTLP만) |
| Alertmanager v0.34.2 | Helm `prometheus-community/alertmanager` | 1 | 1Gi | Secret 뒤 | `alertmanager.monitoring.svc:9093` | `172.30.0.35:9093` |
| Grafana 13.3.0 | Helm `grafana-community/grafana` | 1 | 2Gi | Secret 뒤 | `grafana.monitoring.svc:3000` | `http://172.30.0.35:3000` |
| node-exporter 1.12.1 | Helm | 노드마다 | - | 3대 | - | - |
| blackbox_exporter v0.29.1 | Helm | 1 | - | monitoring2 | `blackbox-exporter:9115` | - |

- 버전은 `versions.env` 한 곳에. Grafana · Loki · Tempo 차트는 2026-01부터 `grafana-community/helm-charts`(옛 `grafana/*`는 deprecated)
- PVC는 local-path라 **만들어진 노드에 묶인다.** 그 노드가 죽으면 해당 저장소가 멈춘다(복제 1 단계의 한계, 3노드 복제에서 해소). cAdvisor는 따로 안 깔고 kubelet 내장 것을 긁는다
- 밖으로 나가는 문은 전부 `type: LoadBalancer` + `kube-vip.io/loadbalancerIPs: "172.30.0.35"`, 포트로 공유. Loki · Tempo는 차트 Service를 내부용으로 두고 `base/external/services.yaml`이 필요한 포트만 연다

수집 대상(Agent `base/prometheus-agent/prometheus.yml`): kubelet 3 · cAdvisor 3 · node-exporter 3 · 스택 자신 · blackbox probe(pfSense 172.30.0.1 · 인터넷 1.1.1.1) · **Proxmox 호스트 5대(`job="pve-host"`)** · Kafka JMX(`job="kafka-jmx"`).
룰: `base/mimir/rules/self.yml` 4개(Watchdog · MonitoringNodeDiskWarning/Critical · MonitoringTargetDown), Mimir ruler가 15초마다 평가.

## 적용 (처음 · 변경 · 재설치 모두 같은 명령)

```bash
# 1회: 차트 레포
helm repo add grafana-community https://grafana-community.github.io/helm-charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add containeroo https://charts.containeroo.ch

export KUBECONFIG=~/gpu-failover/iac/ansible/inventory/monitoring/artifacts/admin.conf
scripts/create-secrets.sh        # 1회(값이 바뀌면 다시). 사람이 터미널에서, 입력은 화면에 안 보임
scripts/deploy-k8s.sh            # 검사(lint) → 적용. API 서버가 172.30.0.30이 아니면 거부한다
```

- `scripts/deploy-k8s.sh --lint-only`: promtool(룰 · Agent 설정) · kustomize 렌더 · helm template · amtool만
- `scripts/deploy-k8s.sh --kind`: 로컬 kind에 더미 Secret으로 올려 구조 확인(`kind create cluster` 뒤). kind에서는 LoadBalancer IP가 안 나오고, ICMP · 이미지 아키텍처가 실 노드와 달라 **kind 통과가 실 클러스터 통과를 뜻하지 않는다**
- `CHART_CACHE=<폴더>`: `<차트>-<버전>.tgz`를 미리 받아 두고 쓰기(GitHub 릴리스 다운로드가 자주 504)
- Secret 없이도 StorageClass · Mimir · Agent · node-exporter · Loki · Tempo · blackbox까지는 뜬다. Alertmanager · Grafana만 Secret을 기다린다

시크릿(레포에 없음): `alertmanager-secrets`(`slack_webhook_url` · `healthchecks_ping_url`), `grafana-secrets`(`admin-user` · `admin-password` · `pg-ro-password`). 값 출처는 Slack 앱 `Alertmanager` / healthchecks.io 체크 `alertmanager-watchdog`(period 1분 · grace 4분) / 현수(grafana_ro).

## 확인

```bash
kubectl -n monitoring get pods,pvc,svc
V=172.30.0.35
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=count by (job) (up == 1)'   # pve-host 5 · kubelet 3 · …
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up == 0'                    # 다운 대상
curl -s  "http://$V:9009/prometheus/api/v1/rules"                                                     # 룰 4개, Watchdog firing이 정상
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=probe_success'              # 2개 다 1
kubectl -n monitoring port-forward svc/prometheus-agent 9090   # Agent 대상 상태 화면
```
Alertmanager가 뜨면: healthchecks.io 체크가 Up, `http://172.30.0.35:9093`에 Watchdog. Grafana `http://172.30.0.35:3000` 로그인 후 데이터소스 Mimir · Loki · Tempo.

## 설정을 바꿀 때

| 바꾸는 것 | 파일 | 반영 |
|---|---|---|
| 스크레이프 대상 | `base/prometheus-agent/prometheus.yml` | `deploy-k8s.sh` → ConfigMap 이름이 바뀌어 Agent가 다시 뜸(WAL은 PVC에 남음) |
| 알람 룰 | `base/mimir/rules/*.yml` (+ `kustomization.yaml`의 files) | `deploy-k8s.sh` → Mimir가 1분 안에 다시 읽음(재시작 없음) |
| Mimir 설정 | `base/mimir/mimir.yaml` | Mimir 재시작(복제 1이라 그동안 수신 중단, Agent WAL이 버팀) |
| Alertmanager 라우팅 | `values/alertmanager.yaml`의 `config` | helm upgrade로 Pod 재시작 |
| 데이터소스 | `values/grafana.yaml` | helm upgrade |
| 버전 | `versions.env` (+ `base/` YAML의 image) | kind에서 먼저 |

## 겪은 것 (11/09)

| 증상 | 원인 | 해결 |
|---|---|---|
| Mimir CrashLoop `open ./metrics-activity.log: permission denied` | activity tracker 기본 경로가 작업 디렉터리, 비루트는 못 씀 | `activity_tracker.filepath: /data/...` |
| Alertmanager 사이드카 ImagePull 실패(kind, arm64) | config-reloader 이미지에 arm64 없음 | `configmapReload.enabled: false` |
| Agent 로그 `out-of-order samples`(cadvisor) | cAdvisor가 타임스탬프를 붙여 줌 | `honor_timestamps: false` |
| `helm --wait`가 끝나지 않음(kind) | LoadBalancer IP가 안 나옴 | kind에서만 `service.type=ClusterIP` |
| blackbox ICMP `socket: operation not permitted`(실 노드만) | 비루트엔 NET_RAW가 실효되지 않고 노드 `ping_group_range = 1 0` | Pod sysctl `net.ipv4.ping_group_range` |
| `promtool check config`가 토큰 파일 없다고 실패 | lint 컨테이너엔 ServiceAccount 토큰이 없음 | lint 때 빈 파일 마운트 |
| 차트 다운로드 504, "chart is deprecated" | GitHub 릴리스 불안정, grafana 차트 레포 이전 | `CHART_CACHE`, `grafana-community` |

## 보안 메모 (결정 필요한 것 포함)

- **저장소 API에 인증이 없다.** `172.30.0.35`의 Mimir(9009: push · 조회 · 룰) · Loki(3100) · Tempo(4317/4318) · Alertmanager(9093)는 LAN 172.30.0.0/24와 사이트 간 VPN 뒤에 있다는 전제로 열려 있다. 그 망에 닿는 누구나 지표 · 로그를 넣거나 읽을 수 있다. 줄인 것: Loki 삭제 API 끔, gRPC · 조회 · jaeger 포트는 밖에 안 냄, blackbox는 ICMP만 · ClusterIP. 남은 선택지: ① 현 상태 유지(실습망) ② pfSense에서 VPN 쪽 출발지를 EKS 대역으로 제한 ③ NetworkPolicy(Calico)로 출발지 제한 ④ 앞단 프록시에 basic auth. 워커 Agent · 게이트웨이를 붙이기 전에 정한다
- **kubelet TLS 검증 생략**(`insecure_skip_verify`): kubelet 서버 인증서가 노드별 자체 서명이라 클러스터 CA로 검증할 수 없다. 고치려면 IaC inventory에 `kubelet_rotate_server_certificates: true`(+ CSR 승인자)를 넣고 Kubespray를 다시 돌린 뒤 `ca_file`로 바꾼다. API 서버 proxy 경유는 `nodes/proxy` 권한이 필요해 더 넓어서 쓰지 않는다
- Agent의 RBAC는 읽기 전용(nodes · nodes/metrics · services · pods · endpointslices). `nodes/proxy` · secrets 없음
- Secret 값은 레포 · 셸 기록에 남기지 않는다(`create-secrets.sh`의 `read -s`)

## 다음

replicas 3 + anti-affinity(Mimir · Loki는 replication_factor 3, Loki는 그때 오브젝트 스토리지 필수) → S3 백엔드(버킷 · Roles Anywhere, filesystem 블록 이전 여부 결정) → ArgoCD 원격 등록 → 워커/관리 클러스터용 Agent · Fluent Bit · OTel 게이트웨이 → 분류기 · kafka_exporter · Loki 룰(Xid).
