# 모니터링 스택 (k8s) — 런북

모니터링 클러스터(monitoring1~3, `docs/monitoring-cluster.md`)에 올리는 스택. `compose/`(VM 1대)를 대체한다. 이 문서가 MON-STD-22 "모니터링 클러스터 복구 런북"이다. 부 담당이 이 절차만으로 다시 올릴 수 있어야 한다.

ADR-18 1단계 상태: 저장소는 **복제 1**, 블록 · 청크는 **S3**(11/16 전환, 버킷 `devoops-prod-monitoring-947027666399-us-east-2`의 `mimir/` · `loki/` · `tempo/`), 배포는 **ArgoCD**(11/17, 아래 「적용」). 3노드 복제 · 분류기는 아직 없다. 클러스터에 붙이는 수집 층은 `agents/`(아래 절).

## 구성

| 구성물 | 방법 | 복제 | PVC | 뜬 노드 (11/09) | 클러스터 안 주소 | 밖 주소 (서비스 VIP) |
|---|---|---|---|---|---|---|
| StorageClass `local-path` | Helm `containeroo/local-path-provisioner` | 1 | - | - | - | - |
| Mimir 3.2.4 (지표 저장 · 룰 평가) | `base/mimir/` | 1 | 10Gi | monitoring3 | `mimir.monitoring.svc:9009` | `172.30.0.35:9009` |
| Prometheus Agent v3.15.1 (스크레이프 → Mimir) | `base/prometheus-agent/` | 1 | 2Gi (WAL) | monitoring1 | `prometheus-agent:9090` | - |
| Loki 3.7.9 | Helm `grafana-community/loki` | 1 | 10Gi | monitoring2 | `loki.monitoring.svc:3100` | `172.30.0.35:3100` |
| Tempo 3.1.1 | Helm `grafana-community/tempo` | 1 | 5Gi | monitoring1 | `tempo.monitoring.svc:3200`(조회) | `172.30.0.35:4317` · `:4318` (OTLP만) |
| Alertmanager v0.34.2 | Helm `prometheus-community/alertmanager` | 1 | 1Gi | monitoring2 | `alertmanager.monitoring.svc:9093` | `172.30.0.35:9093` |
| Grafana 13.3.0 | Helm `grafana-community/grafana` | 1 | 2Gi | monitoring2 | `grafana.monitoring.svc:3000` | `http://172.30.0.35:3000` |
| node-exporter 1.12.1 | Helm | 노드마다 | - | 3대 | - | - |
| kube-state-metrics v2.20.0 (11/13 추가) | Helm `prometheus-community/kube-state-metrics` | 1 | - | - | `kube-state-metrics:8080` | - |
| blackbox_exporter v0.29.1 | Helm | 1 | - | monitoring2 | `blackbox-exporter:9115` | - |
| kafka_exporter v1.11.0 (11/17 추가) | Helm `prometheus-community/prometheus-kafka-exporter` | 1 | - | - | `kafka-exporter:9308` | - |
| postgres_exporter v0.20.1 (11/17 추가) | Helm `prometheus-community/prometheus-postgres-exporter` | 1 | - | - | `postgres-exporter:80` | - |

- 버전은 `versions.env` 한 곳에. Grafana · Loki · Tempo 차트는 2026-01부터 `grafana-community/helm-charts`(옛 `grafana/*`는 deprecated)
- PVC는 local-path라 **만들어진 노드에 묶인다.** 그 노드가 죽으면 해당 저장소가 멈춘다(복제 1 단계의 한계, 3노드 복제에서 해소). cAdvisor는 따로 안 깔고 kubelet 내장 것을 긁는다
- Mimir · Loki · Tempo의 PVC에는 WAL · 아직 안 올린 최근 데이터 · 캐시만 있다. 오래 두는 데이터는 S3에 있고 보존 30일은 각 앱 설정이 지킨다(버킷에는 수명 규칙 없음). S3 키는 Pod 안 `aws-credentials` 사이드카가 인증서(IAM Roles Anywhere)로 1시간짜리를 받아 준다
- 밖으로 나가는 문은 전부 `type: LoadBalancer` + `kube-vip.io/loadbalancerIPs: "172.30.0.35"`, 포트로 공유. Loki · Tempo는 차트 Service를 내부용으로 두고 `base/external/services.yaml`이 필요한 포트만 연다

수집 대상(Agent `base/prometheus-agent/prometheus.yml`): kubelet 3 · cAdvisor 3 · node-exporter 3 · kube-state-metrics 1 · 스택 자신 · blackbox probe(pfSense 172.30.0.1 · 인터넷 1.1.1.1) · **Proxmox 호스트 5대(`job="pve-host"`)** · Kafka JMX(`job="kafka-jmx"`) · Kafka · DB VM 4대(`job="vm-host"`) · kafka_exporter · postgres_exporter(아래 「Kafka · DB VM 수집」) · pfSense 2대(`job="pfsense-host"`, 아래 「pfSense 수집」) · 분류기(`job="classifier"`, Pod를 찾아 긁는다).
룰: `base/mimir/rules/self.yml` 4개(Watchdog · MonitoringNodeDiskWarning/Critical · MonitoringTargetDown), Mimir ruler가 15초마다 평가.
`base/mimir/rules/recovery.yml` 1개(`RecoveryHold`, MON-STD-25): 복구 컨트롤러의 `recovery_hold_total`이 늘거나 새로 생기면 약 5분 발화. 단위 테스트는 `base/mimir/tests/recovery_test.yml`(lint 단계가 돌림, 클러스터에는 안 실림).
`base/mimir/rules/records.yml` 레코딩 룰 3개: 대시보드 타일(OOM kill · OTel 거절)용. `_total` 없는 카운터의 `increase()` · `rate()`를 미리 계산해 Grafana 안내 아이콘이 타일 제목을 가리지 않게 한다.
`base/mimir/rules/infra.yml` 4개(MON-STD-13 · 23 · 26): PostgreSQL 접속 · 수집기 lag · Kafka/DB VM과 pfSense의 루트 디스크. 단위 테스트는 `base/mimir/tests/infra_test.yml`. 울렸을 때 볼 것은 아래 「알람이 울리면」.
`base/mimir/rules/classifier.yml` 2개(MON-STD-27): 분류기가 없거나 안 긁힘 · `job.lifecycle`을 못 읽음. 단위 테스트는 `base/mimir/tests/classifier_test.yml`. 울렸을 때 볼 것은 아래 「알람이 울리면」.

`base/mimir/rules/failover.yml` 3개: 분류기가 읽는 원인 알람(`FailoverXidDcgm` · `FailoverPodExit` · `FailoverSiteDown`). Slack으로 가지 않고 Alertmanager의 `pipeline=failover` 라우트로 간다: Xid · Pod 종료는 분류기 웹훅(수신자 `classifier`), 사이트는 받는 곳 없는 수신자(`classifier-pending`). 떴는지는 Alertmanager 화면(`http://172.30.0.35:9093`)에서 본다. 라벨 계약과 한계는 `docs/classifier.md` 2절 · 8절, 단위 테스트는 `base/mimir/tests/failover_test.yml`.
라우팅: Watchdog → healthchecks.io · `RecoveryHold` → Slack `slack-event`(해결 알림 없음: 꺼짐이 해결이 아니라서) · 나머지 → Slack `slack`.
`recovery.yml`과 `slack-event`는 11/11 19:36 클러스터에 적용했다. 복구 컨트롤러 Pod의 수집 어노테이션은 11/19에 들어갔다(Gitops `onprem/controllers/recovery-controller`, 그 전까지는 `recovery_hold_total`이 수집되지 않아 울릴 수 없었다). 확인: `up{cluster="onprem-manager", namespace="platform"}` 3. HOLD가 한 번도 없으면 `recovery_hold_total` 시계열 자체가 없다.

## 적용

**평소: ArgoCD.** 이 레포 `main`에 머지하면 관리 클러스터의 ArgoCD가 모니터링 클러스터에 반영한다(11/17부터). 손으로 돌릴 명령이 없다.

- Application 11개는 Gitops 레포 `bootstrap/onprem/applications/gpu-monitoring-*.yaml`(프로젝트 `gpu-monitoring`, 대상 클러스터 `monitoring`). **새 Application은 같은 폴더 `kustomization.yaml`의 resources에도 넣는다**(파일만 두면 gpu-root가 만들지 않는다, 11/17 kafka-exporter에서 겪음)
  - `gpu-monitoring-base`: 이 레포 `deploy/base`(kustomize: Mimir · Prometheus Agent · 밖으로 여는 Service · 룰 · 대시보드)
  - 나머지 10개(`local-path` · `node-exporter` · `kube-state-metrics` · `loki` · `tempo` · `blackbox` · `alertmanager` · `grafana` · `kafka-exporter` · `postgres-exporter`): 차트 버전은 Gitops Application의 `targetRevision`, 값은 이 레포 `deploy/values/*.yaml`
- 자동 sync이고 `prune` · `selfHeal`은 끈다. 저장소 리소스를 자동으로 지우지 않고, 아래 비상 경로로 넣은 변경을 ArgoCD가 되돌리지 않는다(Git이 바뀔 때만 반영)
- prune을 꺼서 **이름이 바뀐 옛 리소스는 남는다.** `mimir.yaml` · `prometheus.yml`을 고치면 ConfigMap 이름(해시)이 바뀌고 옛 ConfigMap이 남아 Application이 OutOfSync로 보일 수 있다. 확인한 뒤 옛 것을 손으로 지운다
- Secret은 ArgoCD가 다루지 않는다. 지금처럼 `scripts/create-secrets.sh`
- 상태 보기: `kubectl -n argocd get applications | grep gpu-monitoring`(관리 클러스터 kubeconfig)

**비상 · 처음 설치: `scripts/deploy-k8s.sh`.** 관리 클러스터가 죽어 ArgoCD를 못 쓰거나 클러스터를 새로 만들었을 때. 처음 · 변경 · 재설치 모두 같은 명령이다.

```bash
# 1회: 차트 레포
helm repo add grafana-community https://grafana-community.github.io/helm-charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add containeroo https://charts.containeroo.ch

export KUBECONFIG=~/gpu-failover/iac/ansible/inventory/monitoring/artifacts/admin.conf
scripts/create-secrets.sh        # 1회(값이 바뀌면 다시). 사람이 터미널에서, 입력은 화면에 안 보임
scripts/deploy-k8s.sh            # 검사(lint) → 적용. API 서버가 172.30.0.30이 아니면 거부한다
```

- `scripts/deploy-k8s.sh --lint-only`: promtool(룰 검사 · 룰 단위 테스트 · Agent 설정) · kustomize 렌더 · helm template · amtool만
- `scripts/deploy-k8s.sh --kind`: 로컬 kind에 더미 Secret으로 올려 구조 확인(`kind create cluster` 뒤). kind에서는 LoadBalancer IP가 안 나오고, ICMP · 이미지 아키텍처가 실 노드와 달라 **kind 통과가 실 클러스터 통과를 뜻하지 않는다**
- `CHART_CACHE=<폴더>`: `<차트>-<버전>.tgz`를 미리 받아 두고 쓰기(GitHub 릴리스 다운로드가 자주 504)
- Mimir · Loki · Tempo는 S3 인증서 Secret(`mimir-s3-cert` · `loki-s3-cert` · `tempo-s3-cert`)이 있어야 뜬다(없으면 스크립트가 적용 전에 멈춘다, kind도 진짜 인증서 필요). Agent · node-exporter · blackbox는 Secret 없이 뜨고, Alertmanager · Grafana는 자기 Secret을 기다린다

시크릿(레포에 없음): `alertmanager-secrets`(`slack_webhook_url` · `healthchecks_ping_url`), `grafana-secrets`(`admin-user` · `admin-password` · `pg-ro-password`). 값 출처는 Slack 앱 `Alertmanager` / healthchecks.io 체크 `alertmanager-watchdog`(period 1분 · grace 4분) / 현수(grafana_ro). postgres_exporter 비밀번호는 `scripts/create-secrets.sh postgres-exporter`로 따로: `postgres-exporter-secrets`(`password`, 현수). S3용 인증서는 `scripts/create-secrets.sh s3-certs <폴더>`로 따로: `mimir-s3-cert` · `loki-s3-cert` · `tempo-s3-cert`(`client.pem` · `client.key`, 기현이 준 ZIP, 만료 2027-01-15).

## 확인

```bash
kubectl -n monitoring get pods,pvc,svc
V=172.30.0.35
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=count by (job) (up == 1)'   # pve-host 5 · kubelet 3 · …
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up == 0'                    # 다운 대상
curl -s  "http://$V:9009/prometheus/api/v1/rules"                                                     # 알람 룰 14개 + 레코딩 룰 3개, Watchdog firing이 정상
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=probe_success'              # 2개 다 1
kubectl -n monitoring port-forward svc/prometheus-agent 9090   # Agent 대상 상태 화면
```
Alertmanager가 뜨면: healthchecks.io 체크가 Up, `http://172.30.0.35:9093`에 Watchdog. Grafana `http://172.30.0.35:3000` 로그인 후 데이터소스 Mimir · Loki · Tempo.

## 알람이 울리면 (`infra.yml` 11/18 · `classifier.yml` 11/19)

| 알람 | 뜻 | 먼저 볼 것 |
|---|---|---|
| `PostgresDown` (critical) | postgres_exporter가 DB(172.30.0.50)에 2분째 접속하지 못한다. 수집기가 이벤트를 DB에 못 쓰고 Grafana의 DB 패널이 빈다 | 「Kafka · DB VM」에서 db VM이 살아 있는지 → 살아 있으면 PostgreSQL 프로세스 · 접속 수(「PostgreSQL」 연결 사용) → 데이터 담당(김현수). exporter 자체가 죽은 것이면 이 알람이 아니라 `MonitoringTargetDown`이 온다 |
| `CollectorLagHigh` (warning) | 수집기(`data-collector`)가 안 읽은 메시지가 토픽 하나에 1000건 넘게 10분째 쌓여 있다. DB 반영이 늦는다. 1000은 임시값 | 「Kafka 브로커」 수집기 lag 행: 소비 속도가 0이면 수집기가 멈춘 것(관리 클러스터 `data-system/collector` Pod · 로그), 0이 아니면 유입이 몰린 것 → 데이터 담당 |
| `HostDiskWarning` · `HostDiskCritical` | Kafka 3대 · DB VM · pfSense 2대 중 하나의 루트 디스크가 80% · 90%를 5분째 넘었다 | 「Kafka · DB VM」 · 「pfSense」 루트 디스크 그래프로 어느 호스트인지 · 언제부터 늘었는지 → Kafka는 로그 보존, DB는 테이블 크기(「PostgreSQL」) → Kafka · DB VM은 데이터 담당, pfSense는 인프라 담당(박기현) |
| `ClassifierDown` (critical) | 분류기 Pod가 없거나 `/metrics`가 3분째 안 긁힌다. 원인 알람이 떠도 `gpu.fault`가 나가지 않아 자동 복구가 시작되지 않는다 | `kubectl -n monitoring get deploy,pod -l app.kubernetes.io/name=classifier` → Pod가 없거나 못 뜨면 `describe pod`(이미지 pull · `ghcr-pull`) · `logs`(시작 때 Kafka에서 기억을 되살리지 못하면 포트를 열지 않는다 → 브로커 상태는 「Kafka 브로커」) · Argo CD `gpu-monitoring-classifier`. Pod가 멀쩡하면 Agent 쪽(`up{job="prometheus-agent", cluster="monitoring"}`) |
| `ClassifierLifecycleUnhealthy` (critical) | 분류기가 `job.lifecycle`을 2분째 못 읽는다. 그동안 Pod 종료 신호는 전부 `hold`로 나가 사람이 봐야 한다(`RecoveryHold`가 따라 울릴 수 있다) | `kubectl -n monitoring logs deploy/classifier \| grep kafka.health_changed`의 `reason` → 브로커 문제면 「Kafka 브로커」에서 복구를 기다린다(돌아오면 알람이 꺼진다). 브로커가 멀쩡한데 안 꺼지면(메시지 반영 실패는 재시작 전까지 0으로 남는다) `kubectl -n monitoring rollout restart deploy/classifier` |

## 대시보드 (Grafana 폴더 「인프라」)

레포의 JSON이 원본이다. `base/grafana/dashboards/infra/*.json` → kustomize가 ConfigMap `grafana-dashboards-infra`로 만들고 → Grafana 차트(`values/grafana.yaml`의 `dashboardProviders` · `dashboardsConfigMaps`)가 디렉터리로 마운트한다. Grafana가 디렉터리를 주기적으로 다시 읽어 **재시작 없이 약 1분 안에 반영**된다(sidecar 안 씀).

| uid | 제목 | 보는 것 | 변수 |
|---|---|---|---|
| `mon-stack` | 모니터링 스택 | 수집 대상 up · 알람(Watchdog · Firing) · 저장소 유입량 · 노드 디스크 · Alertmanager 발송 · blackbox ping | - |
| `mon-collect` | 수집 파이프라인 | Agent remote_write · WAL, Fluent Bit 출력, OTel 게이트웨이 큐 · 거절 | cluster |
| `pve-hosts` | Proxmox 호스트 | 5대 온도(5분 평균) · 자원 · nic0 · 배터리 · AC | host |
| `kafka-jmx` | Kafka 브로커 | 브로커 · 컨트롤러 · 파티션 복제 · 처리량 · 수집기(data-collector) lag · 요청 p99 · JVM | - |
| `k8s-nodes` | Kubernetes 노드 | 온프레 두 클러스터 노드 9대와 EKS 노드의 자원 · PSI · 디스크 · NIC · OOM · NTP. EKS 노드는 이름이 전부 `ip-…`라 범례 · 표 · 노드 목록에 종류(`node_role`: training = GPU 학습 · spare = GPU 예비 · system = CPU)를 붙여 보여 준다. 이 라벨은 수집 층 Agent의 `node-exporter` 잡이 노드 라벨에서 만든다 | cluster · node |
| `k8s-workloads` | Kubernetes 워크로드 | Pod 상태 · 재시작 · 대기 사유 · 마지막 종료 코드 · 워크로드 복제 · 학습 Pod | cluster · namespace |
| `vm-hosts` | Kafka · DB VM | Kafka 3대 · DB VM의 자원 · PSI · 루트 디스크(80 · 90% 선) · 디스크 IO · eth0 | host |
| `pfsense` | pfSense | 주 · 대기의 인터페이스 상태 · 트래픽(WAN · LAN · AWS VPN 터널) · 오류 · 드롭 · 게이트웨이/인터넷 ping · 상태 동기화(pfsync) · 자원 · 루트 디스크 | host |
| `postgres` | PostgreSQL | DB 접속(pg_up) · 연결 수(상태 · 사용자별) · 잠금 · 트랜잭션 · 캐시 적중률 · gpuf_data 테이블 크기 · 삽입 속도 · 죽은 행 · WAL · 체크포인트 | - |

- **화면에서 고친 것은 저장되지 않는다**(`allowUiUpdates: false`). 고치는 흐름: 화면에서 편집 → 「Export → JSON」 → 레포 파일 교체 → 커밋 → 적용
- 새 대시보드는 `kustomization.yaml`의 `grafana-dashboards-infra` files에도 넣는다. lint가 JSON 문법 · `uid` · `title` · uid 중복을 본다
- uid를 바꾸거나 파일을 지우면 옛 대시보드가 Grafana에 남을 수 있다(11/11 `mon-self` → 분리 때). 남아 있으면 화면에서 지운다
- 타일(폭 4칸) 제목은 한 줄로만 나오고 넘치면 "…"로 잘린다. 제목은 짧게, 자세한 뜻은 설명(ⓘ)에 둔다. 이름이 `_total`로 안 끝나는 카운터에 `increase()` · `rate()`를 쓰면 PromQL 안내 아이콘이 제목 칸을 차지하므로, 타일에서는 레코딩 룰(`base/mimir/rules/records.yml`)을 읽는다
- 색: 정상 `#73BF69` · 경고 `#FF9830` · 위험 `#F2495C` · 좋고 나쁨이 없는 값 `#8AB8FF`. 점선 임계선은 MON-STD에 값이 있는 패널에만 긋는다

## 설정을 바꿀 때

파일을 고쳐 `main`까지 머지하면 ArgoCD가 반영한다(머지 전 `scripts/deploy-k8s.sh --lint-only`). 아래 「반영」은 머지 뒤 일어나는 일이다.

| 바꾸는 것 | 파일 | 반영 |
|---|---|---|
| 스크레이프 대상 | `base/prometheus-agent/prometheus.yml` | ConfigMap 이름이 바뀌어 Agent가 다시 뜸(WAL은 PVC에 남음) |
| 알람 · 레코딩 룰 | `base/mimir/rules/*.yml` (+ `kustomization.yaml`의 files, `scripts/deploy-k8s.sh` lint의 파일 목록, 테스트는 `base/mimir/tests/`) | Mimir가 1분 안에 다시 읽음(재시작 없음) |
| 대시보드 | `base/grafana/dashboards/infra/*.json` (+ `kustomization.yaml`의 files) | Grafana가 1분 안에 다시 읽음(재시작 없음) |
| Mimir 설정 | `base/mimir/mimir.yaml` | Mimir 재시작(복제 1이라 그동안 수신 중단, Agent WAL이 버팀) |
| Alertmanager 라우팅 | `values/alertmanager.yaml`의 `config` | Pod 재시작 |
| 데이터소스 | `values/grafana.yaml` | Grafana 재시작 |
| 이미지 버전 | `base/` YAML의 image (+ `versions.env`) | kind에서 먼저 |
| 차트 버전 | `versions.env` **와** Gitops Application의 `targetRevision` 두 곳 | kind에서 먼저. 한쪽만 올리면 비상 경로와 ArgoCD가 서로 다른 버전을 깐다 |

## Kafka · DB VM 수집 (11/17)

Kafka VM 3대(.40~.42)와 DB VM(.50)에서 받는 것은 셋이다.

| 무엇 | 어디서 도나 | 잡 | 주로 보는 것 |
|---|---|---|---|
| OS 지표 | VM의 node-exporter(apt 패키지 1.7.0, `ansible/playbooks/service-vms.yml`) | `vm-host`(`host` = `kafka1`~`kafka3` · `db`) | CPU · 메모리 · 디스크 |
| 컨슈머 그룹 lag | 모니터링 클러스터 `kafka-exporter`(브로커 3대 :9092에 원격 접속, 평문) | `kafka-exporter` | `kafka_consumergroup_lag{consumergroup="data-collector"}` |
| PostgreSQL 상태 | 모니터링 클러스터 `postgres-exporter`(.50 `gpuf_data`, 계정 `postgres_exporter`, sslmode require) | `postgres-exporter` | `pg_up` · 연결 수 · DB 크기 |

- lag를 보는 이유: 수집기(관리 클러스터 `data-system/collector`)는 `/metrics`가 없어 밀리는지 알 방법이 lag뿐이다. 값 `-1`은 그 파티션에 커밋한 오프셋이 아직 없다는 뜻이라 룰 · 패널에서 거른다
- `pg_up`이 0이고 `up{job="postgres-exporter"}`가 1이면 exporter는 살아 있고 DB 접속만 안 되는 것이다. `kubectl -n monitoring logs deploy/postgres-exporter`에 이유가 나온다(비밀번호 · `pg_hba`는 모니터링 노드 .31~.33만 허용)
- **새 exporter를 붙이는 순서**: ① Secret(필요하면) → ② Gitops Application(+ kustomization 등록) → ③ 이 레포의 values · 스크레이프 잡을 main에. ③이 먼저 들어가면 Agent가 없는 대상을 긁어 5분 뒤 `MonitoringTargetDown`이 난다(11/17 kafka-exporter에서 19분 동안 `up` 0)
- VM 접속은 VM 이름과 같은 일반 계정 + sudo(`-k --ask-become-pass`). root SSH 로그인은 막혀 있다

확인:
```bash
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up{job=~"vm-host|kafka-exporter|postgres-exporter"}'                          # 6개 1
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=kafka_consumergroup_lag_sum{consumergroup="data-collector"}'                  # 토픽마다 값
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=pg_up'                                                                        # 1
```

## pfSense 수집 (11/17)

pfSense 2대(주 .2 · 대기 .3)의 OS 지표만 받는다(박기현 합의: node-exporter만, 로그는 범위 밖).

| 무엇 | 값 |
|---|---|
| 설치 | pfSense 공식 패키지 `pfSense-pkg-node_exporter` 0.18.1_8(node_exporter 1.11.0), `ansible/playbooks/pfsense.yml`(대기 → 주) |
| 듣는 주소 | LAN 주소의 9100만(.2 · .3). 127.0.0.1 · CARP VIP(.1) · WAN에서는 응답하지 않는다. 플레이북이 `sockstat`으로 확인한다 |
| 잡 | `pfsense-host`, `host` = `pfsense` · `pfsense-standby`. 두 VM은 복제본이라 `nodename`이 둘 다 `pfSense` → 구분은 `host`로만 |
| 설정 위치 | pfSense `config.xml`의 `installedpackages/nodeexporter`(웹 화면 Services → Prometheus node_exporter와 같은 값). 패키지는 대기에 자동으로 따라가지 않아 두 대 모두에 실행한다 |

- FreeBSD라 지표 이름이 Linux와 다르다: 메모리는 `node_memory_{active,wired,free,size}_bytes`, CPU 모드는 `user · nice · system · interrupt · idle`. 인터페이스 상태는 `node_pfsense_interface_up`
- **CPU에 1분 주기 톱니가 보이는 것은 정상이다.** 패키지가 넣는 1분 cron(인터페이스 지표를 textfile로 쓰는 스크립트, nice 20)이 매분 0~15초에 돈다. 실측(11/17 20:02~20:11, 15초 구간): 대기(2코어) 평균 4.1% · 최대 7.0%(나머지 구간 0.2~1.3%), 주(10코어) 평균 0.5% · 최대 0.9%. `mode="nice"`는 이 구간에만 나온다. 패널은 1분 이상 평균으로 본다
- 접속: SSH 계정은 `admin`(메뉴가 뜨고 `8`이 셸) · `root`(바로 셸)뿐이고 비밀번호가 같다. Ansible은 `root`(`-k`). `pfsense` · `pfsense-standby`는 VM 이름이지 계정이 아니다
- **pfSense의 22 · 443을 반복해서 두드리지 않는다.** 로그인 실패 · 인증 없는 접속이 쌓이면 sshguard가 출발지 IP의 SSH · 웹을 몇 분 막는다(ping은 됨, 장비마다 따로). Tailscale 경유 접속은 전부 devoops(.90)로 보여 팀 전체가 같이 막힌다(11/17 19:10~19:27). 확인 · 해제는 Proxmox 콘솔에서 `pfctl -t sshguard -T show` · `pfctl -t sshguard -T delete 172.30.0.90`, 기록은 `grep -i sshguard /var/log/auth.log /var/log/system.log`

확인:
```bash
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up{job="pfsense-host"}'                       # 2개 1
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=node_textfile_scrape_error{job="pfsense-host"}'  # 2개 0
```

## PVC에 남은 데이터를 S3로 올리기 (11/17에 한 것)

저장 백엔드를 filesystem에서 S3로 바꾸면 그 전 데이터는 PVC에만 남아 조회되지 않는다. 지우기 전에 올리면 살아난다. 앱은 멈추지 않는다.

공통: 앱과 **같은 노드**에 임시 Pod를 띄운다(PVC가 노드에 묶여 있다). PVC는 `readOnly`로 붙이고, 앱 Pod의 `aws-credentials` 사이드카 정의(args · 인증서 Secret)를 그대로 복사해 넣고, `amazon/aws-cli` 컨테이너에 `AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911/`를 준다(**끝의 `/` 필수**, 없으면 `Invalid endpoint`). 역할이 자기 접두어만 허용하므로 앱마다 따로 띄운다. 끝나면 Pod를 지운다.

| 앱 | PVC의 옛 데이터 | S3 위치 | 주의 |
|---|---|---|---|
| Loki 청크 | `/var/loki/chunks/fake/<스트림>/<base64 이름>` | `loki/fake/<스트림>/<from>:<through>:<checksum>` | 파일 이름이 base64로 싸여 있다. `base64 -d`로 푼 이름으로 올린다 |
| Loki 인덱스 | `/var/loki/chunks/index/index_<일>/…tsdb.gz` | `loki/index/index_<일>/…` | 그대로 복사. S3에 같은 날 테이블이 있어도 컴팩터가 다음 주기(10분)에 하나로 합친다(올리기 전 S3 쪽 파일 백업) |
| Mimir 블록 | `/data/blocks/anonymous/<ULID>/` | `mimir/anonymous/<ULID>/` | `deletion-mark.json`이 있는 블록은 건너뛴다(이미 합쳐진 원본). `chunks/` · `index` 먼저, **`meta.json`은 마지막**(반쯤 올라간 블록을 읽지 않게). `sparse-index-header` · `bucket-index.json.gz` · `markers/`는 안 올린다 |

- Loki는 올린 직후부터 조회된다(빈 테이블을 캐시하고 있던 날짜는 약 5분 뒤)
- Mimir는 컴팩터가 버킷 목록을 고치고(15분 주기) 저장소 게이트웨이가 다시 읽은 뒤(15분 주기)에 조회된다
- 확인은 올리기 전후로 같은 조회를 돌려 비교한다(Loki `sum(count_over_time({service_name=~".+"}[6h]))`, Mimir `count(up)`을 옛 시각으로). 조회가 확인된 뒤에 PVC의 옛 폴더를 지운다(Loki `/var/loki/chunks`, Mimir `/data/blocks`, Tempo `/var/tempo/traces/single-tenant`)

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

## 보안 메모 (11/09 결정 반영)

- **저장소 API에 인증이 없다.** `172.30.0.35`의 Mimir(9009: push · 조회 · 룰) · Loki(3100) · Tempo(4317/4318) · Alertmanager(9093)는 LAN 172.30.0.0/24와 사이트 간 VPN 뒤에 있다는 전제로 열려 있다. 그 망에 닿는 누구나 지표 · 로그를 넣거나 읽을 수 있다. 줄인 것: Loki 삭제 API 끔, gRPC · 조회 · jaeger 포트는 밖에 안 냄, blackbox는 ICMP만 · ClusterIP. **결정(11/09): 현 상태 유지.** 망에 닿는 것은 팀원 6명과 우리 AWS 계정뿐이고, 분류기 라우트는 `pipeline=failover` 라벨이 붙은 알람만 받게 할 예정이라(분류기를 올릴 때 추가) 라벨 없는 테스트 알람은 복구로 이어지지 않는다. 운영 환경이라면 pfSense에서 VPN 쪽 출발지 제한, NetworkPolicy, 앞단 프록시 인증 중 하나가 필요하다(하지 않음)
- **kubelet TLS 검증 생략**(`insecure_skip_verify`): kubelet 서버 인증서가 노드별 자체 서명이라 클러스터 CA로 검증할 수 없다. **결정(11/09): 그대로 둔다**(실습망에서 위험이 낮고 Agent 토큰은 읽기 전용). 고치는 방법은 IaC inventory에 `kubelet_rotate_server_certificates: true`(+ CSR 승인자)를 넣고 Kubespray를 다시 돌린 뒤 `ca_file`로 바꾸는 것이다. API 서버 proxy 경유는 `nodes/proxy` 권한이 필요해 더 넓어서 쓰지 않는다
- Agent의 RBAC는 읽기 전용(nodes · nodes/metrics · services · pods · endpointslices). `nodes/proxy` · secrets 없음
- Secret 값은 레포 · 셸 기록에 남기지 않는다(`create-secrets.sh`의 `read -s`)

## 분류기 (`classifier/` · `deploy/classifier/`, 11/18)

Alertmanager가 보낸 원인 알람을 판정해 Kafka `gpu.fault`로 낸다. 설계와 판정 표는 `docs/classifier.md`.
**실 클러스터(모니터링 클러스터 `monitoring` 네임스페이스)에는 Argo CD가 올린다**(11/18): Application `gpu-monitoring-classifier` → Gitops 레포 `monitoring/onprem/classifier`. 그 overlay가 이 레포의 `deploy/classifier/base`를 릴리스 태그로 참조하고 이미지 태그를 고정한다.

- **버전을 올릴 때**: 손으로 할 일이 없다(배포 봇 연결 11/19). `classifier/` · `schemas/fault-event.v1.json` · `.dockerignore` · `.github/workflows/ci.yml`이 바뀐 main 머지에서 `ci.yml`의 `deploy`가 Gitops overlay의 `newTag` · `digest`와 `?ref=vX.Y.Z`(매니페스트 base)를 같은 릴리스로 바꾼다. `deploy/classifier/base`만 바뀐 머지는 이미지가 안 올라가 `deploy`도 돌지 않는다: 그때만 Gitops overlay의 `?ref=`를 손으로 올린다
- **pull secret**: 이미지 패키지가 비공개라 Secret `ghcr-pull`이 필요하다. Git에 없다. 관리 클러스터 `platform`의 것을 복사한다(값이 화면에 나오지 않는다):
  ```bash
  kubectl --kubeconfig <관리 클러스터 admin.conf> -n platform get secret ghcr-pull -o json \
  | jq 'del(.metadata.namespace,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.ownerReferences,.metadata.managedFields,.metadata.annotations)' \
  | kubectl --kubeconfig <모니터링 클러스터 admin.conf> -n monitoring apply -f -
  ```
- **알람이 들어오는 길**: Alertmanager 라우트 `pipeline=failover` + `signal=~"xid|pod_exit"` → 웹훅 `http://classifier.monitoring.svc:8080/alerts`. 사이트 알람은 아직 보내지 않는다(`values/alertmanager.yaml` 주석)
- **확인**: `kubectl -n monitoring get deploy classifier`(1/1) · `kubectl -n monitoring logs deploy/classifier` · 아래 지표
- **끄기**: 웹훅만 끊으려면 `values/alertmanager.yaml`의 수신자 `classifier`에서 `webhook_configs`를 지운다. 분류기는 떠 있어도 알람이 안 오면 아무것도 내지 않는다

| 무엇 | 어디 |
|---|---|
| 코드 · 판정 표 | `classifier/classifier/` (`rules.yaml`이 표) |
| 단위 테스트 | `cd classifier && .venv/bin/python -m pytest` (처음엔 `python3.12 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt`) |
| 이미지 | `classifier/Dockerfile`, 빌드 컨텍스트는 레포 루트 |
| 매니페스트 | `deploy/classifier/base` (복제본 1 · Recreate · ClusterIP), `overlays/kind` (임시 Kafka 포함) |
| 종단 검증 | `scripts/classifier-e2e-kind.sh` |

**종단 검증이 하는 일** (약 5분, 끝나면 `--delete`로 클러스터 삭제):

1. 워커 2대짜리 kind에 수집 층 · 분류기 · 임시 Kafka를 올리고, 작업 라벨이 붙은 가짜 Pod 3개를 띄운다
2. 웹훅을 보내 응답을 확인한다: Xid 79 → 1건 발행 / 재전송 → 중복 / 분류기 재시작 뒤 재전송 → 중복 / `job.fenced` 뒤 Pod 종료 → 버림 / 기록 없는 Pod 종료 → 1건 발행 / Xid가 난 작업의 Pod 종료 → 버림
3. kind의 `gpu.fault`에서 메시지를 읽어 v1 스키마 · 봉투 · `rule_version` 헤더를 확인한다

알아 둘 것:

- **지표 · 로그가 실제 Mimir · Loki로 간다**(`cluster="kind-test"`). `deploy-k8s.sh --agents-kind`와 같은 방식이고, 분류기가 `job_id` · `collector_healthy`를 Mimir에서 읽기 때문에 필요하다. Kafka는 kind 안의 임시 브로커라 실제 브로커(`.40~.42`)에는 쓰지 않는다
- kubeconfig는 `$TMPDIR/classifier-e2e/kubeconfig`에 따로 둔다. 평소 쓰는 컨텍스트를 바꾸지 않는다
- kind 노드에는 `/var/log/journal`이 없어 Fluent Bit가 뜨지 못한다. 스크립트가 노드에 디렉터리를 만든다(`--agents-kind`만 따로 쓸 때도 같은 문제가 난다)
- `kind load docker-image`가 Docker의 containerd 이미지 저장소에서 실패해(`content digest ... not found`) 이미지를 파일로 저장해 넣는다

분류기 자체를 볼 지표(`/metrics`): `classifier_alerts_total{outcome}` · `classifier_events_published_total` · `classifier_lifecycle_healthy`(0이면 Pod 종료 신호가 전부 `hold`로 나간다) · `classifier_pending_pod_exits`. 모니터링 클러스터 Agent가 긁어(`job="classifier"`) Mimir에서 조회한다. 이상 알람은 위 「알람이 울리면」.

## 수집 층 (`deploy/agents/`) — 관리 클러스터 · EKS · 모니터링 클러스터(Fluent Bit만)에 올리는 것

저장소(위)가 아니라 **클러스터마다 하나씩 붙이는 쪽**. 배치 규약과 이유는 `docs/worker-agents.md`.

| 구성물 | 종류 | 버퍼 (관리 클러스터 hostPath `/var/lib/monitoring-agents/…`) | 보내는 곳 |
|---|---|---|---|
| kube-state-metrics v2.20.0 | Deployment ×1 | - | Agent가 긁음 |
| Prometheus Agent v3.15.1 | Deployment ×1 | WAL `prometheus-agent/` | Mimir `172.30.0.35:9009` |
| Fluent Bit 5.1.1 | DaemonSet (toleration Exists) | 책갈피 db · 미전송 청크 `fluent-bit/` | 게이트웨이 `otel-gateway:4318` |
| node-exporter 1.12.1 | DaemonSet (toleration Exists, hostNetwork · 노드 IP:9100) | - | Agent가 긁음 |
| OTel Collector contrib 0.164.0 | Deployment ×1 | file_storage 큐 `otel-gateway/` | Loki `172.30.0.35:3100/otlp` · Tempo `172.30.0.35:4317` |

- `base/`는 모든 클러스터 공통. 다른 것(cluster · site 이름, nodeSelector, 버퍼 디스크)만 `overlays/<클러스터>/`에서 patch. 클러스터 이름은 Deployment env `CLUSTER_NAME` · `SITE`로 들어가 Agent `external_labels` · 게이트웨이 `resource` 프로세서가 읽는다
- 적용은 **ArgoCD**(Gitops 레포 Application `gpu-monitoring-agents` → Monitoring main `deploy/agents/overlays/onprem`, automated). 이 스크립트는 kind만: `scripts/deploy-k8s.sh --agents-kind`(`kind create cluster --name agents-test` 뒤, 컨텍스트 kind-*만 허용). kind에서도 데이터는 실제 저장소로 간다(cluster 라벨 `kind-test`)
- lint(`--lint-only`)가 수집 층도 본다: promtool(Agent 설정) · `fluent-bit --dry-run` · `otelcol-contrib validate` · overlay 렌더
- Loki 라벨 이름은 `cluster`가 아니라 **`k8s_cluster_name`**(OTel 리소스 속성 `k8s.cluster.name`을 Loki가 그대로 인덱싱). 함께 라벨이 되는 것: `k8s_namespace_name` · `k8s_pod_name` · `k8s_container_name` · `service_name`(= 컨테이너 이름, 커널 로그는 `kernel`). `k8s_node_name` · `site` · `stream`은 structured metadata

확인:
```bash
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up{cluster="onprem-manager"}'          # kubelet · cadvisor · kube-state-metrics · prometheus-agent · otel-gateway · fluent-bit · node-exporter(노드마다 1) · annotated-pods
curl -sG "http://$V:3100/loki/api/v1/label/k8s_namespace_name/values" --data-urlencode 'query={k8s_cluster_name="onprem-manager"}'
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=otelcol_exporter_queue_size{cluster="onprem-manager"}'   # 0이 정상, 쌓이면 저장소 쪽 문제
```

### Kueue 지표 수집 (11/17)

Agent의 `kueue` 잡(`base/prometheus-agent/prometheus.yml`)이 `kueue-system`의 Kueue 컨트롤러 Pod `:8443/metrics`를 긁는다. base에 있어서 Kueue가 있는 클러스터면 그대로 붙고, 없으면 타깃이 0개다.

- 8443은 HTTPS + 토큰 인증이다. Agent ServiceAccount에 `/metrics` 읽기 권한(`rbac.yaml`의 nonResourceURLs)이 이미 있어 RBAC는 바꾸지 않았다
- 인증서 검증은 건너뛴다(kubelet 잡과 같은 방식). 검증하려면 Kueue 쪽에 cert-manager 연동을 켜야 한다. 토큰은 읽기 전용이다
- 관리 클러스터는 Kueue Pod 3개가 **모두 같은 큐 지표를 낸다** → 그냥 `sum`하면 3배가 된다. 패널 · 룰에서는 `replica_role="leader"`로 고른다
- 큐별 쿼터 · 사용량(`kueue_cluster_queue_nominal_quota` · `kueue_cluster_queue_resource_usage`)은 Kueue 설정 `metrics.enableClusterQueueResources: true`가 있어야 나온다(Gitops `onprem/multikueue/kueue/values.yaml` · `eks/scheduling/base/kueue-values.yaml`, 11/17 켬)
- 입장 대기 시간(`kueue_admission_wait_time_seconds`) · 입장 누적(`kueue_admitted_workloads_total`)은 잡이 한 번 입장해야 시계열이 생긴다
- `kueue_*` 말고 컨트롤러 내부 지표(`controller_runtime_*` · `workqueue_*` · `go_*`)도 같이 들어온다(Pod당 약 1,700 시계열). 거르지 않는다

확인:
```bash
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up{cluster="onprem-manager", job="kueue"}'                                           # 3개 1
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=kueue_cluster_queue_nominal_quota{cluster="onprem-manager", replica_role="leader"}'   # 큐마다 1줄
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=kueue_pending_workloads{cluster="onprem-manager", replica_role="leader"}'             # 대기 잡 수
```

### 모니터링 클러스터 자신의 로그 (`overlays/monitoring`)

모니터링 클러스터에는 수집 층 중 **Fluent Bit만** 올린다. 지표 쪽(Agent · kube-state-metrics · node-exporter)은 저장 층(`deploy/base` · `values/`)에 이미 있다.

- OTel 게이트웨이를 두지 않고 Loki OTLP(`172.30.0.35:3100/otlp`)로 직접 보낸다: 트레이스를 내는 앱이 없고 Loki가 같은 클러스터라 버퍼 이득이 작다. 그래서 설정 파일을 따로 둔다(`overlays/monitoring/fluent-bit.yaml`, 입력 · 필터는 base와 같고 출력만 다름)
- 라벨은 `k8s_cluster_name="monitoring"` · `service_name`(`pod` | `kernel`) 둘뿐이다. Fluent Bit은 리소스 속성에 고정 값만 넣을 수 있어서, 네임스페이스 · Pod · 컨테이너 이름은 structured metadata(`kubernetes_namespace_name` · `kubernetes_pod_name` · `kubernetes_container_name` · `kubernetes_host`)로 들어간다 → **관리 클러스터와 조회 방법이 다르다**
- 적용은 ArgoCD(Gitops 레포 Application `gpu-monitoring-logs`, 자동 sync · prune/selfHeal 끔). `deploy-k8s.sh`는 lint(`--dry-run` · overlay 렌더)만 하고 적용하지 않는다
- kind 노드는 journald가 `/run/log/journal`에 있고 커널 메시지가 없다 → kind에서 볼 때는 렌더 결과의 `path`를 바꾸고 `systemd_filter`를 떼야 저널 출력 경로를 확인할 수 있다

확인:
```bash
curl -sG "http://$V:3100/loki/api/v1/query_range" --data-urlencode 'query={k8s_cluster_name="monitoring", service_name="pod"} | kubernetes_namespace_name="monitoring" | kubernetes_container_name="loki"' --data-urlencode limit=5
curl -sG "http://$V:3100/loki/api/v1/query" --data-urlencode 'query=sum by (_HOSTNAME) (count_over_time({k8s_cluster_name="monitoring", service_name="kernel"}[1h]))'   # 노드 3대
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up{cluster="monitoring", job="fluent-bit"}'                                                    # 3개 1
```

겪은 것 (11/10, kind):

| 증상 | 원인 | 해결 |
|---|---|---|
| `otelcol validate`: `yaml: did not find expected ',' or '}'` | flow 매핑 `{value: ${env:X}}`의 `:`·`}` | `"${env:CLUSTER_NAME}"` 따옴표 |
| `file_storage: directory must exist` (lint만) | 컨테이너 단독 실행엔 hostPath가 없음 | `create_directory: true` |
| 게이트웨이 경고 `"otlphttp" alias is deprecated` | 0.164에서 exporter 이름 변경 | `otlp_http` · `otlp_grpc` |
| Loki에 Fluent Bit 자기 로그가 매초 1줄 | OTLP 출력이 요청마다 `HTTP status=200`을 info로 찍음 | `log_level: warn` |
| Loki 메타데이터에 `_p` · `time` | cri 파서 잔여 필드 | 게이트웨이 transform에서 `delete_key` |
| Fluent Bit 첫 10초 `no upstream connections` | 게이트웨이보다 먼저 떴음 | 정상. 재시도 뒤 전부 전송(버퍼가 일하는 모습) |

## EKS 수집 층 (`deploy/agents/overlays/eks`, 11/18)

EKS 워커 클러스터(eks-a · eks-b · eks-c)에 올리는 묶음. 공통 수집 층(`base`) + dcgm-exporter(`gpu`). 배치 규약은 `docs/worker-agents.md`.
EKS는 비용 때문에 켰다 끈다. 그래서 **클러스터 이름을 파일에 적지 않고** ConfigMap `monitoring/monitoring-cluster-info`의 `CLUSTER_NAME`으로 받는다.

| 무엇 | 어디에 | 비고 |
|---|---|---|
| Agent · KSM · OTel 게이트웨이 | CPU 노드 (`workload=system`, t3.large 1대) | GPU 노드는 스팟이라 회수되면 수집이 같이 죽는다. 버퍼는 hostPath |
| Fluent Bit · node-exporter | 전 노드 | 어떤 taint든 통과 |
| dcgm-exporter 4.8.4 | GPU 노드 (`devoops/gpu=true`) | 수집 5초. Agent의 `annotated-pods` 잡이 긁는다(`job="annotated-pods"`). 메모리는 T4에서 약 435MiB를 쓴다(한도 1Gi, 256Mi에서는 OOMKilled 반복) |
| 학습 Pod의 trainer 지표 (`train_*`, :8000) | 학습 Pod (`training` 네임스페이스) | 같은 `annotated-pods` 잡. 어노테이션은 Backend가 JobSet Pod 템플릿에 붙인다(Backend v1.7.0부터). 우리 쪽 설정은 없다 |

**올라가는 길**

- 평소: 클러스터가 뜨면 Gitops ApplicationSet `gpu-eks-monitoring`이 `gpu-eks-monitoring-<이름>`을 만들어 자동 적용한다. 이름은 ArgoCD 클러스터 이름이 그대로 들어간다
- 손으로(ApplicationSet이 안 될 때): 레포의 같은 overlay에 이름만 넣어 적용한다

```bash
# KUBECONFIG는 그 EKS 것. 이름은 eks-a · eks-b · eks-c 중 하나
kubectl kustomize deploy/agents/overlays/eks | sed 's/CLUSTER_NAME: unset/CLUSTER_NAME: eks-a/' | kubectl apply -f -
```

이름이 안 들어간 채로 뜨면 지표가 `cluster="unset"`으로 들어온다 → `count(up{cluster="unset"})`이 0인지 본다.
pfSense의 IPsec 인바운드(EKS 172.28.0.0/16 → `172.30.0.35` 9009 · 3100 · 4317 · 4318)는 IaC `GPU-149`로 들어가 있다.

**EKS가 뜬 날 확인 순서** (위에서부터. 앞이 안 되면 뒤는 볼 것이 없다)

| # | 무엇 | 어떻게 | 기대 |
|---|---|---|---|
| 1 | ArgoCD 앱 | 관리 클러스터 `kubectl -n argocd get application \| grep eks-monitoring` | 클러스터마다 하나, Synced · Healthy |
| 2 | 수집 대상 | `count by (cluster, job) (up{cluster=~"eks-.*"} == 1)` | `prometheus-agent` 1 · `kube-state-metrics` 1 · `otel-gateway` 1 · `fluent-bit` 6 · `node-exporter` 6 · `kubelet` 6 · `cadvisor` 6 |
| 3 | 이름 주입 | `count(up{cluster="unset"})` | 0 |
| 4 | dcgm-exporter | `count by (cluster) (DCGM_FI_DEV_GPU_TEMP)` | 클러스터마다 5 |
| 5 | dcgm 라벨 이름 | `DCGM_FI_DEV_GPU_TEMP{cluster="eks-a"}`의 라벨 | `node` · `UUID`가 있다. 다르면 `classifier/classifier/enrich.py`의 `GPU_METRIC` 조회를 고친다(커널 로그 경로와 DCGM 경로의 중복 제거가 여기에 달렸다) |
| 6 | 노드 라벨 | `kube_node_labels{cluster="eks-a"}` | `label_topology_kubernetes_io_zone` · `label_node_kubernetes_io_instance_type` |
| 7 | 커널 로그 | Loki `{k8s_cluster_name="eks-a", service_name="kernel"}` | GPU 노드에서 줄이 온다. 안 오면 AL2023 NVIDIA AMI의 journald 위치(`/var/log/journal`)를 본다 |
| 8 | T4가 내는 지표 | `DCGM_FI_DEV_GPU_MAX_OP_TEMP` · `SLOWDOWN_TEMP` · `SHUTDOWN_TEMP` · `DCGM_FI_DEV_THERMAL_VIOLATION` · `POWER_VIOLATION` | 값이 나오는지(MON-STD-01 · 02의 전제). 안 나오는 것을 적어 둔다 |
| 9 | 학습 Pod 라벨 | `kube_pod_labels{cluster="eks-a", label_devoops_job_id!=""}` | 학습 Pod마다 `label_devoops_job_id` · `label_devoops_generation` |
| 10 | trainer 지표 | `train_step_seconds_count{cluster="eks-a"}` | rank마다. `docs/training-metrics.md`의 8개 |
| 11 | 분류기(노트북) | 아래 명령으로 띄워 Xid · Pod 종료 웹훅을 실제 노드 · Pod 이름으로 보낸다 | `job_id` · `generation` · `az` · `gpu_uuid`가 채워지고 `collector_healthy=true` |

```bash
# 11번: Kafka 없이(화면에만 찍는다) 실제 Mimir를 읽게. 실제 gpu.fault에는 쓰지 않는다
cd classifier && MIMIR_URL=http://172.30.0.35:9009 .venv/bin/uvicorn classifier.app:create_app --factory --port 18080
```

내릴 때: `FailoverSiteDown`이 클러스터마다 약 8분 뜬다(11/19 17:48 ~ 17:56). 받는 곳 없는 수신자로 가서 Slack · 분류기에는 오지 않는다. `MonitoringTargetDown`은 뜨지 않는다(대상이 사라질 뿐 `up == 0`이 생기지 않는다). 일부러 내린 것과 장애를 구분하는 방법은 아직 없다.

**11/19 첫 테스트 결과**: 1 ~ 10번 확인, 11번은 장애 주입 날. 값과 타임라인은 `docs/measurements/2026-11-19-eks-first-test.md`.

**겪은 것 (11/19)**

- **dcgm-exporter 10대가 전부 재시작 반복**: `up{container="dcgm-exporter"} == 0`, KSM 마지막 종료 사유 `OOMKilled`. 한도 256Mi가 실제 사용량(약 435MiB)보다 작았다. kind에는 GPU가 없어 볼 수 없던 값이다
- **`train_*` 지표 0건**: 학습은 도는데 `up{namespace="training"}`이 없으면 학습 Pod에 `prometheus.io/scrape` 어노테이션이 없는 것이다(Backend jobspec). 이미 떠 있는 Pod에는 나중에 붙지 않는다. 새로 제출한 작업부터
- **`recovery_*` 지표 0건**: 복구 컨트롤러 Pod도 같은 어노테이션이 없었다. 「알람이 조용하다」와 「지표가 아예 안 들어온다」는 Mimir에서 `up`으로 구분한다
- **EKS 노드 이름이 전부 `ip-…`**: node-exporter 지표에 `node_role`(training · spare · system)을 붙였다. 붙기 전(11/19 16:55 이전) 구간을 걸친 그래프는 같은 노드가 선 두 개로 보인다

## 장애 주입 (11/20, GPU-92)

주입은 두 가지다. ① trainer 강제 종료(진짜 신호, 룰부터 전부 실제 경로) ② 한송이 시나리오 Xid 79 → 48(분류기에 시험 알람을 직접 넣음, 룰은 건너뜀).
`dcgmi`로 GPU에 Xid를 넣는 것은 이번엔 안 한다: dcgm-exporter가 내장 엔진이라 넣어도 지표에 안 나온다(사이드카는 테스트 뒤 따로).

**복구 컨트롤러가 실행 모드다.** 주입하면 실제로 복구(새 세대 제출 · 노드 격리)가 일어난다. 주입마다 한송이에게 먼저 알리고 순서를 맞춘다. 격리 해제는 한송이가 한다.

**준비** (위 「EKS가 뜬 날 확인 순서」 1 ~ 10이 끝난 뒤)

1. 학습 작업 하나(eks-a, 2랭크 이상)를 띄우고 `train_*` 지표가 들어오는지 본다(10번)
2. 첫 체크포인트 승인(`job.lifecycle`의 `checkpoint.approved`) 뒤에 주입한다. 그 전이면 복구할 체크포인트가 없다
3. 띄워 둘 것: Alertmanager `http://172.30.0.35:9093` · Grafana · 아래 시각 기록표
4. 주입 시각은 누를 때 `date '+%T'`로 적는다(KST)

### EKS에 kubectl로 붙기

EKS API는 사설 주소만 열려 있어 노트북에서 바로 닿지 않는다. 온프레 관리 클러스터 master1을 거치는 SOCKS 터널로 들어간다(IaC `terraform/run.sh --proxy`와 같은 방식).
권한은 따로 받을 것이 없다: Terraform이 SSO 관리자 역할(`AWSReservedSSO_AdministratorAccess_…`)을 클러스터 관리자로 등록한다(IaC `terraform/modules/eks/access.tf`).

처음 한 번: AWS 프로필 `devoops`(IaC `run.sh` 기본값과 같은 이름)

```bash
aws configure sso --profile devoops
#   SSO session name: devoops (노트북 안 별명)
#   SSO start URL: Confluence 「7. 접속 정보」의 포털 주소 (끝의 /#/는 뺀다), SSO region: 주소 안의 리전(ap-northeast-2)
#   계정 947027666399 → 역할 AdministratorAccess, 기본 region us-east-2
aws sts get-caller-identity --profile devoops   # Arn에 AWSReservedSSO_AdministratorAccess
```

EKS가 뜬 날마다:

```bash
aws sso login --profile devoops                 # 로그인은 하루쯤 지나면 만료된다
ssh -N -D 1080 master1@172.30.0.11              # 별도 터미널에서 띄워 둔다. 비밀번호를 물으므로 Claude Code의 !로는 안 된다.
                                                # 비밀번호 뒤 아무것도 안 나오고 멈춘 것처럼 보이면 열린 것(-N = 통로만). 닫으면 끊긴다
aws eks update-kubeconfig --name devoops-prod-eks-a --region us-east-2 --profile devoops --kubeconfig ~/.kube/eks-a.yaml
# 프록시는 환경 변수가 아니라 kubeconfig에 넣는다: HTTPS_PROXY로 걸면 토큰을 받는 aws eks get-token까지 SOCKS를 타다 실패한다
kubectl --kubeconfig ~/.kube/eks-a.yaml config set-cluster \
  "$(kubectl --kubeconfig ~/.kube/eks-a.yaml config view -o jsonpath='{.clusters[0].name}')" \
  --proxy-url=socks5://127.0.0.1:1080
export KUBECONFIG=~/.kube/eks-a.yaml
kubectl -n training get pods                    # 보이면 준비 끝
```

eks-b는 이름만 바꾼다(`devoops-prod-eks-b`, `~/.kube/eks-b.yaml`). 터널은 하나로 같이 쓴다.

### ① trainer 강제 종료

위 「EKS에 kubectl로 붙기」를 마친 터미널에서 한다. trainer 컨테이너는 PID 1이 `torchrun`이고 그 아래 `python … train.py`가 워커다.
PID 1은 컨테이너 안에서 `kill -9`가 먹지 않으므로 **워커를 죽인다** → `torchrun`이 실패로 끝나고 컨테이너가 `Error`로 끝난다.

```bash
# 대상 Pod (작업 · 세대 · 랭크가 같이 보인다)
kubectl -n training get pods -o wide -L devoops/job-id,devoops/generation,batch.kubernetes.io/job-completion-index
POD=<랭크 1 Pod>

# 1) 프로세스 목록 먼저. PID 1 = torchrun, cmdline에 train.py가 있는 다른 PID = 워커
kubectl -n training exec "$POD" -c trainer -- sh -c 'for p in /proc/[0-9]*; do printf "%s " "${p#/proc/}"; tr "\0" " " < "$p/cmdline"; echo; done'

# 2) 워커 강제 종료 (sh 내장 kill. 이미지에 /bin/kill이 없을 수 있다)
date '+%T'; kubectl -n training exec "$POD" -c trainer -- sh -c 'kill -9 <워커 PID>'
```

볼 것 (위에서부터 순서대로 나와야 한다. 수집 15초 · 룰 평가 15초 · 분류기 대기 30초라 **주입 뒤 2분 안에 `gpu.fault`**)

| # | 무엇 | 어디서 | 기대 |
|---|---|---|---|
| 1 | 컨테이너 종료 | `kube_pod_container_status_terminated_reason{cluster="eks-a", namespace="training", container="trainer"} == 1` | 그 Pod가 `reason="Error"` |
| 2 | 원인 알람 | Alertmanager | `FailoverPodExit`, 수신자 `classifier`, 라벨 `pod` · `node` · `reason` |
| 3 | 분류기 수신 | `classifier_alerts_total{outcome="deferred"}` | +1. 30초 뒤 `published` +1 |
| 4 | 발행 | Loki `{k8s_cluster_name="monitoring", service_name="pod"} \| kubernetes_container_name="classifier" \|= "event.published"` | `action` `restart_in_place` · `job_id` 채워짐. `dedup_key` 끝의 숫자 = 원본 신호 시각(epoch초) |
| 5 | 다른 랭크 | 1 · 4를 다시 본다 | 다른 랭크 Pod가 언제 · 어떻게 끝나는지 적는다(같이 `Error` → `gpu.fault` 한 건 더? 펜싱으로 지워짐 → `intended`?). 같은 세대에 `restart_in_place`가 두 번 나가면 컨트롤러가 한 번만 복구하는지 한송이와 확인 |
| 6 | 컨트롤러 안전망 | `sum by (reason) (recovery_hold_total)` | 늘지 않는다. `job.failed`만 받고 300초 안에 `gpu.fault`가 안 오면 컨트롤러가 HOLD한다(`unclassified_job_failure`, 이 이름이 지표 라벨로도 나오는지는 미확인 → 전체 증가로 본다) |
| 7 | 복구 | Grafana PostgreSQL `events`에서 그 `job_id`: `recovery.decided` → `job.submitted`(새 세대) → `job.resumed` · `train_current_step{job_id="…"}` 새 `generation` 증가 | 새 세대가 이어서 학습 |
| 8 | 덤: 진짜 Xid | Loki `{k8s_cluster_name="eks-a", service_name="kernel"} \|= "NVRM: Xid"` · `DCGM_FI_DEV_XID_ERRORS{cluster="eks-a"}` | 워커가 GPU를 쓰던 중이면 Xid 45가 날 수 있다. 나면 `FailoverXidDcgm`(`xid="45"`)이 울리고 분류기는 `ignored`. **로그 줄 원문을 측정 노트에 그대로 붙인다**(커널 로그 Xid 룰을 이 원문에 맞춰 쓴다) |

### ② 한송이 시나리오: Xid 79 → 48

**한송이가 요청할 때만 넣는다.** 받을 것: 작업 ID · 현재 세대 · EKS 이름 · 대상 노드 · 시각.
노드 이름이 실제여야 분류기가 Mimir에서 `job_id` · `collector_healthy` · `gpu_uuid`를 채운다(가짜 노드면 `hold`).
`source="dmesg"`로 보내고 `gpu_uuid`를 비우면 분류기가 dcgm-exporter 라벨에서 찾는다.

```bash
# 다른 터미널에 띄워 둔다 (모니터링 클러스터 kubeconfig. 분류기를 밖으로 열지 않는다)
kubectl --kubeconfig <모니터링 admin.conf> -n monitoring port-forward svc/classifier 18080:8080

CLUSTER=eks-a NODE=<한송이가 준 노드> XID=79 TAG="TEST NODE-004"   # 48은 XID=48 TAG="TEST NODE-005"
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ); date '+%T'
curl -s -X POST localhost:18080/alerts -H 'Content-Type: application/json' -d "{\"alerts\":[{\"status\":\"firing\",
  \"labels\":{\"alertname\":\"ManualXidTest\",\"pipeline\":\"failover\",\"signal\":\"xid\",\"source\":\"dmesg\",\"cluster\":\"$CLUSTER\",\"node\":\"$NODE\",\"xid\":\"$XID\"},
  \"annotations\":{\"event_ts\":\"$TS\",\"log_line\":\"$TAG\"}}]}"
```

- 응답 `{"published":1}`이면 나갔다. Loki `event.published`에서 79 = `isolate_migrate`, 48 = `reset_then_migrate`, `job_id`가 채워졌는지 본다
- `hold`로 나가면: `job_id`가 없거나(그 노드에 작업이 없음 · 둘 이상) 수집이 비정상(`collector_healthy=false`: 그 클러스터 Agent · OTel 게이트웨이 · 그 노드 Fluent Bit 중 하나가 `up` 0)
- **45초 안에 같은 것을 다시 보내면 `duplicate`**(같은 사건). 다시 보내야 하면 1분 기다린다
- 79의 복구가 끝나면 한송이가 새 세대 · 새 노드 · 시각을 준다 → 그 노드로 48
- 시험 이벤트는 실제 `gpu.fault`에 남는다. 구분은 `raw.log_line`의 `TEST NODE-00x`

### HOLD가 나면

`RecoveryHold` 알람이 Slack `#alerts-gpu`에 와야 한다(Slack 화면은 사용자가 확인). `recovery_hold_total{job_id, reason}`가 늘었는지 같이 적는다.

### 시각 기록표

주입마다 한 줄. 시각은 KST, 원 시각을 남기고 차이는 나중에 계산한다(정의가 바뀌어도 다시 계산할 수 있게).

| 주입 | 작업 · 세대 · 노드 | T0 주입 | T1 원본 신호(`dedup_key` epoch) | T2 알람 발화(Alertmanager) | T3 발행(Loki `event.published`) | T4 `recovery.decided` | T5 `job.resumed` | 결과 · 메모 |
|---|---|---|---|---|---|---|---|---|
| ① kill -9 | | | | | | | | |
| ② Xid 79 | | | | | | | | |
| ② Xid 48 | | | | | | | | |

감지 = T3 − T0, 분류 = T3 − T1, 복구 = T5 − T3. 끝나면 `docs/measurements/2026-11-20-…md`로 옮긴다.

### 끝난 뒤 (EKS를 내리기 전에)

- dcgm-exporter 메모리 최대값: `max by (cluster) (max_over_time(container_memory_working_set_bytes{container="dcgm-exporter"}[6h]))` → 한도 조정(768Mi · 512Mi 안)의 근거
- `POWER_VIOLATION` 원인: 학습 중 노드와 유휴(spare) 노드의 `rate(DCGM_FI_DEV_POWER_VIOLATION[10m])` 비교(11/19 측정 노트 3절)
- 내릴 때 `FailoverSiteDown`이 약 8분 뜨는 것은 정상(위)

## 다음

~~S3 백엔드~~(11/16) → ~~ArgoCD 원격 등록~~(11/17) → ~~관리 클러스터용 수집 층~~(11/10) → replicas 3 + anti-affinity(Mimir · Loki는 replication_factor 3) → ~~EKS overlay~~(11/18, EKS에서 확인은 11/19) → ~~분류기 본체~~(11/18, 실 클러스터 배포는 남음) · ~~kafka_exporter~~(11/17) · 원인 알람 룰(Xid · Pod 종료 · 스팟 · 사이트).
