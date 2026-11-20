# 모니터링 클러스터 HA 계획 (GPU-227)

ADR-18의 목표 구성(저장소 ×3 · Alertmanager ×2 · Agent ×2 · 분류기 ×2)으로 가는 계획. 지금은 모니터링 클러스터의 거의 모든 것이 1개라, 하나가 죽으면 그동안 룰 평가 · 알람 · 원인 분류가 멈춘다.

- 상태: **계획 (2026-11-20 확정)**. 구현은 11/22 뒤, 기능 동결 12/06 전
- Jira: GPU-227 「모니터링 클러스터 HA 구현」 하나로 전부 진행
- 근거: 아래 「출처」의 버전 고정 소스 · 문서와 차트 렌더링 결과(11/20 조사). 확인하지 못한 것은 6절에 따로 적었다

## 1. 지금 → 목표

| 대상 | 지금 | 목표 | 핵심 변경 |
|---|---|---|---|
| Alertmanager | 1 | 2 | 개수만 올리면 차트가 클러스터링을 자동 설정. Mimir · Loki ruler가 Service 하나가 아니라 **모든 AM에** 보내게 주소를 바꾼다 |
| Mimir | 1 | 3 | `replication_factor` 1 → 3 (2곳), 노드마다 하나씩(anti-affinity), 메모리 한도 2Gi → 3Gi |
| Loki | 1 | 3 | `replication_factor` 3 + **ruler 분담 켜기**(기본 꺼짐, 안 켜면 3개가 같은 알람을 각자 냄) |
| Tempo | 1 | 분산형 | 3.x는 Kafka 없이는 늘릴 수 없다 → `tempo-distributed` + Kafka 토픽 + rollout-operator (5절 결정 1) |
| Prometheus Agent | 1 (모니터링 · 관리 클러스터) | 2 | Mimir HA tracker + Agent 묶음 전용 라벨 (5절 결정 2) |
| 분류기 | 1 | 2 | active-standby: 대장 하나만 처리 (5절 결정 3) |
| Grafana | 1 | 1 | 그대로 (persistence 끄고 파일 프로비저닝) |

EKS Agent는 CPU 노드가 1대라 2개를 둬도 같은 노드에 뜬다 → 1 그대로.

## 2. 자원 (11/20 실측, EKS 기동 중)

| | monitoring1 | monitoring2 | monitoring3 |
|---|---|---|---|
| 할당 가능 | 3.4 코어 · 8.6GiB | 같음 | 같음 |
| 메모리 실사용 | 1.68GiB | 2.01GiB | 2.22GiB |
| CPU 실사용 (5분) | 0.17 | 0.18 | 0.28 |
| 메모리 요청 합계 | 1.0GB | 1.15GB | 1.55GB |
| 디스크 여유 (`/`) | 50GiB | 49GiB | 49GiB |

- Mimir 1개가 활성 시계열 17.8만에 약 0.9GiB를 쓴다. 3개 · RF 3이면 Pod마다 전체 사본을 들므로 Pod당 비슷하게 약 1GiB(+ 조회 · 압축 여유)
- 늘린 뒤 예상 실사용: 노드당 약 3 ~ 3.5GiB + Tempo 분산형 합계 요청 약 1.5 ~ 2GiB → 들어간다. 단계마다 실사용을 다시 본다

## 3. 대상별 바꿀 것

### 3.1 Alertmanager ×2 (차트 prometheus-community/alertmanager 2.3.1)
- `deploy/values/alertmanager.yaml`: `replicaCount: 2`. 차트가 `--cluster.listen-address` · `--cluster.peer=<이름>-<i>.<이름>-headless:9094`를 넣고 headless Service에 9094 TCP/UDP를 연다
- 차트 기본이 꺼져 있어 직접 켠다: `podAntiAffinity` · PodDisruptionBudget
- 알람을 보내는 쪽(AM 공식 README: 로드밸런서 말고 AM 전부에게 보낼 것)
  - Mimir: `ruler.alertmanager_url`(3.2.4에서 폐기 예정) → `limits.ruler_alertmanager_client_config.alertmanager_url: dnssrvnoa+http://_http._tcp.alertmanager-headless.monitoring.svc.cluster.local.`
  - Loki: `rulerConfig.alertmanager_url: http://_http._tcp.alertmanager-headless.monitoring.svc.cluster.local` + `enable_alertmanager_discovery: true`
- 바꾸지 않는 것: AM의 웹훅 수신자(분류기 · Slack · healthchecks.io)

### 3.2 Mimir ×3 (자체 StatefulSet, 3.2.4 모놀리식)
- 링은 이미 전부 memberlist → 추가 설정 없음
- `deploy/base/mimir/mimir.yaml`: `ingester.ring.replication_factor` · `store_gateway.sharding_ring.replication_factor` 1 → 3 (기본값이 3이라 줄을 지워도 된다)
- `deploy/base/mimir/statefulset.yaml`: replicas 3, 필수 anti-affinity(hostname), PDB `maxUnavailable: 1`, 메모리 한도 3Gi
- 쓰기는 3개 중 2개가 받으면 성공. 수신 한도(50k/s)는 클러스터 전체 값을 distributor들이 나눠 씀 → 그대로
- compactor · ruler는 링으로 자동 분담. ruler는 Pod마다 같은 룰 파일(지금 ConfigMap 마운트)이면 분담된다
- 늘릴 때는 그냥 늘려도 안전. **줄일 때만** 절차: `/ingester/prepare-instance-ring-downscale` → `/ingester/shutdown` → 종료

### 3.3 Loki ×3 (차트 grafana-community/loki 21.0.0, Monolithic)
- `deploy/values/loki.yaml`: `singleBinary.replicas: 3`, `commonConfig.replication_factor: 3`
- 차트가 memberlist(`join_members` + headless Service) · 노드당 하나 anti-affinity · 종료 때 WAL flush를 자동으로 넣는다(렌더링으로 확인)
- **`rulerConfig.enable_sharding: true` 추가 필수**: 기본 false라 그대로면 3개가 모든 룰을 각자 평가해 알람이 3배로 나간다. 켜면 로컬 파일 룰도 링으로 나눈다
- compactor는 링으로 하나만 동작 → 그대로
- PDB는 차트 기본 없음 → 추가

### 3.4 Prometheus Agent ×2 + Mimir HA tracker
- Mimir: `distributor.ha_tracker.enable_ha_tracker: true`(저장소는 memberlist 기본) + `limits.accept_ha_samples: true` · `ha_cluster_label: <전용 라벨>` · `ha_replica_label: __replica__`
- Agent: `external_labels`에 전용 라벨(Agent 묶음 이름) + `__replica__: ${POD_NAME}`. `__replica__`는 Mimir가 저장 전에 지운다. 전용 라벨은 남는다
- **왜 전용 라벨인가**: HA tracker는 요청 안 **첫 시계열**의 라벨만 보고, 라벨이 없으면 거르지 않고 받는다. 모니터링 클러스터 Agent는 `cluster`를 대상마다 relabel로 붙이고 LAN 대상(Proxmox · Kafka · DB · pfSense)에는 `cluster`가 없다 → `cluster`로는 중복 제거가 틀린다
- 대가: 이 Agent들이 보내는 시계열에 라벨이 하나 늘어 **시계열이 새로 생긴다(과거 그래프와 한 번 끊김)**. 대시보드 · 룰은 이 라벨로 거르지 않으므로 식 변경은 없다(구현 때 확인)
- 대상: 모니터링 클러스터 Agent(StatefulSet) · 관리 클러스터 Agent. EKS는 1 그대로(라벨이 없으면 HA tracker가 그냥 받는다)

### 3.5 분류기 ×2 (active-standby)
- 둘 다 떠 있고 **쿠버네티스 Lease를 잡은 대장만** 알람을 처리한다. 대기 쪽은 readiness를 실패시켜 Service에서 빠진다 → Alertmanager 웹훅은 늘 대장에게 간다
- 대장이 죽으면 Lease 만료(약 15초) 뒤 대기 쪽이 대장이 된다. 넘겨받을 때 지금 시작 절차와 같이 `gpu.fault` 1시간 · `job.lifecycle` 24시간을 다시 읽어 중복 기억을 복원한다
- 넘겨받는 동안 들어온 알람은 Alertmanager가 재시도한다. 옛 대장이 들고 있던 「Pod 종료 대기(30초)」 신호는 사라진다 → 같은 알람이 `repeat_interval`(1분)로 다시 오므로 그때 처리
- 바꿀 것: `classifier/`(Lease · readiness), `deploy/classifier/base`(replicas 2 · anti-affinity · Lease RBAC), 테스트 + kind 종단 검증에 「대장 죽임 → 넘겨받음 → 같은 알람 1건만」 장면
- `docs/classifier.md` 7절 「복제본은 1개다」 줄 교체

### 3.6 Tempo 분산형 (tempo-distributed + Kafka)
- 이유: Tempo 3.0에서 「확장형 모놀리식」이 없어졌다. 모놀리식을 3개 띄우면 서로 모르는 Tempo 3개가 되어 트레이스가 Pod마다 흩어진다. 늘리는 공식 방법은 마이크로서비스 모드뿐이고 **Kafka가 필수**다
- 구조: `앱 → distributor → Kafka(토픽) → block-builder → S3` / `Kafka → live-store(최근 데이터)`, 조회는 `query-frontend → querier → S3 + live-store`, 압축은 `backend-scheduler · backend-worker`
- 차트: `grafana-community/tempo-distributed` **3.25.0**(app 3.1.1, 11/05 출시). 3.29.1은 11/18 출시라 2주 규칙에 걸린다
- Kafka 토픽(현수 요청 보냄 11/20): `tempo.ingest`(이름 미정), 파티션 3 · 복제 3 · `min.insync.replicas` 2, 보존 짧게(예 24시간). live-store 수 = 파티션 수
- rollout-operator: live-store를 존마다 두는 HA에 필요(차트가 요구)
- OTLP 받는 주소: `.35:4317/4318`의 대상 Service를 distributor로 바꾼다. 보내는 쪽(OTel 게이트웨이) 설정은 그대로
- Gitops: Application `gpu-monitoring-tempo`를 새 차트로 교체, AppProject에 차트 · rollout-operator 리소스 허용
- 기존 트레이스: 거의 없음. 같은 S3 접두어(`tempo/`)의 옛 블록을 새 구성이 읽는지는 확인 전 → 못 읽으면 버린다

## 4. 순서와 검증

각 단계 = kind에서 먼저(같은 values · 같은 버전) → 실 클러스터 → 커밋 하나마다 보고. 실 클러스터 반영은 ArgoCD(Gitops 변경이 있으면 Gitops main → Monitoring main 순서).

| 순서 | 할 일 | 실 클러스터에서 확인할 것 |
|---|---|---|
| 1 | Alertmanager ×2 + ruler 주소 | `/api/v2/status`의 peers 2 · Watchdog이 healthchecks.io로 계속 감 · 한 Pod를 지워도 알람이 감 |
| 2 | Mimir ×3 | ring 페이지 ACTIVE 3 · 한 Pod를 지워도 쓰기 · 조회 · 룰 평가 계속 · Agent 쪽 429/5xx 없음 |
| 3 | Loki ×3 + ruler 분담 | 링 3 · 로그 유입 계속 · 룰 그룹이 한 곳에서만 평가됨(알람 1건) · 한 Pod를 지워도 유입 계속 |
| 4 | Agent ×2 + HA tracker | `cortex_ha_tracker_elected_replica_changes_total` · 중복 샘플 거름(`cortex_distributor_deduped_samples_total`) · 한 Agent를 지우면 30초 안에 다른 쪽 선출 |
| 5 | 분류기 ×2 | 대장 1 · 대기 1(Ready 0) · 대장 지우면 넘겨받음 · 같은 알람 `gpu.fault` 1건 |
| 6 | Tempo 분산형 | 트레이스 수신 · 조회 · 한 live-store를 지워도 최근 트레이스 조회 |

마지막에: ADR-18 적용 현황 · 아키텍처 그림 「지금 어디까지 왔나」 표 · 런북 · `docs/monitoring-cluster.md` 첫 줄 갱신, 노드 한 대 재부팅 시험(4절 방식)으로 전체 확인.

## 5. 결정 (사용자 2026-11-20)

1. **Tempo는 분산형(B)으로 간다.** 1개 유지(A)도 검토했으나 진짜 HA를 택함. Kafka가 데이터 파트 설비라 토픽은 현수 합의 뒤
2. **Agent 묶음은 전용 라벨**로 구분한다(`cluster` 재사용 안 함). 라벨 이름은 구현 때
3. **분류기는 active-standby**(Lease 대장 하나만 처리)
4. **Jira는 GPU-227 하나로 통일**
5. 받아들이는 위험 2개(6절 맨 위)

## 6. 위험 · 확인할 것

**받아들이는 것**
- **노드를 새로 만들 때만 손이 필요하다.** 디스크가 노드 로컬(local-path)이라 Pod가 그 노드에 묶인다. 노드가 잠깐 죽으면 남은 2개로 버티고, 노드가 돌아오면 자기 디스크로 자동으로 뜬다. VM을 지우고 새로 만들면 옛 PVC를 지워야 그 Pod가 뜬다(데이터는 S3 · 다른 복제본에 있음) → 런북에 절차 추가. 노드끼리 복제하는 저장소(Longhorn · Ceph)는 노드 3대에 무거워 안 쓴다
- **Alertmanager 둘 사이가 끊기면 같은 알림이 두 번 갈 수 있다.** AM은 놓치기보다 두 번 보내기를 택하도록 설계됐다. 분류기는 중복 제거가 있고 Slack은 가끔 두 번 온다

**확인할 것 (구현 때)**
- Mimir store-gateway RF 3이면 Pod마다 모든 블록의 index-header를 든다 → PVC 10Gi로 충분한지
- Loki `common.compactor_grpc_address`가 ClusterIP라 리더 아닌 Pod로 갈 수 있음(삭제 기능이 꺼져 있어 무해할 것으로 봄)
- Loki 종료 대기 30초 안에 WAL flush가 끝나는지
- Alertmanager Pod가 준비되기 전 피어 DNS 해석 지연
- Tempo 분산형이 옛 블록(`tempo/`)을 읽는지 · rollout-operator 버전(2주 규칙)
- 전용 라벨을 넣은 뒤 대시보드 · 룰 · 분류기의 Mimir 조회에 영향 없는지

## 출처 (11/20 조사, 버전 고정)

- Mimir 3.2.4: `configuration-parameters/index.md`(링 기본 memberlist · RF 기본 3 · HA tracker 키), `configure-high-availability-deduplication.md`(첫 시계열만 봄 · 라벨 없으면 통과 · memberlist 권장), `components/ruler/index.md`(로컬 룰 분담), `run-production-environment/scaling-out.md`(줄일 때 절차), `planning-capacity.md`(메모리)
- Loki 3.7.9: `pkg/ruler/base/ruler.go`(`enable_sharding` 기본 false), `pkg/loki/config_wrapper.go`(join_members면 링 전부 memberlist), configuration.md(`enable_alertmanager_discovery`). 차트 21.0.0 `templates/validate.yaml`(오브젝트 스토리지만 요구)
- Tempo 3.1.1: `reference-tempo-architecture/deployment-modes.md`(SSB 제거 · 마이크로서비스는 Kafka 필수), `cmd/tempo/app/modules.go:250`(`PushSpansToKafka = !singleBinary`), `setup-tempo/configure-kafka.md`(복제 3 · 최소 동기 2 · 파티션 ≥ live-store), 차트 tempo-distributed 3.25.0 `templates/validate.yaml`(Kafka 주소 필수)
- Alertmanager v0.34.2 README(모든 AM에 보낼 것), 차트 2.3.1 `templates/statefulset.yaml`(클러스터링 자동) · values.yaml(anti-affinity · PDB 기본 꺼짐)
