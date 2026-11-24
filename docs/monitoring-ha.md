# 모니터링 클러스터 HA 계획 (GPU-227)

ADR-18의 목표 구성(저장소 ×3 · Alertmanager ×2 · Agent ×2 · 분류기 ×2)으로 가는 계획. 지금은 모니터링 클러스터의 거의 모든 것이 1개라, 하나가 죽으면 그동안 룰 평가 · 알람 · 원인 분류가 멈춘다.

- 상태: **1 ~ 6단계 운영 반영 · 노드 재부팅 전체 확인 끝 (2026-11-24)**. 남은 것 = 6절 「확인할 것」. 계획 확정은 11/20
- Jira: GPU-227 「모니터링 클러스터 HA 구현」 하나로 전부 진행
- 근거: 아래 「출처」의 버전 고정 소스 · 문서와 차트 렌더링 결과(11/20 조사). 확인하지 못한 것은 6절에 따로 적었다

## 1. 지금 → 목표

| 대상 | 지금 | 목표 | 핵심 변경 |
|---|---|---|---|
| Alertmanager | 1 | 2 | 개수만 올리면 차트가 클러스터링을 자동 설정. Mimir · Loki ruler가 Service 하나가 아니라 **모든 AM에** 보내게 주소를 바꾼다 |
| Mimir | 1 | 3 | `replication_factor` 1 → 3 (2곳), 노드마다 하나씩(anti-affinity), 메모리 한도 2Gi → 3Gi |
| Loki | 1 | 3 | `replication_factor` 3 + **ruler 분담 켜기**(기본 꺼짐, 안 켜면 3개가 같은 알람을 각자 냄) |
| Tempo | 1 | 분산형 | 3.x는 Kafka 없이는 늘릴 수 없다 → `tempo-distributed` + Kafka 토픽 + live-store 존 2벌 + rollout-operator (5절 결정 1 · 6) |
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
- **구현함(11/23). 정본은 `docs/classifier.md` 7.1절**
- 둘 다 떠 있고 **쿠버네티스 Lease를 잡은 대장만** 알람을 처리한다. 대기 쪽도 Ready이고, 받은 웹훅을 대장에게 넘겨준다
  - 원안은 「대기 쪽은 readiness를 실패시켜 Service에서 뺀다」였다. Ready가 아닌 Pod가 늘 있으면 Deployment 교체가 멈추고 Argo CD가 비정상으로 봐서 바꿨다
- 대장이 죽으면 Lease 만료(약 15초) 뒤 대기 쪽이 대장이 된다. 넘겨받을 때 지금 시작 절차와 같이 `gpu.fault` 1시간 · `job.lifecycle` 24시간을 다시 읽어 중복 기억을 복원한다
- 넘겨받는 동안 들어온 알람은 Alertmanager가 재시도한다. 옛 대장이 들고 있던 「Pod 종료 대기(30초)」 신호는 사라진다 → 같은 알람이 `repeat_interval`(1분)로 다시 오므로 그때 처리
- 바꿀 것: `classifier/`(Lease · readiness), `deploy/classifier/base`(replicas 2 · anti-affinity · Lease RBAC), 테스트 + kind 종단 검증에 「대장 죽임 → 넘겨받음 → 같은 알람 1건만」 장면
- `docs/classifier.md` 7절 「복제본은 1개다」 줄 교체

### 3.6 Tempo 분산형 (tempo-distributed + Kafka)
- 이유: Tempo 3.0에서 「확장형 모놀리식」이 없어졌다. 모놀리식을 3개 띄우면 서로 모르는 Tempo 3개가 되어 트레이스가 Pod마다 흩어진다. 늘리는 공식 방법은 마이크로서비스 모드뿐이고 **Kafka가 필수**다
- 구조: `앱 → distributor → Kafka(토픽) → block-builder → S3` / `Kafka → live-store(최근 데이터)`, 조회는 `query-frontend → querier → S3 + live-store`, 압축은 `backend-scheduler · backend-worker`
- 차트: `grafana-community/tempo-distributed` **3.25.0**(app 3.1.1, 11/05 출시). 3.29.1은 11/18 출시라 2주 규칙에 걸린다
- Kafka 토픽(현수가 생성 11/20, 데이터 파트 설계 · Kafka 흐름 문서 토픽 표에 있음): `tempo.ingest`, 파티션 3 · 복제 3 · `min.insync.replicas` 2, 보존 24시간 + 파티션당 1GB 상한(`retention.bytes`, 브로커 디스크를 학습 사건과 같이 써서 트레이스가 몰려도 못 채우게). 접속 `172.30.0.40~42:9092`(PLAINTEXT, 인증 없음), 컨슈머 그룹은 Tempo 이름으로. live-store 수 = 파티션 수(3)
- values: `deploy/values/tempo-distributed.yaml`(옛 `tempo.yaml`은 전환이 끝나면 지운다). 이미지 tempo 3.1.1 · rollout-operator v0.41.1
- 개수(11/24 kind 검증으로 확정):

  | 구성 요소 | 개수 | 이유 |
  |---|---|---|
  | distributor · query-frontend · querier | 각 2 | 상태 없음. 하나 죽어도 수신 · 조회 |
  | block-builder | 3 | = 파티션 수(필수) |
  | live-store | 3 × 존 2 | = 파티션 수 × 두 벌(5절 결정 6). 한 벌이면 하나가 죽은 동안 그 파티션의 「아직 S3에 안 올라간 최근 트레이스」를 답할 곳이 없다 |
  | backend-scheduler · backend-worker | 각 1 | 압축 · 보존 삭제만 한다. 죽어도 쓰기 · 읽기는 안 멈추고 늦어지기만 한다(6절) |
  | rollout-operator | 1 | 두 벌을 한 존씩 차례로 교체한다(존 복제를 켤 때만 차트가 요구). 죽어도 Tempo는 돈다 |

- **PVC 없음**(차트 기본): 다시 뜨면 Kafka에 커밋한 오프셋부터 다시 읽는다. 그래서 Tempo는 노드 디스크에 묶이지 않는다(6절 첫 위험에서 빠짐)
- **rollout-operator webhook은 끈다.** webhook 4개가 실패하면 거절(`Fail`)하고 네임스페이스 전체에 걸려서, operator가 죽은 동안 Mimir · Loki · AM · Agent의 StatefulSet 수정과 노드 drain까지 막는다. `objectSelector`로 좁히면 eviction(라벨 없는 Eviction 객체)에는 아예 안 걸려 보호가 사라진다(kind). 차례 교체는 webhook 없이 돈다 → live-store는 존별 PDB 대신 일반 PDB 하나(6개 중 동시에 1개). CRD는 남긴다(operator가 없으면 오류를 반복)
- **Kafka 주소는 하나만 받는다**(쉼표 목록 안 됨, franz-go `SeedBrokers`). 떠 있는 Pod는 브로커 3대를 다 알지만 그 하나가 죽은 동안 새로 뜨는 Pod는 못 붙는다 → Pod `hostAliases`로 `kafka-bootstrap`에 .40 · .41 · .42를 걸고 Go가 차례로 시도, `dial_timeout` 6s(주소당 2초). Service + EndpointSlice는 Argo CD가 기본으로 관리하지 않아 안 쓴다. Kafka에 붙는 것은 distributor · block-builder · live-store
- 컨슈머 그룹: block-builder는 `block-builder`로 코드에 고정(바꿀 수 없음, 운영 Kafka에 같은 이름 없음 11/24 확인), live-store는 `tempo-live-store-zone-a` · `-b`
- 보존: 차트 기본은 48h → `backendScheduler.config.provider.compaction.compaction.block_retention: 720h`(backend-worker가 물려받음)
- S3 키 사이드카: S3를 부르는 block-builder · querier · query-frontend · backend-scheduler · backend-worker에만(kind에서 구성 요소별 `tempodb_backend_request_duration_seconds_count`로 확인). memcached와 캐시 설정은 끈다(캐시 설정을 비우지 않으면 memcached를 꺼도 1분마다 없는 주소를 찾는다)
- 자원: kind 실측 구성 요소당 22 ~ 43MiB, 조회 직후 querier · live-store 약 160MiB, 합계 약 780MiB → 요청 64Mi(querier · live-store 128Mi) · 한도 512Mi(querier 1Gi)
- OTLP 받는 주소: `.35:4317/4318`의 대상 Service(`tempo-external`)를 distributor로(`component: distributor` 추가). 보내는 쪽(OTel 게이트웨이) 설정은 그대로. Grafana 데이터소스는 `tempo-query-frontend:3200`, Agent는 Pod마다 수집
- Gitops: Application `gpu-monitoring-tempo`를 새 차트로 교체(releaseName `tempo` 그대로 → 리소스 이름 `tempo-*`라 옛 StatefulSet `tempo`와 안 겹친다), AppProject에 CRD 허용, `SkipDryRunOnMissingResource`는 필요 없음(CR을 만들지 않음)
- 기존 트레이스: 같은 S3 접두어(`tempo/`)의 옛 블록을 새 구성이 그대로 읽는다(kind 11/24). prune이 꺼져 있어 차트를 바꿔도 옛 Tempo가 계속 떠 있으므로, 새 구성 확인 직후 옛 StatefulSet을 내린다(옛 압축기와 새 backend-worker가 같은 접두어에서 같이 돌지 않게)
- 순서(main PR 둘): ① 새 values · 이 문서 → main ② Gitops AppProject · Application 교체 ③ 연결부(외부 Service 선택자 · Agent · Grafana URL) → main ④ 옛 Tempo 정리(지시 때) ⑤ 옛 values 삭제. 연결부를 차트 교체보다 먼저 넣으면 Gitops 승인을 기다리는 동안 OTLP 대상이 0개가 된다

## 4. 순서와 검증

각 단계 = kind에서 먼저(같은 values · 같은 버전) → 실 클러스터 → 커밋 하나마다 보고. 실 클러스터 반영은 ArgoCD(Gitops 변경이 있으면 Gitops main → Monitoring main 순서).

| 순서 | 할 일 | 실 클러스터에서 확인할 것 |
|---|---|---|
| 1 | Alertmanager ×2 + ruler 주소 | `/api/v2/status`의 peers 2 · Watchdog이 healthchecks.io로 계속 감 · 한 Pod를 지워도 알람이 감 |
| 2 | Mimir ×3 | ring 페이지 ACTIVE 3 · 한 Pod를 지워도 쓰기 · 조회 · 룰 평가 계속 · Agent 쪽 429/5xx 없음 |
| 3 | Loki ×3 + ruler 분담 | 링 3 · 로그 유입 계속 · 룰 그룹이 한 곳에서만 평가됨(알람 1건) · 한 Pod를 지워도 유입 계속 |
| 4 | Agent ×2 + HA tracker | `cortex_ha_tracker_elected_replica_changes_total` · 중복 샘플 거름(`cortex_distributor_deduped_samples_total`) · 한 Agent를 지우면 30초 안에 다른 쪽 선출 |
| 5 | 분류기 ×2 | Pod 2개 Ready · `classifier_leader`가 1인 Pod 하나 · 대장 지우면 넘겨받음 · 같은 알람 `gpu.fault` 1건 |
| 6 | Tempo 분산형 | 트레이스 수신 · 조회 · 한 live-store를 지워도 최근 트레이스 조회 |

### 6단계 운영 결과 (2026-11-24)
분산형 18 Pod(Tempo 구성 요소 7종 17 Pod + rollout-operator 1) 11:00 Ready → 12:40 연결부(v1.28.0) 반영 → 12:45 옛 StatefulSet `tempo` · PVC 삭제(옛 Pod는 11/16 뒤 받은 span 0건).

| 시험 | 결과 |
|---|---|
| 새 distributor로 보낸 span → query-frontend 조회 | 8초 안에 조회 |
| block-builder S3 쓰기 | PUT 성공 |
| 존 a live-store 3개 삭제 | 조회 계속 200 |
| 존 b 3개 + distributor 1개 동시 삭제, 아직 S3에 없는 span 조회 | 조회 · 수신 계속 200 |
| block-builder 3개 삭제(11:08) | 다시 떠서 쌓인 18건 처리 · 오프셋 18 커밋, 지연 0(빈 파티션은 -1) |
| 연결부 반영 뒤 VIP `.35:4318`로 span → 조회 | 보내기 · 조회 200, 외부 Service 대상 = distributor 2개, Agent 수집 17개 up |

반영 중 걸린 것 2개: ① AppProject(CRD 허용)는 Argo CD가 관리하지 않아 Gitops 머지만으로는 안 바뀐다 → IaC `register-root.yml`과 같은 `kubectl apply -k bootstrap/onprem`으로 적용 ② S3 키 사이드카 기본 uid(65532)와 Tempo uid(1000)가 달라 키 파일을 못 읽고 시작 단계에서 멈췄다 → 사이드카를 uid 1000으로(v1.27.1). kind는 사이드카 없이 정적 키로 시험해서 못 잡았다

마지막에: ADR-18 적용 현황 · 아키텍처 그림 「지금 어디까지 왔나」 표 · 런북 · `docs/monitoring-cluster.md` 첫 줄 갱신, 노드 한 대 재부팅 시험(4절 방식)으로 전체 확인. → 11/24 끝(아래).

### 전체 확인 (2026-11-24 오후)

| 시험 | 결과 |
|---|---|
| monitoring1 재부팅(VIP 리더 둘 · Agent-0이 있던 노드) | 쓰기 · 조회 · 룰 평가 · 알림 · 분류기 계속. 끊김은 API VIP 약 15초 · 서비스 VIP 1초뿐, 3분 뒤 손대지 않고 원상태. 상세 `docs/monitoring-cluster.md` 4.2절 |
| 5단계 「같은 알람 `gpu.fault` 1건」(EKS 학습 중) | 대기 Pod로 port-forward → 표에 없는 Xid 999(`TEST NODE-006`, 실제 노드 · 작업 57812059) → `published` 1(`fault.held`, `job_id` · GPU UUID 채움) → 대장 Pod 삭제 → 2초 만에 대기 쪽이 대장 + Kafka 복원(`gpu.fault` 1시간 5건) → 같은 `event_ts`로 다시 보냄 → `duplicate` 1 |

- Xid 999를 쓴 이유: 복구 컨트롤러가 11/24 13:04부터 execute 모드라 79를 보내면 실제 격리 · 이전이 일어난다. 999는 hold라 작업 57812059에 HOLD와 `RecoveryHold` 알림만 남는다

## 5. 결정 (사용자 2026-11-20)

1. **Tempo는 분산형(B)으로 간다.** 1개 유지(A)도 검토했으나 진짜 HA를 택함. Kafka는 데이터 파트 설비 → 현수 합의 · 토픽 생성 완료(11/20)
2. **Agent 묶음은 전용 라벨**로 구분한다(`cluster` 재사용 안 함). 라벨 이름은 구현 때
3. **분류기는 active-standby**(Lease 대장 하나만 처리)
4. **Jira는 GPU-227 하나로 통일**
5. 받아들이는 위험 2개(6절 맨 위)
6. **Tempo live-store는 존 2벌 × 3 + rollout-operator**(11/23 사용자). 11/24: Kafka 주소는 이름 하나에 브로커 3대(사용자)

## 6. 위험 · 확인할 것

**받아들이는 것**
- **노드를 새로 만들 때만 손이 필요하다.** 디스크가 노드 로컬(local-path)이라 Pod가 그 노드에 묶인다. 노드가 잠깐 죽으면 남은 2개로 버티고, 노드가 돌아오면 자기 디스크로 자동으로 뜬다. VM을 지우고 새로 만들면 옛 PVC를 지워야 그 Pod가 뜬다(데이터는 S3 · 다른 복제본에 있음) → 런북에 절차 추가. 노드끼리 복제하는 저장소(Longhorn · Ceph)는 노드 3대에 무거워 안 쓴다
- **Alertmanager 둘 사이가 끊기면 같은 알림이 두 번 갈 수 있다.** AM은 놓치기보다 두 번 보내기를 택하도록 설계됐다. 분류기는 중복 제거가 있고 Slack은 가끔 두 번 온다
- **Tempo backend-scheduler · backend-worker는 1개씩이다.** 죽은 동안 압축 · 30일 지난 블록 삭제가 늦어질 뿐, 쓰기 · 읽기는 계속된다
- **rollout-operator webhook을 꺼서** live-store 줄이기 보호(쓰지 않음)와 존별 PDB가 없다. 일반 PDB가 6개 중 1개만 내려가게 막는다

**확인할 것 (구현 때)**
- Mimir store-gateway RF 3이면 Pod마다 모든 블록의 index-header를 든다 → PVC 10Gi로 충분한지
- Loki `common.compactor_grpc_address`가 ClusterIP라 리더 아닌 Pod로 갈 수 있음(삭제 기능이 꺼져 있어 무해할 것으로 봄)
- Loki 종료 대기 30초 안에 WAL flush가 끝나는지
- Alertmanager Pod가 준비되기 전 피어 DNS 해석 지연
- ~~Tempo 분산형이 옛 블록(`tempo/`)을 읽는지 · rollout-operator 버전(2주 규칙)~~ → 읽는다 · v0.41.1(10/21 출시) 통과 (kind 11/24, 3.6절)
- 전용 라벨을 넣은 뒤 대시보드 · 룰 · 분류기의 Mimir 조회에 영향 없는지
- **모니터링 묶음 Agent의 HA 선출이 평소에도 자주 바뀐다**(11/24 `cortex_ha_tracker_elected_replica_changes_total` 24시간 171번, 조용한 시간에도 10분에 1 ~ 2번. 관리 클러스터 묶음은 0). 원인 미확인
- 노드가 종료되는 동안 그 노드 Pod가 Service 대상에 남아 약 47초 간헐 실패(`docs/monitoring-cluster.md` 4.2절). 원인 미확인

## 출처 (11/20 조사, 버전 고정)

- Mimir 3.2.4: `configuration-parameters/index.md`(링 기본 memberlist · RF 기본 3 · HA tracker 키), `configure-high-availability-deduplication.md`(첫 시계열만 봄 · 라벨 없으면 통과 · memberlist 권장), `components/ruler/index.md`(로컬 룰 분담), `run-production-environment/scaling-out.md`(줄일 때 절차), `planning-capacity.md`(메모리)
- Loki 3.7.9: `pkg/ruler/base/ruler.go`(`enable_sharding` 기본 false), `pkg/loki/config_wrapper.go`(join_members면 링 전부 memberlist), configuration.md(`enable_alertmanager_discovery`). 차트 21.0.0 `templates/validate.yaml`(오브젝트 스토리지만 요구)
- Tempo 3.1.1: `reference-tempo-architecture/deployment-modes.md`(SSB 제거 · 마이크로서비스는 Kafka 필수), `cmd/tempo/app/modules.go:250`(`PushSpansToKafka = !singleBinary`), `setup-tempo/configure-kafka.md`(복제 3 · 최소 동기 2 · 파티션 ≥ live-store), 차트 tempo-distributed 3.25.0 `templates/validate.yaml`(Kafka 주소 필수)
- Alertmanager v0.34.2 README(모든 AM에 보낼 것), 차트 2.3.1 `templates/statefulset.yaml`(클러스터링 자동) · values.yaml(anti-affinity · PDB 기본 꺼짐)
