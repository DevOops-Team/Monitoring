# EKS 첫 테스트 — 수집 층 · 학습 지표 · 학습 중단 (GPU-217)

- 측정: 2026-11-19 13:53 ~ 17:51 KST (EKS가 떠 있던 구간)
- 구성: `eks-a` · `eks-b`(us-east-2a · 2b), 클러스터마다 t3.large 1대(`system`) + g4dn.xlarge 5대(`training` 4 · `spare` 1, Tesla T4, 드라이버 580.178.04). 수집 층은 ApplicationSet `gpu-eks-monitoring`이 자동으로 올렸다
- 범위: 기본 학습 흐름과 사용자 중단까지. **장애 주입(Xid · Pod 강제 종료)은 하지 않았다** → 분류기가 받은 알람 0건, `gpu.fault` 0건
- 방법: 전부 Mimir(`172.30.0.35:9009`) · Loki(`:3100`) 조회. EKS에는 직접 접속하지 않았다

## 1. 확인 순서 결과 (`deploy/README.md` 「EKS가 뜬 날 확인 순서」)

| # | 무엇 | 결과 |
|---|---|---|
| 1 | ArgoCD 앱 | `gpu-eks-monitoring-eks-a` · `-eks-b` 자동 생성, Synced · Healthy |
| 2 | 수집 대상 | 클러스터마다 Agent 1 · KSM 1 · OTel 게이트웨이 1 · Kueue 1 · Fluent Bit 6 · node-exporter 6 · kubelet 6 · cAdvisor 6 |
| 3 | 이름 주입 | `cluster="unset"` 시계열 없음 |
| 4 | dcgm-exporter | 처음엔 0 (2절). 한도를 올린 뒤 클러스터마다 5, `DCGM_*` 23종 · 115 시계열 |
| 5 | dcgm 라벨 | `node` · `UUID` 있음 → `enrich.py` · `failover.yml` 수정 불필요. 그 밖 `gpu` · `device` · `modelName` · `hostname` · `pci_bus_id` |
| 6 | 노드 라벨 | `label_topology_kubernetes_io_zone` · `label_node_kubernetes_io_instance_type` 나옴 |
| 7 | 커널 로그 | `service_name="kernel"` 클러스터마다 약 1.5만 줄(4시간). `NVRM: Xid` 줄은 0 (주입 없음) |
| 8 | T4가 내는 지표 | 한계 온도 85 · 93 · 96°C(최대 동작 · 감속 · 차단), 유휴 25°C. `THERMAL_VIOLATION` 0. `POWER_VIOLATION`은 3절 |
| 9 | 학습 Pod 라벨 | `label_devoops_job_id` · `label_devoops_generation` 나옴 |
| 10 | trainer 지표 | 처음엔 0 (4절). Backend 수정 뒤 8개 전부 |
| 11 | 분류기 | 하지 않음(주입 날) |

## 2. dcgm-exporter 메모리

| 시각 | 한도 | 상태 |
|---|---|---|
| 13:53 ~ 15:47 | 256Mi | 10대 전부 `OOMKilled` 반복, 재시작 누계 클러스터당 약 125 ~ 135회. 로그는 「DCGM successfully initialized」 직후 재시작 |
| 15:47 ~ | 1Gi | 재시작 0회 |

- 사용량: 시작 직후 150MiB → 1 ~ 2분 사이 약 410 ~ 420MiB → 10분 뒤 **432 ~ 437MiB에서 평탄**. 4시간 최대 438MiB, 학습 중에도 같다
- 결론: 누수가 아니라 평소 사용량이 256Mi보다 크다. kind에는 GPU가 없어 이 값을 볼 수 없었다
- 남은 일: 한도 768Mi · 요청 512Mi로 조정(요청 128Mi는 실제의 3분의 1). 다음 EKS 기동 때 반영을 확인한다

## 3. `DCGM_FI_DEV_POWER_VIOLATION`

- 17:20경 값이 클러스터 최대 약 1.26 × 10¹³ ns(약 12,600초). 노드가 뜬 뒤 거의 내내 쌓인 크기다
- 실제 전력 제한인지, T4에서 이 카운터가 유휴에도 오르는지는 **확인하지 못했다**. 학습 중과 유휴의 증가 속도를 비교해야 한다(MON-STD-02 근거로 쓰기 전에)

## 4. 학습 지표 (`train_*`)

- 원인: Backend가 만드는 학습 Pod 템플릿에 `prometheus.io/*` 어노테이션이 없어 Agent의 `annotated-pods` 잡이 대상으로 잡지 않았다. Backend PR #28 · #29(v1.7.0, 17:24 배포)로 추가
- 확인: 작업 `fd6e69de` 세대 1 (SmolLM2-135M, 2랭크, eks-a, 17:27 시작, 310 step)

| 항목 | 랭크 0 | 랭크 1 |
|---|---|---|
| step 시간 평균 (`steady`) | 1.34초 | 1.34초 |
| `train_rank_compute_seconds` | 1.38초 | 1.37초 |
| `train_rank_comm_seconds` | 0.55초 | 0.83초 |
| 데이터 대기 평균 | 0.19ms | 0.28ms |
| 계산 시간 ÷ 중앙값 (MON-STD-08 식) | 1.004 | 0.996 |

- Pod당 수집 대상 1개(init 컨테이너는 Agent가 뺀다). 라벨 `job_id` · `generation` · `rank` · `phase`는 규약과 같다
- `train_rank_*`는 첫 프로파일(25 step) 뒤에, `train_last_checkpoint_step`은 첫 저장 뒤에 생긴다. **`train_last_checkpoint_step`은 랭크 0에서만 나왔다**
- **step 시간이 전부 버킷 (1, 3]에 들어갔다** (경계 0.1 · 0.3 · 1 · 3 · 10 · 30). `histogram_quantile`로는 분포가 보이지 않는다 → 버킷 변경을 김현수에게 제안(11/19)

## 5. 사용자 중단 때 종료 시간

| 작업 | 중단 요청 | Pod 소멸 | 요청 뒤 학습 | 비고 |
|---|---|---|---|---|
| `6bf3cb2b` | 15:25:28 | 15:26:33경 | step 155 → 203 | 수정 전 |
| `bd0ca4f0` | 16:34:09 | 16:35:20경 | step 101 → 129 | 수정 전 |
| `fd4bcd65` | 16:53:15 | 16:53:19경 | 없음 (약 2초) | Backend `exec torchrun` 배포 뒤 |

- 원인: trainer 명령이 `sh -c "…; torchrun …"`이라 SIGTERM을 `sh`(PID 1)가 받고 넘기지 않았다. 유예 60초 뒤 SIGKILL. 펜싱(relocator가 옛 JobSet 삭제)도 같은 경로
- 수정 뒤 로그: `Received 15 death signal, shutting down workers` → `SignalException: Process 1 got signal: 15`. 정상 종료인데 트레이스백이 남는다
- 재는 법: `kube_pod_deletion_timestamp` − 유예 60초 = 요청 시각, 사이드카 `progress.sent` 로그, `kube_pod_info` 마지막 샘플(15초 간격이라 소멸 시각은 ±15초)
- 세 번 모두 원인 알람은 뜨지 않았고 분류기는 아무것도 내지 않았다

## 6. EKS를 내릴 때

- `FailoverSiteDown`이 두 클러스터에 17:48 ~ 17:56 발화. 수신자 `classifier-pending`(받는 곳 없음)이라 Slack · 분류기로 가지 않았다
- `MonitoringTargetDown` · `ClassifierDown`은 뜨지 않았다(대상 자체가 사라져 `up == 0`이 생기지 않는다)

## 7. 같이 고친 것

| 무엇 | 어디 |
|---|---|
| dcgm-exporter 메모리 한도 1Gi | Monitoring v1.5.2 |
| 복구 컨트롤러 Pod에 수집 어노테이션(`recovery_hold_total`이 수집되지 않고 있었다) | Gitops #71, 16:36부터 `up` 3 |
| 학습 Pod에 수집 어노테이션 | Backend v1.7.0 |
| 노드 종류 라벨 `node_role`(training · spare · system)과 「Kubernetes 노드」 대시보드 표시 | Monitoring v1.6.0 · v1.7.0 |
