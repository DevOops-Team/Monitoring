# Monitoring

GPU Failover 프로젝트(팀 DevOops)의 모니터링 레포. 지표 · 로그 · 트레이스를 모으고, 알람으로 장애를 잡아, 분류기가 원인을 판정해 `gpu.fault` 사건으로 내보낸다. 담당: 이태림(정) · 김성모(부).

## 어디에 무엇이 올라가나

`deploy/` 아래 폴더 이름이 곧 올라가는 곳이다.

| 올라가는 곳 | 무엇 | 폴더 | 누가 배포하나 |
|---|---|---|---|
| 모니터링 클러스터 (온프레, 3노드) | Mimir · 알람 룰 · 대시보드 · Prometheus Agent · 밖으로 여는 Service | `deploy/monitoring-cluster/storage/` | Argo CD `gpu-monitoring-base` |
| 〃 | 차트로 올리는 것의 values (Loki · Tempo · Alertmanager · Grafana · exporter들) | `deploy/monitoring-cluster/values/` | Argo CD `gpu-monitoring-<이름>` (차트 버전은 Gitops) |
| 〃 | 이 클러스터 자신의 로그 (Fluent Bit) | `deploy/monitoring-cluster/logs/` | Argo CD `gpu-monitoring-logs` |
| 〃 | 분류기 매니페스트 | `deploy/monitoring-cluster/classifier/` | Argo CD `gpu-monitoring-classifier` (Gitops overlay가 릴리스 태그로 참조) |
| 관리 클러스터 (온프레) | 수집 층: Agent ×2 · KSM · Fluent Bit · OTel 게이트웨이 | `deploy/manager-cluster/` | Argo CD `gpu-monitoring-agents` |
| EKS (`eks-a` · `b` · `c`, 켰다 끔) | 수집 층 + GPU 노드용 (dcgm-exporter · spot-notice-exporter), 장애 주입 시험 Pod | `deploy/eks/` | Argo CD ApplicationSet `gpu-eks-monitoring` (EKS가 등록되면 자동) |
| 위 클러스터들 공통 | 수집 층의 공통 매니페스트. 직접 배포하지 않고 클러스터별 폴더가 가져다 쓴다 | `deploy/cluster-common/` | - |
| 노트북 kind | 검증용 overlay | `deploy/kind/` | `scripts/classifier-e2e-kind.sh` · `scripts/deploy-k8s.sh --agents-kind` |
| 쿠버네티스 밖 (Proxmox 호스트 · Kafka · DB VM · pfSense) | node-exporter · Fluent Bit | `ansible/` | 사람이 Ansible 실행 (`ansible/README.md`) |

Argo CD Application은 Gitops 레포 `bootstrap/onprem/applications/`에 있다. 이 레포 `main`에 머지하면 반영된다(분류기만 릴리스 태그 · 이미지 digest를 배포 봇이 Gitops에 올린다).

## 그 밖의 폴더

| 폴더 | 무엇 |
|---|---|
| `classifier/` | 분류기 코드(Python · FastAPI)와 테스트. 이미지 `ghcr.io/devoops-team/classifier` |
| `dcgm-hostengine/` | dcgm-exporter 사이드카 이미지(Xid 주입용 DCGM 엔진) |
| `schemas/` | 장애 이벤트(`gpu.fault`) JSON 스키마와 예시. 다른 파트와의 경계 규약 |
| `docs/` | 설계 · 규약 문서, `docs/measurements/`는 측정 기록 |
| `scripts/` | 비밀 생성 · 비상 배포 · 로컬 검증 스크립트 |
| `deploy/versions.env` | 차트 · 이미지 버전 한곳 정리 |

## 어디부터 읽나

| 알고 싶은 것 | 문서 |
|---|---|
| 스택 구성 · 적용 · 알람 대응 · 장애 주입 절차 (런북) | `deploy/README.md` |
| 분류기가 무엇을 받아 어떻게 판정하나 | `docs/classifier.md` |
| 장애 이벤트 형식과 action 뜻 | `docs/fault-event.md` · `schemas/` |
| 무엇을 어떤 임계로 보나 (MON-STD-nn) | `docs/monitoring-standard.md` |
| 이중화 구성 | `docs/monitoring-ha.md` |
| 모니터링 클러스터 자체 (VM · 노드) | `docs/monitoring-cluster.md` |
| 수집 층이 어느 노드에 뜨나 | `docs/worker-agents.md` |
| 학습 지표 규약 | `docs/training-metrics.md` |

## 작업 규칙

- 브랜치: `GPU-<번호>-<설명>` → `develop` → `main` (merge commit). main PR 본문에 `## 릴리스 노트` 필수
- 검사: `scripts/deploy-k8s.sh --lint-only` (룰 · 대시보드 · 렌더 · values), 분류기는 `cd classifier && .venv/bin/python -m pytest`
- 폴더를 옮기거나 이름을 바꿀 때는 Gitops Application의 `path` · `valueFiles`를 같이 바꿔야 한다
