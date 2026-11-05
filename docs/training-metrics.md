# 학습 Job 계측 규약 (경계 문서 · 모니터링 ↔ 서비스)

train.py(노찬경, GPU-96)가 `/metrics`로 내보내는 지표의 이름 · 라벨 · 버킷을 고정한다. 모니터링 쪽 룰(스트래글러 MON-STD-08 · 정체 판정 · 재개 확인)과 Grafana 패널이 이 이름을 그대로 참조한다. 바꿀 때는 이 문서를 먼저 고치고 양쪽 코드를 맞춘다.

- 합의: 2026-11-05 (Slack). 포트 8000, phase 기준 `warmup_steps` 승인
- 구현: trainer 컨테이너의 train.py가 `prometheus_client`로 직접 노출. 사이드카 경유 아님 (스텝 시간은 학습 루프 안에서만 잴 수 있고 rank마다 나와야 해서)
- 수집: 워커 Prometheus Agent가 Pod 어노테이션(`kubernetes_sd` role pod)으로 찾아 15초마다 scrape → 중앙 Prometheus

## 1. 지표 5개

| 이름 | 타입 | 라벨 | 뜻 | 갱신 시점 |
|---|---|---|---|---|
| `train_step_seconds` | histogram | `job_id` `generation` `rank` `phase` | 한 step의 벽시계 시간(초). 버킷은 2절 | 매 step 완료 직후 (`loss.item()` 뒤, 새 동기화 지점 안 만듦) |
| `train_current_step` | gauge | `job_id` `generation` `rank` | 완료한 global step | 매 step |
| `train_loss` | gauge | `job_id` `generation` `rank` | 마지막 step의 loss | 매 step |
| `train_stage` | gauge (0/1) | `job_id` `generation` `rank` `stage` | 현재 단계. `stage` ∈ `init` `train` `checkpoint` `finalize`, 해당 단계만 1 | 단계 바뀔 때 |
| `train_last_checkpoint_step` | gauge | `job_id` `generation` `rank` | 마지막으로 `.done`을 만든 step | `/ckpt/step-{8}.done` 생성 직후 |

- 접두어 `train_`, 단위는 접미어(`_seconds`). 이 다섯 개 외에는 추가하지 않는다 (필요하면 이 문서부터)
- 모든 rank가 전부 낸다. rank 0만 쓰는 progress.json(DynamoDB, 프론트용)과는 별개
- `train_stage`가 있는 이유: 체크포인트 저장 중엔 step이 안 오르는 게 정상. 이걸 "학습 정체"로 오판하지 않으려고 (한송이 "장애 판단" 6절의 현재 단계와 같은 용도)

## 2. 라벨 값

| 라벨 | 값 | 출처 |
|---|---|---|
| `job_id` | 백엔드 작업 ID 8자리 hex (예 `3f9a1c2e`) | env `JOB_ID` |
| `generation` | 세대 번호 (예 `2`) | env `GENERATION` |
| `rank` | `0`~`3` (node rank = Pod 번호) | `$JOB_COMPLETION_INDEX` |
| `phase` | `warmup` (step ≤ `warmup_steps`) · `steady` (그 뒤) | config.json `warmup_steps` |

- `phase`는 둘뿐이다. 장애 주입 구간(`injected`)은 학습 코드가 모르고, 모니터링이 Kafka `fault.injection` 사건 시각으로 걸러낸다. 기준선은 `phase="steady"`에서 주입 구간을 뺀 것 (monitoring-standard.md 2절)
- 카디널리티: job 1개 × 세대 ≤ 5 × rank 4 × phase 2 × 버킷 8 ≈ 320 시계열. 문제 없음

## 3. 히스토그램 버킷

```
buckets=(0.1, 0.3, 1, 3, 10, 30)   # 초. +Inf는 자동
```

- 약 3배 간격 (SRE Book 6장 "Worrying About Your Tail": 지수 간격이면 분포가 보인다)
- GPT-2 small · T4 · batch 4 기준 추정값. **기준선 3h 누적 측정 뒤 p50이 한가운데 오도록 조정** → 바뀌면 이 문서와 train.py 같이 (histogram 버킷 변경은 비가산이라 바꾼 시점 전후를 섞어 쓰지 않는다)
- 왜 histogram인가: 스트래글러는 평균이 아니라 rank별 p99로 잡는다. gauge 하나면 느린 step 몇 개가 평균에 묻힌다

## 4. 노출과 수집

train.py 시작 때:

```python
from prometheus_client import start_http_server
start_http_server(8000)
```

JobSet 템플릿 (김현수 합의안에 추가, 노찬경이 반영):

```yaml
# trainer 컨테이너
ports:
  - {containerPort: 8000, name: metrics}
# Pod 템플릿 metadata.annotations
prometheus.io/scrape: "true"
prometheus.io/port: "8000"
prometheus.io/path: "/metrics"
```

- 포트 8000은 Pod 안 포트라 노드의 dcgm-exporter 9400 · node-exporter 9100과 충돌 없음. trainer는 torchrun 29500만 쓰고 있었음 (찬경 확인)
- Agent 쪽 relabel: 어노테이션 `prometheus.io/scrape=true`인 Pod만, `cluster`는 Agent `external_labels`, `node`는 `__meta_kubernetes_pod_node_name`에서. 설정은 2차 스택 Agent 매니페스트에 (worker-agents.md)

## 5. 이 지표로 만드는 것

| 용도 | 식 (초안) | 표준 |
|---|---|---|
| 스트래글러 | `rank별 histogram_quantile(0.99, rate(train_step_seconds_bucket{phase="steady"}[5m])) / 전체 rank 중앙값 > 1.5 for 2m` | MON-STD-08 |
| 학습 정체 | `train_stage{stage="train"}==1 and increase(train_current_step[5m])==0` | MON-STD-20 보조, 분류기 `hold`/`restart_in_place` 입력 |
| 재개 확인 (ETTR) | 새 `generation` 라벨로 `train_current_step`이 처음 증가한 시각 = `job.resumed`와 대조 | SLO-1 recover_s |
| 기준선 | `quantile_over_time`(steady, 주입 구간 제외) p50 · p95 · p99 | 표준 2절 |
| 손실 스텝 | 장애 직전 `train_current_step` − `train_last_checkpoint_step` | 데이터 파트 `lost_steps` 대조 |

## 6. 안 하는 것

- Pushgateway · TensorBoard · W&B 안 씀. 오래 사는 Pod라 pull이 맞음
- `injected` phase 라벨 안 넣음 (2절)
- 종료 코드 75(STOP)로 끝날 때 지표 쪽 특별 처리 없음. 분류기가 장애로 안 셈 (fault-event.md 3절)
- GPU 비동기 보정(CUDA event) 안 함. 벽시계 시간이면 이번 범위에 충분. 필요해지면 `train_step_compute_seconds`를 따로 추가

## 7. 계측 비용 확인 (기준선 실험 때)

`observe()`·`set()`은 메모리 숫자 갱신이라 수 µs, step은 수백 ms. 그래도 지표 켠 실행과 끈 실행의 steady 스텝 시간 p50을 한 번 비교해 "계측 비용 측정 오차 이내"를 숫자로 남긴다 (MON-STD 근거 행 후보).
