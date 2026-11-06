# 학습 Job 계측 규약 (경계 문서 · 모니터링 ↔ 서비스)

train.py(노찬경, GPU-96)가 `/metrics`로 내보내는 지표의 이름 · 라벨 · 버킷을 고정한다. 모니터링 쪽 룰(스트래글러 MON-STD-08 · 정체 판정 · 재개 확인)과 Grafana 패널이 이 이름을 그대로 참조한다. 바꿀 때는 이 문서를 먼저 고치고 양쪽 코드를 맞춘다.

- 합의: 2026-11-05 (Slack). 포트 8000, phase 기준 `warmup_steps` 승인
- 변경 요청: 2026-11-06. 랭크별 계산 시간(프로파일러 샘플링) · 데이터 대기 지표 추가, `train_step_seconds`는 Job 스텝 시간으로 의미 정정 (3-1절). 찬경 답 대기
- 구현: trainer 컨테이너의 train.py가 `prometheus_client`로 직접 노출. 사이드카 경유 아님 (스텝 시간은 학습 루프 안에서만 잴 수 있고 rank마다 나와야 해서)
- 수집: 워커 Prometheus Agent가 Pod 어노테이션(`kubernetes_sd` role pod)으로 찾아 15초마다 scrape → 중앙 Prometheus

## 1. 지표 8개

| 이름 | 타입 | 라벨 | 뜻 | 갱신 시점 |
|---|---|---|---|---|
| `train_step_seconds` | histogram | `job_id` `generation` `rank` `phase` | 한 step의 벽시계 시간(초). **FSDP 동기화 때문에 랭크 간 값이 거의 같다 = 사실상 Job 스텝 시간** (3-1절). 버킷은 3절 | 매 step 완료 직후 (`loss.item()` 뒤, 새 동기화 지점 안 만듦) |
| `train_rank_compute_seconds` | gauge | `job_id` `generation` `rank` | 프로파일된 step에서 이 랭크의 GPU **계산** 커널 시간 합(초). NCCL 제외 | N step마다 1회 (3-1절) |
| `train_rank_comm_seconds` | gauge | `job_id` `generation` `rank` | 같은 step의 NCCL 커널 시간 합(초). 다른 랭크를 기다린 시간이 여기 들어감 | N step마다 1회 |
| `train_data_wait_seconds` | histogram | `job_id` `generation` `rank` `phase` | step 시작부터 배치가 GPU에 올라올 때까지(초). 집합 통신 전이라 랭크마다 다르게 나옴 | 매 step |
| `train_current_step` | gauge | `job_id` `generation` `rank` | 완료한 global step | 매 step |
| `train_loss` | gauge | `job_id` `generation` `rank` | 마지막 step의 loss | 매 step |
| `train_stage` | gauge (0/1) | `job_id` `generation` `rank` `stage` | 현재 단계. `stage` ∈ `init` `train` `checkpoint` `finalize`, 해당 단계만 1 | 단계 바뀔 때 |
| `train_last_checkpoint_step` | gauge | `job_id` `generation` `rank` | 마지막으로 `.done`을 만든 step | `/ckpt/step-{8}.done` 생성 직후 |

- 접두어 `train_`, 단위는 접미어(`_seconds`). 이 여덟 개 외에는 추가하지 않는다 (필요하면 이 문서부터)
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
- 카디널리티: histogram 2개 × (job 1 × 세대 ≤ 5 × rank 4 × phase 2 × 버킷 8) + gauge 5개 × 20 ≈ 740 시계열. 문제 없음

## 3. 히스토그램 버킷

```
buckets=(0.1, 0.3, 1, 3, 10, 30)   # 초. +Inf는 자동
```

- 약 3배 간격 (SRE Book 6장 "Worrying About Your Tail": 지수 간격이면 분포가 보인다)
- GPT-2 small · T4 · batch 4 기준 추정값. **기준선 3h 누적 측정 뒤 p50이 한가운데 오도록 조정** → 바뀌면 이 문서와 train.py 같이 (histogram 버킷 변경은 비가산이라 바꾼 시점 전후를 섞어 쓰지 않는다)
- 왜 histogram인가: 분포(p50 · p99)를 봐야 한다. gauge 하나면 느린 step 몇 개가 평균에 묻힌다. `train_data_wait_seconds`도 같은 버킷을 쓴다
- `histogram_quantile`은 버킷 안에서 선형 보간한다. 샘플이 전부 (0.3, 1] 버킷이면 p50은 실제 값과 무관하게 0.65로 나온다 (A5 promtool 테스트). 비율로 쓰면 상쇄되지만 절대값으로 쓸 땐 버킷 경계가 곧 해상도 → 기준선 측정 뒤 버킷 조정이 중요한 이유

## 3-1. 랭크별 계산 시간: 왜 스텝 시간으로는 안 되고, 어떻게 재나

**문제.** FSDP는 매 step 집합 통신(all-gather · reduce-scatter)에서 모든 랭크가 가장 느린 랭크를 기다린다. 그래서 `loss.item()` 뒤에 잰 벽시계 step 시간은 어느 랭크에서 재도 "가장 느린 랭크의 시간"으로 같다. 한 GPU가 느려졌다는 사실은 보이지만 **어느 랭크인지는 안 보인다.** 우리 구성(노드 1 = GPU 1, 랭크 간 통신이 전부 노드 간 네트워크)은 통신 비중이 커서 더 묻힌다.

**해법: 프로파일러 샘플링.** GPU 커널은 계산 커널과 NCCL 커널(통신 + 대기) 둘로 나뉜다. `torch.profiler`로 N step마다 1 step만 커널 단위 시간을 받아, 이름에 `nccl`이 들어가면 comm, 아니면 compute로 합산한다. 느린 랭크는 compute가 길고 comm이 짧다.

```
rank 0  compute 0.62s  comm 0.31s  ─┐
rank 1  compute 0.61s  comm 0.32s   │ step 0.95s (전부 같음)
rank 2  compute 0.60s  comm 0.33s   │
rank 3  compute 0.90s  comm 0.04s  ─┘  ← 스트래글러
```

```python
# 스케치. 속성 이름(self_device_time_total 등)은 PyTorch 버전에 맞춰 확인
from torch.profiler import profile, ProfilerActivity, schedule
from torch.autograd import DeviceType

def on_ready(prof):
    comp = comm = 0.0
    for e in prof.events():                      # 커널 이벤트, 시간 단위 µs
        if e.device_type != DeviceType.CUDA:
            continue
        if "nccl" in e.name.lower():
            comm += e.self_device_time_total
        else:
            comp += e.self_device_time_total
    RANK_COMPUTE.labels(**L).set(comp / 1e6)
    RANK_COMM.labels(**L).set(comm / 1e6)

prof = profile(activities=[ProfilerActivity.CUDA],
               schedule=schedule(wait=N-2, warmup=1, active=1),
               on_trace_ready=on_ready, record_shapes=False, with_stack=False)
prof.start()
for step in loop:
    ...                                          # 평소 학습
    prof.step()                                  # 매 iteration 끝
```

- 비용: 프로파일된 step만 1.5~2배 느려짐. N=25면 전체 영향 4~8%, N=50이면 2~4%. **N은 step 시간 실측 뒤 확정** (스트래글러 감지 창 2분 안에 샘플 2개 이상 들어오게)
- 모델이 커져도 그대로 쓴다. DDP였다면 `no_sync()` 구간 CUDA 이벤트로 공짜로 됐겠지만 FSDP 유지 결정(큰 모델 예정)이라 해당 없음
- `train_data_wait_seconds`는 통신 전 구간이라 `time.perf_counter()`로 충분. CPU 쪽 대기라 비동기 문제 없음
- 랭크 간 통신 길(NIC 드롭 · 재전송 · PCIe replay)은 학습 코드가 못 잼 → node-exporter · dcgm-exporter 노드 지표를 rank → node로 조인

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
| 스트래글러 (GPU) | `train_rank_compute_seconds / on(job_id, generation) group_left quantile by (job_id, generation) (0.5, train_rank_compute_seconds) > 1.5` for 2m. 기준값은 **랭크 값들의 중앙값**(합친 분포의 p50 아님. 느린 랭크가 기준을 끌어올려 정상 랭크가 1.0 아래로 밀림, A5 테스트 0.85) | MON-STD-08 |
| 스트래글러 (노드) | 같은 구조로 `histogram_quantile(0.5, sum by (job_id, generation, rank, le) (rate(train_data_wait_seconds_bucket{phase="steady"}[5m])))` ÷ 랭크 중앙값 > 1.5 for 2m. 튀면 GPU가 아니라 노드(로더 · 디스크 · CPU) 문제 | MON-STD-08 보조 |
| Job 저하 | `histogram_quantile(0.5, sum by (job_id, generation, le) (rate(train_step_seconds_bucket{phase="steady"}[5m])))`가 기준선 × 1.5 초과. 누가 느린지는 위 둘로 | MON-STD-08 전제 |
| 학습 정체 | `train_stage{stage="train"}==1 and increase(train_current_step[5m])==0` | MON-STD-20 보조, 분류기 `hold`/`restart_in_place` 입력 |
| 재개 확인 (ETTR) | 새 `generation` 라벨로 `train_current_step`이 처음 증가한 시각 = `job.resumed`와 대조 | SLO-1 recover_s |
| 기준선 | `quantile_over_time`(steady, 주입 구간 제외) p50 · p95 · p99 | 표준 2절 |
| 손실 스텝 | 장애 직전 `train_current_step` − `train_last_checkpoint_step` | 데이터 파트 `lost_steps` 대조 |

## 6. 안 하는 것

- Pushgateway · TensorBoard · W&B 안 씀. 오래 사는 Pod라 pull이 맞음
- `injected` phase 라벨 안 넣음 (2절)
- 종료 코드 75(STOP)로 끝날 때 지표 쪽 특별 처리 없음. 분류기가 장애로 안 셈 (fault-event.md 3절)
- 매 step CUDA 이벤트 측정 안 함. 랭크별 계산 시간은 프로파일러 샘플링(3-1절)으로 대신. 상시 프로파일러도 안 함(2배 느려짐)

## 7. 계측 비용 확인 (기준선 실험 때)

`observe()`·`set()`은 메모리 숫자 갱신이라 수 µs, step은 수백 ms. 그래도 지표 켠 실행과 끈 실행의 steady 스텝 시간 p50을 한 번 비교해 "계측 비용 측정 오차 이내"를 숫자로 남긴다 (MON-STD 근거 행 후보).
