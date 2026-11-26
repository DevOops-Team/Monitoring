# 분류기 설계 (GPU-192)

분류기는 Alertmanager가 보낸 원인 알람을 받아 「무슨 일이 났고 무엇을 추천하는가」를 판정하고, Kafka `gpu.fault`에 이벤트 한 건으로 낸다.
출력 형식은 [fault-event.md](fault-event.md)와 [`schemas/fault-event.v1.json`](../schemas/fault-event.v1.json)이 정본이다. 이 문서는 그 이벤트를 **어떻게 만드는가**를 적는다.

- 위치: 온프레 모니터링 클러스터(ADR-18 · ADR-29), 코드 `classifier/`(Python · FastAPI)
- 판정은 표(룰)로만 한다. 표에 없는 신호는 `hold`로 내고, 나중에 모델을 꽂을 자리 하나만 둔다(5절)
- 이번 범위 밖: 원인 알람 룰(Loki Xid 룰 · DCGM 룰), 스트래글러 판정, 실 클러스터 배포, Tempo span 전송, 2중화
- 검증: 단위 테스트 `classifier/tests/`, kind 종단 검증 `scripts/classifier-e2e-kind.sh`(런북 `deploy/README.md` 「분류기」). 실 클러스터에는 아직 올리지 않았다

## 1. 흐름

```
Alertmanager 웹훅 ─▶ signals ─▶ enrich ─▶ intended ─▶ rules ─▶ dedup ─▶ publish ─▶ Kafka gpu.fault
 POST /alerts        정규화     Mimir     거르기      표 판정   ±45초    검증+봉투
                                           ▲
                             Kafka job.lifecycle 구독 (의도된 종료 표 · 최신 세대 · 남은 스텝)
```

| 모듈 | 하는 일 |
|---|---|
| `signals.py` | 웹훅의 알람 1건을 내부 신호로 바꾼다. `ts`는 annotation `event_ts`만 쓴다(수신 시각 금지). resolved 알람은 버린다 |
| `enrich.py` | Mimir 조회로 신호에 없는 값을 채운다. 조회가 실패하면 모르는 값으로 둔다(`collector_healthy=false` · `job_id=null`). 아래 표. `job_failed`는 Loki도 조회한다(죽은 노드, 2026-11-26 GPU-288). `pod_exit` · `job_failed`는 같은 로그에서 GPU 메모리 부족 줄도 찾는다(GPU-222) — 신호가 들어오는 길은 그대로 Alertmanager 웹훅 하나다 |
| `intended.py` | `job.lifecycle`을 읽어 「의도된 종료」 표와 작업별 최신 세대를 유지한다(4절). `job.progress`의 `step` · `total_steps`로 작업 · 세대의 남은 스텝도 기억한다(스트래글러 이득 계산, 3.3절) |
| `rules.py` · `rules.yaml` | 3절 표로 `category` · `suggested_action` · `confidence`를 정한다 |
| `dedup.py` | 같은 사건을 한 번만 낸다. 규칙은 fault-event.md 2.1절(±45초 창, 키와 action이 둘 다 같으면 재발행 안 함). `pod_exit`의 대상은 노드가 아니라 Pod 이름이다(같은 노드에서 거의 동시에 끝난 다른 Pod를 삼키지 않게) |
| `event.py` | 신호 + 판정 + Mimir 값 → v1 장애 이벤트. `event_id`(= trace-id)는 판정마다 새로 만든다 |
| `publish.py` | v1 스키마 검증 → 봉투 → Kafka 발행(6절) |
| `pipeline.py` | 위 단계를 알람 1건마다 순서대로 부르고 결과를 카운터 `classifier_alerts_total{outcome}`로 센다 |
| `app.py` | `POST /alerts` · `/healthz` · `/metrics`. Kafka 전달에 실패한 알람이 있으면 503을 돌려줘 Alertmanager가 다시 보내게 한다(이미 낸 것은 중복 제거로 걸러짐). 대장이 아니면 받은 웹훅을 대장에게 넘긴다(7.1절) |
| `lease.py` | 대장 선출. 쿠버네티스 Lease를 잡은 Pod 하나만 위 흐름을 돈다(7.1절) |

`enrich.py`가 묻는 것 (site 신호는 작업 목록만 묻는다: `collector_healthy=false` · `az=unknown`. node_lost는 그 노드의 작업 목록 · 신호 시각의 `az` · 클러스터 수집 경로(Agent · 게이트웨이, 그 노드의 Fluent Bit은 빼고)):

| 채우는 값 | 지표 | 기준 시각 | 못 찾으면 |
|---|---|---|---|
| `collector_healthy` | 그 클러스터의 `up{job="prometheus-agent"}` · `up{job="otel-gateway"}`와 그 노드의 `up{job="fluent-bit"}`가 **셋 다 1** | 지금(분류 시점) | `false` → `hold` |
| `job_id` · `generation` | `kube_pod_labels`의 `label_devoops_job_id` · `label_devoops_generation`. `pod_exit`는 Pod 이름으로, 나머지는 `kube_pod_info`로 그 노드의 Pod를 찾는다 | 신호 발생 시각(직전 5분) | `null`. 한 노드에 작업이 둘이어도 `null` → 컨트롤러가 HOLD |
| `gpu_uuid` | 커널 로그 경로 Xid만. dcgm-exporter 지표의 `UUID` 라벨(g4dn은 노드당 GPU 1장) | 신호 발생 시각(직전 5분) | `null` → 중복 제거 대상이 `node`가 된다 |
| `az` | `kube_node_labels`의 `label_topology_kubernetes_io_zone` | 지금 | `unknown` |
| `job_failed`의 `node` · `raw.log_line` | **Loki**(env `LOKI_URL`): 그 클러스터 · `training` · `trainer` 컨테이너에서 Pod 이름이 `job-<job_id>-g<세대 4자리>-…`인 로그 중 `failed (exitcode`가 든 줄을 시간 순으로 1줄. 그 줄의 스트림 라벨 `k8s_node_name`이 `node`, 줄 원문이 `raw.log_line`, 그 노드의 `az`도 채운다. 조회 제한 3초 | 신호 발생 시각 −10분 ~ +1분 | `node: null` · `az: unknown`, `raw.log_line`은 알람 문구. 사건은 그대로 낸다 |
| `pod_exit` · `job_failed`의 메모리 부족 여부 · `raw.log_line` | **Loki**: 같은 조건(클러스터 · `training` · `trainer` · 그 작업 · 세대의 모든 Pod)에서 `CUDA out of memory`가 든 줄을 시간 순으로 1줄. 있으면 `refuse`(3.3절), `job_failed`는 `node`도 이 줄의 Pod 것을 쓴다. `pod_exit`는 종료 사유가 `OOMKilled`가 아니고 작업 · 세대를 알 때만 조회. 조회 제한 3초 | 신호 발생 시각 −10분 ~ +1분 | 메모리 부족이 아닌 것으로 본다(`restart_in_place`) |
| `straggler`의 step 시간 두 개 | 그 작업 · 세대의 5분 평균 step 시간 `sum(rate(train_step_seconds_sum{phase="steady"}[5m])) / sum(rate(train_step_seconds_count{phase="steady"}[5m]))`. FSDP는 랭크가 같이 나아가 step 시간이 랭크마다 같으므로 작업 전체로 본다. `job_id` · `generation` · `node`는 알람 라벨에서, `gpu_uuid`는 Xid와 같은 식으로 노드에서 찾는다 | 느린 값 = 지금, 정상 값 = 신호 발생 시각(느려진 시각) 1분 전 | 하나라도 없으면(시작부터 느렸음 · 조회 실패) 이득을 계산할 수 없다 → `hold`, `straggler_evidence` 없이 낸다 |
| site의 작업 목록 | 그 클러스터의 `kube_pod_labels` 중 `kube_pod_status_phase{phase="Running"}`이 1인 Pod를 작업별로 묶고 세대는 가장 큰 값 | 신호 발생 시각 = 마지막 샘플(직전 5분) | 조회 실패 → `job_id` 없는 사건 1건(컨트롤러 HOLD). 0개 → 사건 없음(`outcome="no_jobs"`). 단 그 시각에 그 클러스터 KSM(`up{job="kube-state-metrics"}`)이 1이 아니었으면 0개가 아니라 「모름」 → 조회 실패와 같다 |
| node_lost의 작업 | site와 같은 식에 그 노드(`kube_pod_info{node}`)로 거른다 | 신호 발생 시각 = 마지막 Ready 샘플(직전 5분) | 작업 1개 → 그 작업의 사건. 0개 → 사건 없음(예비 노드 등, KSM이 그 시각에 1이었을 때만). 조회 실패 · 작업 여럿 → `job_id` 없는 1건(컨트롤러 HOLD) |

## 2. 알람 → 분류기 라벨 계약

분류기로 보낼 알람 룰은 아래를 지킨다. Alertmanager는 `pipeline="failover"` 라벨이 붙은 알람만 분류기 웹훅으로 보낸다.

| 종류 | 이름 | 필수 | 뜻 |
|---|---|---|---|
| 라벨 | `pipeline` | 모든 알람 | 값 `failover` |
| 라벨 | `signal` | 모든 알람 | `xid` · `pod_exit` · `job_failed`(이벤트 `source: pod`) · `spot` · `site` · `node_lost`(이벤트 `source: node`) · `straggler` |
| 라벨 | `source` | `xid`만 | `dmesg`(커널 로그 경로) 또는 `dcgm`(dcgm-exporter 지표 경로: 필드 230 마지막 번호 + Xid 번호별 횟수). 나머지 신호는 `signal`에서 정해진다 |
| 라벨 | `cluster` | 모든 알람 | 예 `eks-a` |
| 라벨 | `node` | `site` · `job_failed` 제외 | 노드 이름 |
| 라벨 | `gpu_uuid` | `xid`(알 수 있을 때) | `GPU-…`. 커널 로그 경로는 PCI 주소만 알므로 비워 두면 분류기가 dcgm-exporter 라벨로 찾는다 |
| 라벨 | `xid` | `xid` | 번호 |
| 라벨 | `pod` · `reason` | `pod_exit` | 학습 컨테이너(`trainer`)가 끝난 Pod와 종료 사유(`Error` · `OOMKilled` …) |
| 라벨 | `job_id` · `generation` | `job_failed` · `straggler` | `job_failed`: 실패한 학습 작업과 세대(관리 클러스터 JobSet 라벨 `devoops/job-id` · `devoops/generation`). `straggler`: 느린 랭크의 작업과 세대(trainer 지표 라벨). 이 두 신호만 알람이 작업을 직접 준다 |
| 라벨 | `exit_code` | `pod_exit`(알 수 있을 때) | 종료 코드. kube-state-metrics는 재시작하지 않는 컨테이너의 종료 코드를 내지 않아 지금 룰은 비워 보낸다(2026-11-18 kind 확인). 종료 원인은 코드가 아니라 사유와 `job.lifecycle`로 가린다(ADR-43) |
| annotation | `event_ts` | 모든 알람 | **원본 신호 발생 시각**(UTC). 룰 평가 시각이 아니다 |
| annotation | `log_line` | 있으면 | 근거가 된 로그 한 줄 |
| annotation | `ratio` | `straggler` | 그 랭크의 GPU 계산 시간 ÷ 랭크 중앙값(예 `1.60`). 사건의 `raw.value`로 나간다 |

- 필수 값이 빠진 알람은 이벤트를 만들지 않고 카운터 `classifier_signals_rejected_total`만 올린다.
- 분류기는 env `ENABLED_SIGNALS`에 적힌 종류만 받는다(기본 `xid,pod_exit`). 그 밖의 `signal`은 계약에 맞아도 이벤트를 만들지 않는다(`outcome="disabled"`). Alertmanager 라우팅이 틀리거나 누가 알람을 직접 넣어도 `site`의 `fence_evacuate`가 나가지 않게 하는 두 번째 잠금이다. `site` · `spot` · `node_lost` · `job_failed` · `straggler`를 켤 때는 라우트와 이 값을 같이 바꾼다(`node_lost` 라우트는 2026-11-24에 미리 열어 둠).
- 룰은 `deploy/base/mimir/rules/failover.yml`에 있다(2026-11-18): `FailoverXidDcgm`(`xid` · `dcgm`) · `FailoverPodExit`(`pod_exit`) · `FailoverSiteDown`(`site`) · `FailoverGpuNodeLost`(`node_lost`, 2026-11-24: GPU 노드만 사라지고 클러스터 지표는 계속 올 때) · `FailoverJobFailed`(`job_failed`, 2026-11-25: 관리 클러스터 JobSet이 실패로 끝남) · `FailoverStraggler`(`straggler`, 2026-11-26 GPU-296: 한 랭크의 GPU 계산 시간이 랭크 중앙값의 1.5배를 10분째 넘음). 커널 로그 경로 Xid와 `spot`은 아직 없다(8절).
- 룰은 사건 직후 1~3분만 울리고(사이트는 8분), 그동안 `event_ts`가 바뀌지 않게 쓴다. **`straggler`만 예외**로 느린 동안 계속 울린다: `event_ts`는 느려지기 시작한 시각(알람이 pending이 된 시각, ruler의 `ALERTS_FOR_STATE`)으로 고정하고, 분류기가 작업 · 세대 · 노드마다 1건만 낸다(3.3절). `event_ts`가 평가할 때마다 밀리면 중복 제거 창(±45초)을 벗어나 같은 고장이 여러 번 나간다.
- `ckpt-init` 종료는 `pod_exit`로 보내지 않는다. 복구 컨트롤러가 `job.init_failed`를 직접 구독한다.

## 3. 판정 표 (초안, 값은 전부 가설)

위에서부터 먼저 맞는 행을 쓴다. `confidence`는 자리 표시이고 5주차 주입 실험 뒤 고친다.

### 3.1 공통 규칙 (표보다 먼저)

| 조건 | 결과 | 근거 |
|---|---|---|
| Xid 45 · 174 | 이벤트 없음(버림) | 45 = 앞선 오류의 뒤처리, 174 = 프로세스가 죽었다는 정보(카탈로그 `IGNORE`). 둘 다 컨트롤러 조치(Pod 종료)가 만드는 Xid라 사건을 내면 루프가 된다 ([worker-agents.md](worker-agents.md)) |
| 종료 사유 `Completed`, 종료 코드 0(코드가 있을 때) | 이벤트 없음 | 정상 완료. 펜싱 종료는 아래 「의도된 종료」로 걸러진다(ADR-43, 옛 종료 코드 75는 11/25 GPU-223에서 무시 목록에서 뺌) |
| 의도된 종료(4절) | 이벤트 없음 | 김현수 답 2026-11-18 (GPU-120) |
| `collector_healthy=false`이고 site가 아님 | 표의 `category`는 그대로, `suggested_action=hold` · `confidence` 0.5 | 스키마 강제 규칙. 예시 08 |

### 3.2 Xid

| Xid | category | suggested_action | confidence | 근거 |
|---|---|---|---|---|
| 13 · 31 · 43 · 68 · 109 | `app` | `restart_in_place` | 0.8 | NVIDIA device plugin이 「앱 오류」로 보고 GPU 건강 검사에서 빼는 목록. 예시 03 |
| 94 | `hw` | `restart_in_place` | 0.8 | 격리된 메모리 오류: 그 앱만 영향, 다른 앱은 그대로. 카탈로그 즉시 조치 `RESTART_APP`(GPU 리셋은 편할 때). 오류 격리는 A100부터라 **T4에선 안 나옴** |
| 11 · 25 · 32 · 69 · 83 | `app` | `restart_in_place` | 0.7 | 푸시 버퍼 오류(11 · 25) · PBDMA 오류(32) · 그래픽 클래스 오류(69) · NVDEC1(83, 68과 같은 종류). 카탈로그 즉시 조치 `RESTART_APP`. 32는 카탈로그 「XID Parsing Policy」가 31과 함께 User-App으로 분류 |
| 8 · 60 · 70 · 71 · 72 · 75 · 76 · 77 · 85 · 183 | `driver` | `restart_in_place` | 0.6 | GPU 처리 멈춤(8) · SEC2(60) · 복사 엔진 CE3~CE9 · GSP 메모리 부족(183). 카탈로그 `RESTART_APP`. 앱 탓이라 단정할 근거가 없어 `driver` · 낮은 confidence |
| 48 | `hw` | `reset_then_migrate` | 0.9 | 고칠 수 없는 2비트 ECC 오류. 예시 02 |
| 64 · 95 · 140 · 175 | `hw` | `reset_then_migrate` | 0.85 | 64 = 메모리 페이지 폐기(T4) · 행 리매핑(Ampere 이후) 기록 실패, 95 = 격리 안 된 메모리 오류(여러 앱 영향, A100부터라 **T4에선 안 나옴**), 140 = 복구 못 한 ECC, 175 = 메모리 부분 정지. 모두 카탈로그 즉시 조치 `RESET_GPU` |
| 62 · 158 | `hw` | `reset_then_migrate` | 0.7 | PMU(내부 마이크로컨트롤러) 정지 · GPU 치명 타임아웃. 카탈로그 `RESET_GPU` |
| 119 · 120 · 184 | `driver` | `reset_then_migrate` | 0.5 | GSP RPC 타임아웃 · GSP 오류(119 짝) · BAD_TSG(리셋 전까지 새 GPU 컨텍스트 못 만듦). 카탈로그 `RESET_GPU`, 119 · 120 조사 조치 `REPORT_ISSUE`. 원인 후보가 HW · 드라이버 · 메모리 · 버스 · 열 · FB로 넓어 confidence를 낮게. 119가 3번 연속이면 드라이버가 Xid 154를 함께 낸다 |
| 79 | `hw` | `isolate_migrate` | 0.95 | GPU가 버스에서 떨어짐. 예시 01 |
| 63 · 92 | `hw_warn` | `none` | 0.9 | 페이지 폐기 기록 · 1비트 오류 빈발. 전조라 기록만 (fault-event.md 3절, MON-STD-03) |
| 37 · 38 · 44 · 66 · 67 · 154 | `driver` | `none` | 0.9 | 드라이버 펌웨어 오류(카탈로그 `IGNORE`) · 154 = 드라이버가 복구 조치 힌트를 바꿈. 기록만. 154는 다른 Xid와 함께 나고 조치는 그 Xid가 정한다. 힌트 문구(리셋 · 재부팅 등)는 dcgm 경로로 안 와서 커널 로그 경로(GPU-218)가 생기면 다시 본다 |
| 171 · 172 | `hw` | `none` | 0.9 | 48의 보조 정보(DRAM · SRAM 구분). 조치는 48이 한다 |
| 그 밖 | `hw` | `hold` | 0.3 | 표에 없는 번호는 사람이 본다. 5절 |

64 · 94 · 95 · 119 근거(11/25, GPU-225): NVIDIA Xid 카탈로그 `Xid-Catalog.xlsx`(https://docs.nvidia.com/deploy/xid-errors/analyzing-xid-catalog.html 에서 받음)의 XIDs 시트 「Tenant In-Operation Action」(즉시 조치) 열과 Guidance Classes 시트 정의, 그리고 옛 판 원문(https://docs.nvidia.com/deploy/xid-errors/archive/index.html 4.7 · 4.11 · 4.13절). `reset_then_migrate`는 컨트롤러에서 「격리 + 예비로 이전, GPU 리셋은 나중에 사람이」다(fault-event.md 3.1). 우리 GPU = Tesla T4 · 드라이버 580.178.04(11/25 dcgm 라벨).

나머지 번호(11/25, GPU-225 2차, 사용자 결정 「T4에서 날 수 있으면 웬만하면 다」): 카탈로그에는 T4 열이 없어서 **A100 열이 YES이고 T4에 없는 기능에 붙은 번호가 아닌 것**을 넣었다. 실제로 T4에서 안 나는 번호가 섞여도 판정이 안 쓰일 뿐 해는 없다. 즉시 조치 → 우리 조치는 `RESET_GPU` → `reset_then_migrate`, `RESTART_APP` → `restart_in_place`, `IGNORE` → `none`(기록만).

- **뺀 번호**(표에 없음 = `hold`): A100 열이 NO인 번호(Hopper 이후) · 46(Windows 전용) · 78(vGPU) · 82 · 86 · 84 · 88 · 89(T4에 없는 NVJPG · OFA · NVDEC2~4) · 106 · 107(SMBPBI 시험 메시지) · 108 · 137(NVLink) · 178(오류 격리 계열, A100부터) · 168(ECC가 꺼졌을 때만)
- **카탈로그와 다르게 둔 기존 행**: 43 = 카탈로그 `IGNORE`인데 우리는 `restart_in_place`(device plugin 앱 오류 목록, 그 앱은 이미 죽음) · 109 = 카탈로그 `XID_154_EVAL` · 79 = `CONTACT_SUPPORT`(`isolate_migrate`가 같은 뜻) · 63 = `RESET_GPU_DEFERRED`(작업이 끝난 뒤 리셋) → 학습을 멈출 일이 아니라 `none` 유지

### 3.3 Xid가 아닌 신호

| signal | 조건 | category | suggested_action | confidence | 근거 |
|---|---|---|---|---|---|
| `spot` | 회수 예고 | `infra` | `evacuate_preemptive` | 1.0 | ADR-36. 예시 05 |
| `site` | 클러스터 수집 끊김 | `site` | `fence_evacuate` | 0.7 | **그때 돌던 작업마다 1건**(컨트롤러가 사건 1건 = 작업 1개로 대피). `job.lifecycle`로 펜싱 · 중단 · 옛 세대인 작업은 뺀다(못 읽으면 작업마다 `hold`). VPN 단절과 구분은 컨트롤러가 한다(fault-event.md 4절). 예시 06. `node` · `gpu_uuid`는 null |
| `node_lost` | GPU 노드가 NotReady · 삭제(클러스터 지표는 계속 옴) | `infra` | `isolate_migrate` | 0.8 | **노드마다 1건, 그 노드에서 돌던 작업만**. 같은 작업의 다른 노드도 사라지면 노드마다 나가고 둘째 건부터는 컨트롤러가 이미 복구 중이라 닫는다. 자리가 없으면 디스패처가 다른 클러스터로(ADR-38). `job.lifecycle`로 펜싱 · 중단 · 옛 세대인 작업은 뺀다(못 읽으면 `hold`). 예시 10 |
| `straggler` | 한 랭크가 10분째 느리고, 옮기는 편이 이득 | `straggler` | `evacuate_preemptive` | 0.75 | ADR-37 · GPU-296. **작업 · 세대 · 노드마다 1건**(알람은 느린 동안 1분마다 다시 오지만 이득을 계산해 낸 뒤에는 남은 스텝이 줄어 판정이 달라져도 다시 내지 않는다 — 옮기는 중에 `hold`로 뒤집히면 안 되고, 이득은 처음 판정할 때가 가장 크다). 조건(`rules.yaml` `straggler`): 지속 = 지금 − `event_ts` ≥ 600초 **이고** 손해 = 남은 스텝 × (느린 step 시간 − 정상 step 시간) ≥ 2 × C(이전 비용, 실측 전 900초). `straggler_evidence` = `duration_seconds` · `cause_node`(= 사건 `node`) · `estimated_gain_seconds`(손해 − C). 남은 스텝 = `job.progress`의 `total_steps − step`, step 시간은 1절 표. 펜싱 · 중단 · 옛 세대는 버린다. 옮길 자리 · 작업당 1회 · 정책 스위치는 컨트롤러가 본다(지금 스위치는 꺼져 있어 전부 HOLD). 예시 07 |
| `straggler` | 느리지만 위 조건 미달 | `straggler` | `hold` | 0.75 | 증거는 그대로 싣는다(HOLD 이유 확인용). 예시 09 |
| `straggler` | 이득을 계산할 수 없음(그 세대의 `job.progress`를 못 봄 · 시작부터 느려 정상 step 시간이 없음 · Mimir 조회 실패) | `straggler` | `hold` | 0.75 | `straggler_evidence` 없이 낸다(스키마가 세 값을 모두 요구한다). 이유는 분류기 로그 `straggler.gain_unknown`. **이 `hold`는 고정하지 않는다**: 알람이 다시 올 때(1분마다) 다시 계산하고, 판정이 달라지면 같은 `dedup_key`의 `fault.reclassified`로 낸다(같으면 중복 제거가 거른다. 1시간 넘게 모르는 상태가 이어지면 `hold`가 한 번 더 나간다). 알람의 `ratio`가 유한한 수가 아니면(중앙값 0 → `+Inf`) 알람을 거부한다. 수집 경로가 죽었거나 `job.lifecycle`을 못 읽으면 3.1절대로 `hold`(0.5) |
| `pod_exit` | 종료 사유가 `Completed`가 아님, 의도된 종료 아님 | `app` | `restart_in_place` | 0.6 | 반복이어도 그대로 보낸다. 횟수는 컨트롤러만 센다(3회, 4번째는 `REJECTED`) |
| `pod_exit` | 종료 사유가 `OOMKilled` (컨테이너 메모리 한도) | `app` | `refuse` | 0.9 | 한도는 제출본 값이라 다시 띄워도 같은 자리에서 죽는다 → 재시도 없이 실패 확정(한송이 합의 2026-11-26, GPU-222). 컨트롤러 `NEEDS_CONFIG_FIX` |
| `pod_exit` · `job_failed` | 그 작업 · 세대 trainer 로그에 `CUDA out of memory` (GPU 메모리, 종료 사유는 `Error`) | `app` | `refuse` | 0.9 | GPU는 전부 T4 16GB라 옮겨도 같다. **Pod 하나가 아니라 작업 전체 로그**를 본다(Pod 종료 사건은 랭크마다 나가는데 한 랭크만 메모리 부족이면 판정이 갈린다). `raw.log_line` = 그 줄. 줄이 없거나 조회가 실패하면 위 행대로. 수집 경로가 죽었거나 `job.lifecycle`을 못 읽으면 `hold`가 우선 |
| `job_failed` | 관리 클러스터 JobSet이 실패로 끝남, 의도된 종료 아님 | `app` | `restart_in_place` | 0.6 | **작업 · 세대마다 1건.** `pod_exit`와 같은 일을 다른 곳에서 본다: JobSet이 Failed가 되면 MultiKueue가 EKS 쪽 JobSet을 바로 지워 죽은 Pod가 약 14초만 보이고, 수집 15초에 한 번도 안 잡히면 `pod_exit`가 안 온다(11/25 주입에서 워커 `kill -9`를 놓침). 관리 클러스터 JobSet은 Failed로 남아 놓치지 않는다. `node`는 trainer 로그에서 찾는다(2026-11-26 GPU-288, 1절 표): 랭크마다 Pod가 따로 `torchrun`을 돌려 **죽은 Pod의 로그에만** `failed (exitcode: N)` 줄이 남는다. 여러 Pod에 줄이 있으면(전 랭크가 코드 오류로 같이 죽음) 가장 먼저 찍힌 줄의 노드다 — 「먼저 죽은 노드」이지 「원인 노드」라는 뜻은 아니다. 못 찾으면 null. `raw.exit_code`는 채우지 않는다(로그의 `-9`는 시그널 번호라 컨테이너 종료 코드 137과 뜻이 다르다). 같은 (job_id, generation)에 `pod_exit` 사건과 겹치면 먼저 낸 쪽만 사건 |

## 4. 의도된 종료 거르기

펜싱 · 사용자 중단으로 지워진 Pod도 종료 코드를 남긴다. 펜싱 STOP을 뜻하던 종료 코드 75는 없어졌고(ADR-43, trainer GPU-244), 남는 코드는 1 · 143 · 137이라 숫자로는 장애와 구분할 수 없다. 그래서 Kafka `job.lifecycle`로 구분한다.

- 분류기는 `job.lifecycle`을 직접 읽어 두 가지를 기억한다.
  - (job_id, generation)별로 `job.fenced` · `job.cancelled`가 왔는가
  - 작업별 최신 세대(`job.submitted` · `job.resumed` 등에서 본 가장 큰 generation)
- **`job.failed`는 의도된 종료로 보지 않는다.** 장애로 JobSet이 먼저 Failed가 되면 그 세대의 끝 사건은 `job.failed` 하나뿐이라, 이것을 거르면 진짜 장애가 가려진다.
- `pod_exit` · `job_failed` 신호는 **신호 발생 시각(`event_ts`) + 30초(임시값)** 까지 기다렸다가 판정한다. 알람이 이미 30초 넘게 늦게 왔으면 바로 판정한다. 기다리는 동안 웹훅 응답은 `deferred`이고, 1초마다 도는 타이머가 기한이 된 신호를 판정한다.

기한이 되면 위에서부터 본다.

| 확인 | 결과 |
|---|---|
| `job_id` · `generation`을 못 찾음 | `hold`로 낸다 (어느 작업인지 몰라 확인할 수 없음) |
| 같은 (job_id, generation)의 `job.fenced` · `job.cancelled`를 봄 | 버림 (`intended`) |
| 신호의 generation < 그 작업의 최신 세대 | 버림 (`stale_generation`, 옛 세대의 뒤늦은 종료) |
| 같은 (job_id, generation)에 원인 사건을 이미 냄 | 버림 (`absorbed`) |
| 같은 (job_id, generation)에 다른 종류의 종료 사건(`pod_exit` ↔ `job_failed`)을 이미 냄 | 버림 (`absorbed`). 같은 종류끼리는 그대로다(여러 랭크의 `pod_exit`는 Pod마다 나간다) |
| `job.lifecycle` 읽기가 끊겨 있음 | `hold`로 낸다 (확인할 수 없음) |
| 위에 해당 없음 | 3.3절 `pod_exit`(또는 `job_failed`) 행으로 낸다 |

**원인 사건에 묶기.** 한 고장으로 다른 랭크가 줄줄이 죽을 때 `restart_in_place`가 따로 나가지 않게 한다.

- 원인 사건 = `xid` · `spot` · `site` · `node_lost` 신호로 낸 이벤트 중 action이 `hold` · `none`이 아닌 것. Xid 63처럼 기록만 하는 사건은 원인으로 치지 않는다(그 뒤의 Pod 종료는 따로 판정해야 함).
- 원인 사건을 낼 때 같은 (job_id, generation)의 `pod_exit`가 기다리고 있으면 그 이벤트의 `correlated`에 넣고 대기에서 뺀다.
- 원인 사건을 낸 뒤에 오는 같은 (job_id, generation)의 `pod_exit` · `job_failed`도 버린다. 그 세대는 이미 복구 대상이라 새 사건이 아니다.
- `site` 사건도 작업마다 원인 사건으로 기억한다. 다만 클러스터가 안 보이는 상황이라 그 클러스터의 `pod_exit` 알람은 보통 오지 않는다.

**알아 둘 것**

- 감시기는 JobSet 삭제를 보는 순간 발행하므로 보통 Pod 종료보다 먼저 온다. 감시기가 재연결 중이면 늦을 수 있어 대기 시간을 둔다.
- 30초는 분류기 · 복구 컨트롤러를 연결한 E2E에서 실제 간격을 재서 고친다.
- 기다리는 신호는 메모리에만 있다. 그 사이 분류기가 재시작하거나 대장이 바뀌면 사라지고, 알람이 아직 켜져 있어 Alertmanager가 다시 보내야 판정된다.

## 5. 표에 없는 신호

`rules.py`가 맞추지 못한 신호는 함수 `unmatched()` 하나로 모인다. 지금은 `hold` 이벤트를 내고 카운터 `classifier_unmatched_total`과 로그 줄을 남긴다.
모델(형식 없는 로그 줄 · 신호 충돌 판정)을 붙일 때는 이 함수만 바꾼다. 그때도 조건은 같다: 룰 우선, 저확신은 `hold`, 되돌릴 수 있는 조치만, 스키마 검증을 통과한 것만 발행.

## 6. 발행

- **검증 먼저.** v1 스키마를 통과하지 못한 이벤트는 발행하지 않고 로그와 카운터 `classifier_events_invalid_total`만 남긴다. 복구 컨트롤러는 형식이 깨진 메시지를 받으면 멈춘다.
- **봉투.** 수집기 · 감시기와 같은 형식(Data 레포 `collector/collector/envelope.py`)이다. 컨트롤러는 `payload`를 읽는다.

| 봉투 필드 | 값 |
|---|---|
| `event_id` | payload의 `event_id`와 같은 값 (= trace-id) |
| `schema_version` | 정수 `1` (payload 안의 `schema_version`은 문자열 `"1"`) |
| `type` | `fault.reclassified`(같은 `dedup_key`로 action이 바뀜. `hold`로 바뀐 경우도 이것) · `fault.held`(처음부터 `hold`) · `fault.detected`(그 밖) |
| `occurred_at` | 분류기 판정 시각 (ADR-15) |
| `source` | `classifier` |
| `cluster` · `node` · `job_id` · `generation` | payload와 같은 값 |
| `payload` | v1 장애 이벤트 |

- 파티션 키는 `node`(site는 `cluster`), 헤더 `rule_version`은 `rules.yaml`의 git SHA 앞 7자.
- 프로듀서 설정은 감시기와 같다: `acks=all` · `enable.idempotence=true`.

## 7. 상태와 재시작

- 중복 제거 창과 의도된 종료 표는 메모리에 둔다. **대장이 될 때 Kafka에서 다시 만든다**: `gpu.fault` 최근 1시간, `job.lifecycle` 최근 24시간. 컨슈머 그룹 없이 시각으로 위치를 잡아 읽으므로 수집기(`data-collector`)와 무관하다.
- `gpu.fault`는 대장이 될 때 한 번만 읽는다(처리하는 쪽은 대장 하나라 그 뒤로는 내가 낸 것을 내가 기억한다). `job.lifecycle`은 따라잡은 뒤 계속 이어 읽고, 5초마다 브로커에 물어 읽을 수 있는 상태인지 확인한다. 못 읽는 동안의 `pod_exit`는 `hold`로 나간다(4절).
- 복원이 끝나야 웹훅을 처리한다(그 전에는 503). 복원에 실패하면(브로커 접속 불가 · 30초 안에 못 따라잡음) 프로세스를 끝내 쿠버네티스가 다시 띄우게 두고, 그동안 다른 쪽이 대장을 잡는다. 기억 없이 받으면 같은 사건을 또 내기 때문이다.
- 오프셋을 커밋하지 않는다. 구현: `classifier/classifier/kafka_reader.py`.
- 이유: 컨트롤러는 `event_id`로만 중복을 거른다. 분류기가 재시작한 뒤 Alertmanager가 같은 알람을 다시 보내면, 창을 잊은 분류기는 같은 사건을 새 이벤트로 낸다.
- 복제본은 2개이고 처리는 대장 하나만 한다(7.1절). 둘이 각자 처리하면 중복 제거 기억이 따로라 같은 사건이 두 번 나간다.
- 토픽 보존 기간(브로커 기본 7일, 실제 값은 김현수 확인 중)이 24시간보다 짧으면 복원 범위를 줄인다.

### 7.1 대장 선출 (2026-11-23, GPU-227)

Pod 2개가 서로 다른 노드에 뜨고, 쿠버네티스 Lease `classifier`를 잡은 쪽(대장)만 알람을 처리한다. 구현 `classifier/classifier/lease.py`, 규칙은 client-go의 leader election과 같다.

| 항목 | 값 | 뜻 |
|---|---|---|
| 갱신 주기 | 2초 | 대장이 Lease의 `renewTime`을 고쳐 쓴다 |
| 내려놓는 기한 | 10초 | 대장이 이 시간 동안 한 번도 못 쓰면 프로세스를 끝낸다 |
| 만료 | 15초 | 대기 쪽은 Lease 내용이 이 시간 동안 그대로면 가져온다. 내려놓는 기한보다 길어서 옛 대장이 먼저 멈춘다 |

- **대기 쪽도 Ready다.** Alertmanager 웹훅은 Service를 거쳐 두 Pod에 번갈아 오고, 대기 쪽은 받은 요청을 대장에게 그대로 넘겨 대장의 응답을 돌려준다. 대장 주소는 대장이 Lease 어노테이션 `devoops/leader-address`에 적는다. 대장에 못 닿거나 대장이 없으면 503 → Alertmanager가 다시 보낸다
  - 대기 쪽을 Ready 0으로 두지 않은 이유: Ready가 아닌 Pod가 늘 있으면 Deployment 교체가 멈추고 Argo CD가 비정상으로 본다
  - 넘길 주소는 「IP:8080」 형식만 받고(루프백 · 링크 로컬 제외) 리다이렉트는 따라가지 않는다. 넘겨받은 요청은 다시 넘기지 않는다
- **대장이 죽으면** 만료(15초) 뒤 대기 쪽이 Lease를 잡고 위 복원을 한 다음 처리를 시작한다. kind 실측 16~19초(대장 프로세스를 얼림, `scripts/classifier-e2e-kind.sh` 3번 장면). 그 사이 알람은 Alertmanager 재시도와 `repeat_interval` 1분으로 다시 온다
- **넘겨받을 때 잃는 것**은 옛 대장이 들고 있던 「Pod 종료 대기(30초)」 신호뿐이다. 알람이 켜져 있는 동안 다시 와서 판정된다
- **정상 종료(버전 올리기)** 하는 대장은 Lease를 비우고 나간다 → 대기 쪽이 만료를 기다리지 않고 다음 시도(2초)에 잡는다. 교체는 하나씩 한다(`maxSurge 0 · maxUnavailable 1`)
- **멈췄다 돌아온 옛 대장**은 Lease가 남의 것이 된 것을 보고 프로세스를 끝낸다. 기억을 버려야 하기 때문이다(그 사이 새 대장이 낸 것을 모른다)
- Lease는 매니페스트에 없다. Argo CD가 Lease를 관리 대상에서 빼므로 분류기가 직접 만든다(Role `classifier`에 create)
- 지표 `classifier_leader`: 1 = 대장이고 복원이 끝남. 알람 `ClassifierDown`은 이 값이 1인 Pod가 없을 때 운다. 대기 쪽의 `classifier_lifecycle_healthy`는 늘 0이다(읽지 않는다)
- `LEASE_NAME`이 없으면(로컬 실행) 선출 없이 혼자 돌고 시작할 때 복원한다

## 8. 확인 필요

- [x] (나) Xid 64 · 94 · 95 · 119를 NVIDIA 문서 원문과 대조해 3.2절에 추가 (11/25, GPU-225)
- [x] (나) 카탈로그가 바뀐 번호 검토(11/25, GPU-225): 63 `RESET_GPU_DEFERRED`(작업 뒤 리셋) → `none` 유지, 79 `CONTACT_SUPPORT` → `isolate_migrate` 그대로 맞음. 같은 날 T4에서 날 수 있는 나머지 Xid 29개 추가(3.2절)
- [x] dcgm-exporter 지표의 라벨 이름(`node` · `UUID`) 확인 — 11/19 EKS(T4 · 4.8.4)에서 둘 다 그대로 나온다. 수정 없음. (원래 걱정: 다르면 커널 로그 경로와 DCGM 경로의 `dedup_key` 대상이 달라져 같은 Xid가 두 건으로 나간다)
- [x] (나, EKS overlay) KSM `--metric-labels-allowlist`에 `nodes=[topology.kubernetes.io/zone]` 추가 — `deploy/agents/overlays/eks`(11/18). kind에서 `az`가 채워지는 것 확인. EKS가 아닌 클러스터는 `az`가 `unknown`
- [x] 분류기용 Alertmanager 라우트(`pipeline=failover`)는 `group_wait` 0초 · `repeat_interval` 1분 — `deploy/values/alertmanager.yaml`(11/18). 기본 4시간이면 분류기가 놓친 알람(재시작으로 사라진 대기 신호 · Kafka 전달 실패)이 다시 오지 않는다. 다시 온 알람은 중복 제거로 걸러진다
- [x] 분류기 실 클러스터 배포와 웹훅 연결(11/18): Gitops `monitoring/onprem/classifier`, Alertmanager 수신자 `classifier`. Xid · Pod 종료만 보낸다
- [x] (나) 사이트 알람을 분류기로 보내기(11/25, GPU-220): 펜싱된 EKS를 룰에서 빼게 된 뒤 AM 라우트 `xid|pod_exit|node_lost|site` + 분류기 `ENABLED_SIGNALS`에 site(Gitops #109)
- [ ] (나, EKS 테스트 뒤) 커널 로그 경로 Xid 룰. Loki에 룰 파일을 넣는 길(ConfigMap 마운트)부터 만들어야 한다. `dcgmi` 주입은 커널 로그를 남기지 않으므로 진짜 Xid 로그 줄(`NVRM: Xid`)이 생기면 원문을 보관해 그것에 맞춘다
- [ ] (박기현 · 김성모) 스팟 회수 예고가 어디로 나오는지(관리형 노드그룹). 정해지면 `spot` 룰을 쓴다
- [x] (나) `FailoverXidDcgm`이 같은 번호 재발을 못 보던 것(11/24, GPU-221): `DCGM_EXP_XID_ERRORS_COUNT`(Xid 번호별 최근 10분 횟수, 11/23 EKS 주입에서 값 확인)가 1분 전보다 늘었거나 1분 전엔 없었으면 울린다. 창은 dcgm-exporter env로 5분 → 10분. 번호 경로는 횟수 지표가 없을 때를 위해 남겼다(같은 GPU면 알람 하나). **한계**: 앞 Xid가 창에서 빠진 뒤 수집 주기(15초) 안에 같은 번호가 다시 나면(간격 10분 ~ 10분 15초) 빈 순간이 기록되지 않아 못 본다. 수집 주기를 줄여도 exporter 계산 주기(5초)가 남아 0은 안 된다. 지금 Xid 신호는 이 dcgm 경로 하나뿐이고, 빈틈 없이 막는 것은 커널 로그 경로(위 항목, GPU-218 — 줄마다 사건 하나)다
- [ ] (나 · 한송이) GPU-221 실측: EKS 학습 중 `dcgmi`로 같은 Xid를 5분 간격 2번 주입 → `gpu.fault` 2건(deploy/README.md 「장애 주입」 ③)
- [x] (나) `FailoverSiteDown`이 일부러 내린 클러스터에도 울리던 것(11/24, GPU-220): EKS OFF 절차가 붙이는 MultiKueueCluster `devoops/fenced=true`를 관리 클러스터 KSM이 지표로 내고(`deploy/agents/overlays/onprem/ksm-custom-resources.yaml`), 룰이 펜싱된 클러스터를 뺀다. 대상도 `eks-(a|b|c)`로 좁혔다. Agent 하나만 죽은 경우는 여전히 울린다
- [x] (나) `FailoverSiteDown`이 온프레 회선이 끊길 때도 울리던 것(11/24 15:30 pve4 Wi-Fi 작업, eks-a · b 동시 발화, GPU-220): 모니터링 클러스터 → 1.1.1.1 ping(`probe_success{target_name="internet"}`)이 최근 10분 안에 실패한 적 있으면 울리지 않는다. 온프레 인터넷 · EKS VPN이 같은 회선을 지나기 때문. 그동안 난 진짜 사이트 장애는 못 본다
- [x] (나 · 한송이) `OOMKilled`를 따로 판정할지 — 2026-11-26 확정 · 구현(GPU-222): 컨테이너 `OOMKilled` · trainer 로그의 `CUDA out of memory`는 `refuse`(3.3절). **남은 것**: `CUDA out of memory` 줄 원문과 `OOMKilled` 사유가 실제로 붙는지를 EKS 주입 ⑥으로 확인 · 컨테이너 OOM인데 Pod가 지워져 사유를 못 본 경우(trainer 로그엔 `exitcode: -9`뿐)는 `restart_in_place`로 나간다
- [x] `classifier_lifecycle_healthy == 0`이 이어지면 Slack으로 알리는 룰 — `ClassifierLifecycleUnhealthy`(2분), 분류기가 없거나 안 긁히면 `ClassifierDown`(3분). `deploy/base/mimir/rules/classifier.yml`(11/19, MON-STD-27). 0인 동안 `pod_exit`는 전부 `hold`이고, 메시지 반영 실패로 0이 되면 재시작 전까지 돌아오지 않는다
- [ ] (나, EKS 테스트) 스트래글러 실측(GPU-296): 학습 중 한 노드에 `nvidia-smi -lgc`로 클럭을 제한 → 10분 뒤 `FailoverStraggler` · 사건의 `straggler_evidence` 값 확인. 이전 비용 C(900초)는 가정값이라 실제 이전 시간을 재서 고친다. 알고 두는 한계: 랭크가 2개면 중앙값이 두 값의 평균이라 3배 넘게 느려야 1.5를 넘는다 · trainer 지표는 사용자 코드가 내는 값이다(영향은 자기 작업의 이전 1회) · 분류기가 사건을 낸 지 1시간 넘어 재시작하면(Kafka 복원 범위) 아직 느린 작업의 사건이 한 번 더 나갈 수 있다 · `ts`가 느려진 시각이라 감지 시간은 10분 이상으로 찍힌다
- [ ] (나 · 한송이) `pod_exit` 대기 30초를 E2E 실측으로 조정
- [ ] (김현수) `job.lifecycle` 토픽 보존 기간, rank별 종료 사유 `job.trainer_exited` 도입 여부
- [x] (나) 종료 코드 75 무시 삭제(11/25, GPU-223): trainer(GPU-244) · Backend jobspec에서 75가 없어져 판정 표 `ignore.exit_code`를 `[0]`으로, 3.1절 · fault-event.md 「이벤트를 만들지 않는 종료」도 고침
