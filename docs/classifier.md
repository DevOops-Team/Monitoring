# 분류기 설계 (GPU-192)

분류기는 Alertmanager가 보낸 원인 알람을 받아 「무슨 일이 났고 무엇을 추천하는가」를 판정하고, Kafka `gpu.fault`에 이벤트 한 건으로 낸다.
출력 형식은 [fault-event.md](fault-event.md)와 [`schemas/fault-event.v1.json`](../schemas/fault-event.v1.json)이 정본이다. 이 문서는 그 이벤트를 **어떻게 만드는가**를 적는다.

- 위치: 온프레 모니터링 클러스터(ADR-18 · ADR-29), 코드 `classifier/`(Python · FastAPI)
- 판정은 표(룰)로만 한다. 표에 없는 신호는 `hold`로 내고, 나중에 모델을 꽂을 자리 하나만 둔다(5절)
- 이번 범위 밖: 원인 알람 룰(Loki Xid 룰 · DCGM 룰), 스트래글러 판정, 실 클러스터 배포, Tempo span 전송, 2중화

## 1. 흐름

```
Alertmanager 웹훅 ─▶ signals ─▶ enrich ─▶ intended ─▶ rules ─▶ dedup ─▶ publish ─▶ Kafka gpu.fault
 POST /alerts        정규화     Mimir     거르기      표 판정   ±45초    검증+봉투
                                           ▲
                             Kafka job.lifecycle 구독 (의도된 종료 표 · 최신 세대)
```

| 모듈 | 하는 일 |
|---|---|
| `signals.py` | 웹훅의 알람 1건을 내부 신호로 바꾼다. `ts`는 annotation `event_ts`만 쓴다(수신 시각 금지). resolved 알람은 버린다 |
| `enrich.py` | Mimir 조회로 `job_id` · `generation`(`kube_pod_labels`) · `collector_healthy`(`up`) · `az`를 채운다. 조회가 실패하면 `collector_healthy=false` |
| `intended.py` | `job.lifecycle`을 읽어 「의도된 종료」 표와 작업별 최신 세대를 유지한다(4절) |
| `rules.py` · `rules.yaml` | 3절 표로 `category` · `suggested_action` · `confidence`를 정한다 |
| `dedup.py` | 같은 사건을 한 번만 낸다. 규칙은 fault-event.md 2.1절(±45초 창, 키와 action이 둘 다 같으면 재발행 안 함) |
| `event.py` | 신호 + 판정 + Mimir 값 → v1 장애 이벤트. `event_id`(= trace-id)는 판정마다 새로 만든다 |
| `publish.py` | v1 스키마 검증 → 봉투 → Kafka 발행(6절) |
| `app.py` | `POST /alerts` · `/healthz` · `/metrics` |

## 2. 알람 → 분류기 라벨 계약

분류기로 보낼 알람 룰은 아래를 지킨다. Alertmanager는 `pipeline="failover"` 라벨이 붙은 알람만 분류기 웹훅으로 보낸다.

| 종류 | 이름 | 필수 | 뜻 |
|---|---|---|---|
| 라벨 | `pipeline` | 모든 알람 | 값 `failover` |
| 라벨 | `signal` | 모든 알람 | `xid` · `pod_exit` · `spot` · `site` |
| 라벨 | `source` | `xid`만 | `dmesg`(커널 로그 경로) 또는 `dcgm`(필드 230 경로). 나머지 신호는 `signal`에서 정해진다 |
| 라벨 | `cluster` | 모든 알람 | 예 `eks-a` |
| 라벨 | `node` | `site` 제외 | 노드 이름 |
| 라벨 | `gpu_uuid` | `xid`(알 수 있을 때) | `GPU-…`. 커널 로그 경로는 PCI 주소만 알므로 비워 두면 분류기가 dcgm-exporter 라벨로 찾는다 |
| 라벨 | `xid` | `xid` | 번호 |
| 라벨 | `pod` · `exit_code` | `pod_exit` | 학습 컨테이너가 끝난 Pod와 종료 코드 |
| annotation | `event_ts` | 모든 알람 | **원본 신호 발생 시각**(UTC). 룰 평가 시각이 아니다 |
| annotation | `log_line` | 있으면 | 근거가 된 로그 한 줄 |

- 필수 값이 빠진 알람은 이벤트를 만들지 않고 카운터 `classifier_signals_rejected_total`만 올린다.
- `ckpt-init` 종료는 `pod_exit`로 보내지 않는다. 복구 컨트롤러가 `job.init_failed`를 직접 구독한다.

## 3. 판정 표 (초안, 값은 전부 가설)

위에서부터 먼저 맞는 행을 쓴다. `confidence`는 자리 표시이고 5주차 주입 실험 뒤 고친다.

### 3.1 공통 규칙 (표보다 먼저)

| 조건 | 결과 | 근거 |
|---|---|---|
| Xid 45 | 이벤트 없음(버림) | 앞선 오류의 뒤처리이고 컨트롤러 조치가 만드는 Xid라 루프가 된다 ([worker-agents.md](worker-agents.md)) |
| 종료 코드 0 · 75 | 이벤트 없음 | 정상 완료 · 펜싱 정상 동작 (fault-event.md 3절) |
| 의도된 종료(4절) | 이벤트 없음 | 김현수 답 2026-11-18 (GPU-120) |
| `collector_healthy=false`이고 site가 아님 | 표의 `category`는 그대로, `suggested_action=hold` · `confidence` 0.5 | 스키마 강제 규칙. 예시 08 |

### 3.2 Xid

| Xid | category | suggested_action | confidence | 근거 |
|---|---|---|---|---|
| 13 · 31 · 43 · 68 · 109 | `app` | `restart_in_place` | 0.8 | NVIDIA device plugin이 「앱 오류」로 보고 GPU 건강 검사에서 빼는 목록. 예시 03 |
| 48 | `hw` | `reset_then_migrate` | 0.9 | 고칠 수 없는 2비트 ECC 오류. 예시 02 |
| 79 | `hw` | `isolate_migrate` | 0.95 | GPU가 버스에서 떨어짐. 예시 01 |
| 63 · 92 | `hw_warn` | `none` | 0.9 | 페이지 폐기 기록 · 1비트 오류 빈발. 전조라 기록만 (fault-event.md 3절, MON-STD-03) |
| 그 밖 | `hw` | `hold` | 0.3 | 표에 없는 번호는 사람이 본다. 5절 |

추가 후보(NVIDIA Xid 문서 원문 대조 뒤): 64 · 94 · 95 · 119. 대조 전에는 「그 밖」 행으로 간다.

### 3.3 Xid가 아닌 신호

| signal | 조건 | category | suggested_action | confidence | 근거 |
|---|---|---|---|---|---|
| `spot` | 회수 예고 | `infra` | `evacuate_preemptive` | 1.0 | ADR-36. 예시 05 |
| `site` | 클러스터 수집 끊김 | `site` | `fence_evacuate` | 0.7 | VPN 단절과 구분은 컨트롤러가 한다(fault-event.md 4절). 예시 06. `node` · `gpu_uuid`는 null |
| `pod_exit` | 종료 코드가 0 · 75가 아님, 의도된 종료 아님 | `app` | `restart_in_place` | 0.6 | 같은 오류 반복은 컨트롤러가 `refuse`로 올린다 |

## 4. 의도된 종료 거르기

펜싱 · 사용자 중단으로 지워진 Pod도 종료 코드를 남긴다. 종료 코드 75가 없어지면 143 · 137이라 숫자로는 장애와 구분할 수 없다. 그래서 Kafka `job.lifecycle`로 구분한다.

- 분류기는 `job.lifecycle`을 직접 읽어 두 가지를 기억한다.
  - (job_id, generation)별로 `job.fenced` · `job.cancelled`가 왔는가
  - 작업별 최신 세대(`job.submitted` · `job.resumed` 등에서 본 가장 큰 generation)
- **`job.failed`는 의도된 종료로 보지 않는다.** 장애로 JobSet이 먼저 Failed가 되면 그 세대의 끝 사건은 `job.failed` 하나뿐이라, 이것을 거르면 진짜 장애가 가려진다.
- `pod_exit` 신호는 **30초(임시값)** 기다렸다가 판정한다.

| 30초 안에 본 것 | 결과 |
|---|---|
| 같은 (job_id, generation)의 `job.fenced` · `job.cancelled` | 버림 |
| 신호의 generation < 그 작업의 최신 세대 | 버림 (옛 세대의 뒤늦은 종료) |
| 같은 작업의 `xid` · `spot` · `site` 신호 | 따로 내지 않고 그 이벤트의 `correlated`에 넣는다. 한 고장으로 다른 랭크가 줄줄이 죽을 때 `restart_in_place`가 먼저 나가지 않게 한다 |
| `job.lifecycle` 읽기가 끊겨 있음 | `hold`로 낸다 (확인할 수 없음) |
| 위에 해당 없음 | 3.3절 `pod_exit` 행으로 낸다 |

- 감시기는 JobSet 삭제를 보는 순간 발행하므로 보통 Pod 종료보다 먼저 온다. 감시기가 재연결 중이면 늦을 수 있어 대기 시간을 둔다.
- 30초는 분류기 · 복구 컨트롤러를 연결한 E2E에서 실제 간격을 재서 고친다.

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

- 중복 제거 창과 의도된 종료 표는 메모리에 둔다. **시작할 때 Kafka에서 다시 만든다**: `gpu.fault` 최근 1시간, `job.lifecycle` 최근 24시간. 컨슈머 그룹 없이 시각으로 위치를 잡아 읽으므로 수집기(`data-collector`)와 무관하다.
- 이유: 컨트롤러는 `event_id`로만 중복을 거른다. 분류기가 재시작한 뒤 Alertmanager가 같은 알람을 다시 보내면, 창을 잊은 분류기는 같은 사건을 새 이벤트로 낸다.
- 복제본은 1개다. 2개면 웹훅이 번갈아 들어가 중복 제거가 깨진다. 2중화는 3중화 단계에서 다룬다.
- 토픽 보존 기간(브로커 기본 7일, 실제 값은 김현수 확인 중)이 24시간보다 짧으면 복원 범위를 줄인다.

## 8. 확인 필요

- [ ] (나) Xid 64 · 94 · 95 · 119를 NVIDIA 문서 원문과 대조해 3.2절에 추가
- [ ] (나 · 한송이) `pod_exit` 대기 30초를 E2E 실측으로 조정
- [ ] (김현수) `job.lifecycle` 토픽 보존 기간, rank별 종료 사유 `job.trainer_exited` 도입 여부
- [ ] (나) 종료 코드 75가 실제로 없어지면 3.1절과 fault-event.md 「이벤트를 만들지 않는 종료」를 같이 고침
