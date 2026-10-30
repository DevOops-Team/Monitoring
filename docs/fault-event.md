# 장애 이벤트 JSON v1 (경계 문서 2)

분류기(모니터링 VM)가 복구 컨트롤러(매니저 k8s)에 HTTP POST로 보내는 "사고 보고서 한 장".
컨트롤러는 PromQL · LogQL · Xid 번호를 몰라도 이 문서만 보고 움직일 수 있어야 한다.

- 스키마: [`schemas/fault-event.v1.json`](../schemas/fault-event.v1.json) (JSON Schema 2020-12)
- 예시: [`schemas/examples/`](../schemas/examples/) (장애 유형별 8개, 전부 스키마 통과)
- 생산자: 분류기 (이태림) · 소비자: 복구 컨트롤러 (한송이), DB 적재 (김현수)
- 변경: `schemas/` · 이 문서 변경 PR은 CODEOWNERS로 한송이 · 김현수가 리뷰어로 자동 지정되고, **둘 중 1명 승인 필수**. 형식 변경은 Slack에서 두 사람과 먼저 합의한 뒤 PR을 올린다

## 1. 역할 나눔

| | 분류기 (이벤트 JSON) | 복구 컨트롤러 |
|---|---|---|
| 아는 것 | 무슨 일이 났나, 원인이 뭔가 | 예비 GPU가 있나, 다른 클러스터가 살아 있나, 이미 복구 중인가 |
| 말하는 것 | `category` + `suggested_action` (**추천**) | 실제로 할 동작 (**결정**, `decide` span) |

분류기 룰이 바뀌어도(Xid 룰 수정, LLM 분류 추가) `suggested_action` 8개 값만 지키면 컨트롤러 코드는 그대로다.

## 2. 필드

모든 필드는 필수다. 해당 없는 값은 빼지 않고 `null`로 보낸다 (DB 컬럼이 항상 같도록).

| 필드 | 타입 | 뜻 |
|---|---|---|
| `schema_version` | `"1"` | 고정 |
| `event_id` | 32자 hex | 이벤트 ID. **`traceparent`의 trace-id와 같은 값** (스키마로는 못 막아서 분류기 테스트로 검증). 전부 0은 거부 |
| `traceparent` | W3C | trace-id · parent-id가 전부 0이면 W3C 무효 값이라 거부. 컨트롤러는 이걸 부모로 `decide` span을 연다 → 분류기 · 컨트롤러 · 학습 Job span이 한 trace로 이어짐 |
| `ts` | UTC 시각 | **원본 신호 발생 시각** (커널 로그 시각 등). 분류기가 받은 시각 아님 → 감지 지연 = 분류기 span 시작 − `ts` |
| `cluster` · `az` | 문자열 | 예: `worker-a`, `ap-northeast-2a` |
| `node` | 문자열 / null | site 이벤트는 null |
| `gpu_uuid` | `GPU-…` / null | dcgm-exporter 레이블(pci_bus_id → UUID)로 채움 |
| `job_id` | 문자열 / null | **학습 Job ID**. k8s Job 이름(`job-0421-gen3`)이 아니라 `job-0421` |
| `generation` | 정수 / null | 체크포인트 세대 번호. 원본은 S3 |
| `source` | enum | `dcgm` · `dmesg`(커널 로그, journald 경유) · `pod` · `straggler` · `site` · `spot` |
| `raw` | 객체 | 분류 근거 원본. `xid` · `exit_code` · `metric` · `value` · `log_line` (해당 없으면 null) |
| `correlated` | 배열 | 상관 창 안에서 함께 본 다른 알람 (`alert` · `source` · `ts`) |
| `collector_healthy` | bool | 분류 시점에 그 노드의 수집 경로(Agent · Alloy)가 살아 있었나 |
| `dedup_key` | 문자열 | 같은 사건 식별 키. `대상\|코드\|기준 시각(epoch 초)`. 규칙은 2.1절 |
| `category` | enum | `hw` · `hw_warn` · `driver` · `app` · `infra` · `site` · `network` · `straggler` |
| `confidence` | 0~1 | 분류 확신도. Xid 하나로 원인 확정 불가(NVIDIA 공식 입장)라서 둠 |
| `suggested_action` | enum | 아래 3절 8개 |

스키마가 강제하는 규칙 두 개:

- `collector_healthy: false`면 `suggested_action`은 `hold`만 허용 (site 제외: 사이트 장애면 수집 경로가 죽는 게 당연함)
- `category: site`면 `node` · `gpu_uuid`는 null
- `dedup_key`는 `대상|코드|10자리 epoch 초` 형식만 허용 (ISO 시각 문자열 금지: `…11Z`와 `…11.000Z`가 다른 키가 되는 것 방지)

### 2.1 중복 처리 규칙

같은 사건이 여러 번 올 수 있다. 경로가 둘(커널 로그 · DCGM)이고, Alloy 재시작이나 Alertmanager 재전송도 있다.

**키 만들기 (분류기)**

- 대상 = `gpu_uuid` (spot은 `node`, site는 `cluster`), 코드 = `raw.xid` (없으면 `source`)
- 같은 대상+코드 이벤트가 이미 본 사건의 기준 시각 **±45초** 안의 `ts`면 같은 사건 → 그 사건의 키를 그대로 쓴다
- 아니면 새 사건 → 이 이벤트의 `ts`(epoch 초)가 기준 시각
- 비교는 도착 시각이 아니라 `ts`(신호 발생 시각)로 한다. 로그 경로가 몇 시간 뒤 살아나 밀린 줄이 와도 같은 사건으로 묶인다

**창 45초 근거**: 같은 사건이 두 경로로 올 때 `ts` 차이의 최댓값 + 여유.

| 구간 | 최대 지연 |
|---|---|
| dcgm-exporter 수집 주기 (Xid 필드 230만 5초로 설정, 기본값 30초) | 5초 |
| Prometheus Agent 스크레이프 | 15초 |
| 룰 평가 주기 | 15초 |
| 합계 (커널 로그 경로는 원본 시각이라 0) | 35초 → 여유 10초 더해 **45초** |

위 주기를 바꾸면 창도 다시 계산한다. 5주차 Xid 주입 실험에서 두 경로의 실제 `ts` 차이를 재서 조정한다.
창을 너무 키우면 제자리 재시작 직후 같은 고장이 다시 났을 때 같은 사건으로 묶여 반복 횟수(`refuse` 승격)에서 빠진다.

**키 소비 (컨트롤러 · 분류기)**

- 컨트롤러는 **실제 조치를 실행했을 때만** 그 키를 "처리됨"으로 기록한다. 이후 같은 키 이벤트는 무시한다
- `hold` · `none`은 키를 소비하지 않는다 → 같은 키로 더 확실한 이벤트가 오면 처리된다 (예: 08 `hold` → 01 `isolate_migrate`)
- 분류기는 직전에 보낸 이벤트와 **키와 `suggested_action`이 둘 다 같을 때만** 다시 보내지 않는다. 키만 같고 action이 바뀌었으면 보낸다

## 3. suggested_action → 컨트롤러 동작

| suggested_action | 뜻 | 컨트롤러가 실제로 하는 일 |
|---|---|---|
| `restart_in_place` | GPU 멀쩡, 프로그램만 문제 | 같은 노드에서 Pod 재시작 → 체크포인트에서 이어 감 |
| `isolate_migrate` | 이 GPU 못 씀 | 노드 cordon → Pod 삭제 → 같은 클러스터 예비 GPU로 이전 |
| `reset_then_migrate` | 이 GPU 리셋 필요 | 먼저 다른 노드로 이전 → 빈 노드에서 GPU 리셋 → 성공하면 예비로 복귀, 실패하면 폐기 |
| `evacuate_preemptive` | 곧 죽거나 느려짐 (스팟 2분 통보, 스트래글러) | 지금 체크포인트 저장 → 다른 노드로 미리 이전 |
| `fence_evacuate` | 클러스터 전체가 사라짐 (AZ 장애) | 세대 번호 +1 (옛 Job이 살아 있어도 못 쓰게) → 다른 AZ 클러스터에서 재개 |
| `refuse` | 같은 Job이 같은 버그로 반복 사망 | 재시도 중단, 사용자에게 실패 알림 |
| `hold` | 애매함, 지켜봐야 함 | 아무것도 안 함. 반복되면 다음 이벤트에서 결정 |
| `none` | 기록만 (Xid 63, 92 등 전조) | 아무것도 안 함, DB 저장만 |

## 4. 컨트롤러가 추천을 바꾸는 경우

| 상황 | 추천 | 컨트롤러의 결정 |
|---|---|---|
| 같은 클러스터에 예비 GPU가 없다 | `isolate_migrate` | 다른 클러스터로 이전 |
| `confidence`가 낮다 (예: 0.5 미만) | `isolate_migrate` | `hold`로 낮춤 |
| 이 Job이 이미 복구 중이다 | 무엇이든 | 무시 (중복 처리 방지) |
| VPN 단절인지 AZ 장애인지 아직 모른다 | `fence_evacuate` | `hold` 후 공인망 프로브 결과 보고 결정 |
| `restart_in_place`를 3번 했는데 또 같은 에러 | `restart_in_place` | `refuse`로 올림 |

## 5. 예시 목록

| 파일 | 장면 | category → action |
|---|---|---|
| `01-hw-xid79-isolate` | Xid 79 (GPU fallen off the bus) + Pod CUDA 오류 | hw → isolate_migrate |
| `02-hw-xid48-reset` | Xid 48 (DBE ECC) | hw → reset_then_migrate |
| `03-app-xid13-restart` | Xid 13 + Pod CUDA 오류 (잘못된 커널) | app → restart_in_place |
| `04-hwwarn-xid63-none` | Xid 63 (페이지 리타이어) | hw_warn → none |
| `05-infra-spot-evacuate` | 스팟 회수 2분 통보 | infra → evacuate_preemptive |
| `06-site-az-fence` | 클러스터 스크레이프 끊김 + 공인망 프로브 실패 | site → fence_evacuate |
| `07-straggler-evacuate` | 스텝 시간 p99 1.8배 + 클럭 스로틀 | straggler → evacuate_preemptive |
| `08-hold-collector-down` | DCGM 필드 230만 바뀜, 로그 경로 죽음 | hw → hold |

01과 08은 같은 사건이다. 로그 경로가 죽어 DCGM 경로(08, 14:02:40)가 먼저 와서 `hold`, 로그 경로가 살아나 커널 로그(01, 14:02:11)가 뒤늦게 와서 `isolate_migrate`. `ts` 차이 29초 < 45초라 01은 08의 키를 재사용하고, 08은 키를 소비하지 않았으므로 01이 처리된다.

예시의 `confidence` 값은 자리 표시용이다. 실제 값은 룰 표에서 정한다.

## 6. 확인 필요 (v1 확정 전)

- [ ] (한송이) 3절 · 4절 동작이 컨트롤러 설계와 맞나
- [ ] (한송이) 스트래글러를 `evacuate_preemptive`로 처리해도 되나, 별도 값(`isolate_straggler` 등)이 필요한가
- [ ] (한송이) `refuse`는 컨트롤러가 반복 횟수로 올리는 것만 있나, 분류기가 직접 추천할 경우도 있나
- [ ] (김현수) DB 컬럼 매핑: `raw` · `correlated`는 JSONB 한 칸으로 충분한가
- [ ] (김현수) `dedup_key` 유니크 제약을 DB에도 걸 것인가
