# 장애 이벤트 JSON v1 (경계 문서 2)

분류기(온프레 모니터링 클러스터 172.30.0.31~.33, ADR-18)가 Kafka `gpu.fault` 토픽(172.30.0.40/.41/.42:9092, 파티션 키 = `node`)에 발행하는 "사고 보고서 한 장" (ADR-7 · ADR-8, 2026-11-05 반영. 이전 "HTTP POST" 표기는 폐기).
소비자는 둘: 복구 컨트롤러(매니저 k8s, 조치)와 수집기(김현수, DB 적재). 컨트롤러는 PromQL · LogQL · Xid 번호를 몰라도 이 문서만 보고 움직일 수 있어야 한다.
전달 보장은 최소 한 번. Kafka 재전송 중복은 소비 측이 `event_id`로 거른다(수집기 · 컨트롤러 모두). 같은 사건이 여러 경로로 올 때 한 번만 내는 것은 분류기 몫이다(`dedup_key`, 2.1절). HTTP POST는 kind · 단위 테스트의 컨트롤러 스텁에만 쓴다.

- 스키마: [`schemas/fault-event.v1.json`](../schemas/fault-event.v1.json) (JSON Schema 2020-12)
- 예시: [`schemas/examples/`](../schemas/examples/) (장애 유형별 8개, 전부 스키마 통과)
- 생산자: 분류기 (이태림) · 소비자: 복구 컨트롤러 (한송이), 수집기 → 데이터 DB (김현수)
- 사건 type 이름(수집기 분류용): `fault.detected`. 보류는 `fault.held`, 재판정은 `fault.reclassified` (김현수 DB 스키마와 합의 중)
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
| `cluster` · `az` | 문자열 | 예: `eks-a`, `us-east-2a` (클러스터 이름은 ADR-32의 `eks-a` · `b` · `c`) |
| `node` | 문자열 / null | site 이벤트는 null. **「학습 작업 실패」 사건**(관리 클러스터 JobSet이 실패로 끝남, 2026-11-25 GPU-220)은 trainer 로그에서 가장 먼저 죽은 Pod의 노드를 찾아 넣고, 못 찾으면 null이다(2026-11-26 GPU-288. 그 전에는 항상 null). 그래서 **`node`가 null인지로 이 사건을 구분하지 않는다** — 구분은 `dedup_key` 대상이 `cluster/job_id/g<세대>` 모양인지로 한다(2.1절). `job_id` · `generation`은 채워진다. 전 랭크가 같이 죽었으면 「먼저 죽은 노드」일 뿐이니 격리 근거로 쓰지 않는다(action은 `restart_in_place`) |
| `gpu_uuid` | `GPU-…` / null | dcgm-exporter 레이블(pci_bus_id → UUID)로 채움 |
| `job_id` | 문자열 / null | **학습 작업 ID** = 백엔드가 만드는 8자리 hex (예 `3f9a1c2e`). k8s JobSet 이름 `job-3f9a1c2e-g0003`이 아니다. 분류기는 **Mimir의 kube-state-metrics 지표**로 찾는다: `kube_pod_info{node}` → `kube_pod_labels{label_devoops_job_id, label_devoops_generation}` (각 클러스터 KSM이 remote_write로 보낸 것, KSM `--metric-labels-allowlist` 필요). EKS API를 직접 부르지 않으므로 VPN·EKS 권한이 없어도 되고, VPN이 끊겨도 마지막 매핑이 남는다 (2026-11-11 김현수 지적으로 정정). 못 찾으면 null. **site는 클러스터가 끊기기 직전에 Running이던 작업마다 이벤트 1건**(작업이 없으면 이벤트 없음, 조회 실패 · 그 시각 KSM이 안 긁히고 있었으면 null로 1건. 2026-11-24 GPU-220) |
| `generation` | 정수 / null | 세대 번호. 원본은 DynamoDB 펜싱 테이블 `devoops-job-fencing` (ADR-6). Pod 라벨 `devoops/generation`에서 읽음 |
| `source` | enum | `dcgm` · `dmesg`(커널 로그, journald 경유) · `pod` · `straggler` · `site` · `spot` · `node`(GPU 노드가 사라짐: NotReady · Node 삭제. 클러스터 지표는 계속 오는 경우만, 끊기면 `site`) |
| `raw` | 객체 | 분류 근거 원본. `xid` · `exit_code` · `metric` · `value` · `log_line` (해당 없으면 null) |
| `correlated` | 배열 | 상관 창 안에서 함께 본 다른 알람 (`alert` · `source` · `ts`) |
| `collector_healthy` | bool | 분류 시점에 그 노드의 수집 경로(Prometheus Agent · Fluent Bit · OTel 게이트웨이)가 살아 있었나 |
| `dedup_key` | 문자열 | 같은 사건 식별 키. `대상\|코드\|기준 시각(epoch 초)`. 규칙은 2.1절 |
| `category` | enum | `hw` · `hw_warn` · `driver` · `app` · `infra` · `site` · `network` · `straggler` |
| `confidence` | 0~1 | 분류 확신도. Xid 하나로 원인 확정 불가(NVIDIA 공식 입장)라서 둠 |
| `suggested_action` | enum | 아래 3절 8개 |
| `straggler_evidence` | 객체 (선택) | 스트래글러 판정 근거. `category: straggler`일 때만 넣는다. `duration_seconds`(느린 상태가 이어진 시간) · `cause_node`(원인 노드, **이벤트의 `node`와 같은 값** — 스키마로는 못 막아서 분류기 테스트로 검증) · `estimated_gain_seconds`(순절감 = 그대로 둘 때 손해 − 이전 비용 C, 음수 가능). 시간은 전부 **정수 초**. 필드 이름은 컨트롤러 FSDP-002와 같다 |

스키마가 강제하는 규칙 두 개:

- `collector_healthy: false`면 `suggested_action`은 `hold`만 허용 (site 제외: 사이트 장애면 수집 경로가 죽는 게 당연함)
- `category: site`면 `node` · `gpu_uuid`는 null
- `category: straggler` + `suggested_action: evacuate_preemptive`면 `straggler_evidence` 필수, `duration_seconds` ≥ 600 · `estimated_gain_seconds` ≥ 1 (ADR-37, 컨트롤러 조건과 같은 값)
- `straggler_evidence`는 `category: straggler`에만 허용. `hold`에는 값 제한 없이 넣을 수 있다(HOLD 이유 확인용)
- `dedup_key`는 `대상|코드|10자리 epoch 초` 형식만 허용 (ISO 시각 문자열 금지: `…11Z`와 `…11.000Z`가 다른 키가 되는 것 방지)

### 2.1 중복 처리 규칙

같은 사건이 여러 번 올 수 있다. 경로가 둘(커널 로그 · DCGM)이고, Fluent Bit · 게이트웨이 재시작이나 Alertmanager 재전송도 있다.

**키 만들기 (분류기)**

- 대상 = `gpu_uuid` (spot · node는 `node`, site는 `cluster/job_id` — 작업마다 다른 사건이라서. `job_id`가 null이면 `cluster`. 학습 작업 실패는 `cluster/job_id/g<세대>`), 코드 = `raw.xid` (없으면 `source`). 예 `eks-a/3f9a1c2e|site|1795010531` · `eks-a/3f9a1c2e/g3|pod|1795010531`
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

**누가 무엇으로 거르나 (분류기 · 컨트롤러 · 수집기)**

- Kafka는 최소 한 번 전달이라 같은 메시지가 두 번 올 수 있다. 수집기는 `event_id`로 거른다(`ON CONFLICT (event_id) DO NOTHING`). `event_id`는 재전송에도 같다
- 컨트롤러도 **`event_id`로만** 중복을 판정한다. `dedup_key`는 중복 제거에 쓰지 않는다 (한송이 2026-11-17, K8s develop 코드 기준). 같은 `event_id`로 내용만 바꿔 보내면 ID 충돌로 거부된다
- 그래서 **같은 사건을 한 번만 내는 것은 분류기 책임이다.** 분류기는 직전에 보낸 이벤트와 **키와 `suggested_action`이 둘 다 같으면** 다시 보내지 않는다. 이걸 빠뜨리면 두 경로(커널 로그 · DCGM)의 같은 사건이 컨트롤러에 새 사건 두 건으로 들어간다
- 키는 같고 action이 바뀌었으면 **새 `event_id`로** 보낸다(재판정 `fault.reclassified`, `dedup_key`는 그대로). 컨트롤러는 새 사건으로 받고, 실제로 새 복구를 시작할지는 그때의 실행 세대와 펜싱 상태를 보고 정한다 (예: 08 `hold` → 01 `isolate_migrate`)

## 3. suggested_action → 컨트롤러 동작

| suggested_action | 뜻 | 컨트롤러가 실제로 하는 일 |
|---|---|---|
| `restart_in_place` | GPU 멀쩡, 프로그램만 문제 (Xid 13 · 31 · 32 · 43 · 94 등, Pod 종료 · 학습 작업 실패) | 세대 +1 → 옛 JobSet 정리 · 중단 확인 → **원래 클러스터를 먼저 보는**(같은 노드는 보장 안 함. 빈 GPU가 잡 크기보다 적으면 디스패처가 다른 클러스터로 보낸다, ADR-38) 새 JobSet 재제출 → 승인 체크포인트에서 이어 감. Pod 재시작이 아니다: torchrun · Job · JobSet · Kueue 자동 재시작은 전부 꺼져 있고 재시작 주체는 컨트롤러뿐 (ADR-12) |
| `isolate_migrate` | 이 GPU 못 씀 (Xid 79 · GPU 노드가 사라짐) | 노드 cordon + taint `devoops/faulty` → 세대 +1 → 옛 JobSet 정리 · 중단 확인 → 같은 클러스터 예비 GPU(taint `devoops/spare`)로 새 JobSet (자리가 모자라면 디스패처가 다른 클러스터로, ADR-38) |
| `reset_then_migrate` | 이 GPU 리셋 필요 (Xid 48 · 64 · 95 · 119 · 120 · 140 등) | 먼저 다른 노드로 이전 → 빈 노드에서 GPU 리셋 → 성공하면 예비로 복귀, 실패하면 폐기 |
| `evacuate_preemptive` | 곧 죽거나 느려짐 (스팟 2분 통보, 스트래글러) | 노드 격리 → 세대 +1 → 옛 실행 종료 확인 → 같은 클러스터 예비 GPU로 새 JobSet, **마지막 승인본에서 재개**. 스팟 회수 예고 때도 마지막 저장을 기다리지 않는다(ADR-36, 11/13. ADR-28의 120초 마지막 저장은 폐기) |
| `fence_evacuate` | 클러스터 전체가 사라짐 (AZ 장애) | 클러스터 펜싱(`devoops/fenced=true`) → 세대 +1 (옛 Job이 살아 있어도 승인 못 하게) → 옛 실행 중단을 확인할 수 없으므로 확인 어노테이션을 기다렸다가 다른 AZ 클러스터에서 재개 |
| `refuse` | 다시 띄워도 같은 자리에서 죽음. 분류기는 **메모리 부족**(컨테이너 `OOMKilled` · trainer 로그의 `CUDA out of memory`)일 때 직접 낸다. 같은 버그로 반복 사망은 컨트롤러가 횟수로 올린다 | 재시도 중단, 사용자에게 실패 알림 |
| `hold` | 애매함, 지켜봐야 함 | 아무것도 안 함. 반복되면 다음 이벤트에서 결정 |
| `none` | 기록만 (Xid 63, 92 등 전조 · 154 같은 보조 정보) | 아무것도 안 함, DB 저장만 |

세대 +1은 복구 종류와 상관없이 **Job을 다시 띄울 때마다** 복구 컨트롤러가 한 번 한다 (ADR-10). 위 표의 "세대 +1"은 그 공통 단계다.

**이벤트를 만들지 않는 종료**: 펜싱(사이드카가 세대 불일치 · 승인 거부로 `/ckpt/STOP`을 만들어 trainer가 스스로 멈춤) · 사용자 취소 · 정상 완료(exit 0)는 장애가 아니므로 `source=pod` 사건을 만들지 않는다. 펜싱 · 취소는 종료 코드가 아니라 Kafka `job.lifecycle`(`job.fenced` · `job.cancelled`)로 구분한다 (ADR-43). 예전에 펜싱을 뜻하던 종료 코드 75는 없어졌다(trainer GPU-244, 분류기 GPU-223).

### 3.1 세 문서의 액션 이름 대응 (매핑표, 2026-11-05 한송이 · 김성모 확인 · 2026-11-09 ADR-19 반영 · 2026-11-11 컨트롤러 contracts.md 기준 갱신 · 2026-11-17 ADR-38 · ADR-39 반영)

분류기(이 문서) · 복구 컨트롤러(한송이 "3. GPU·드라이버 장애복구" 4절) · 재배치 실행기(김성모 "평소 학습, 장애복구 시 스케줄링 방식" RelocationRequest)가 서로 다른 이름을 쓴다. 변환은 **컨트롤러가 한다**: Kafka에서 `suggested_action`을 읽어 자기 액션으로 결정하고, 실행이 필요하면 RelocationRequest를 만든다. 분류기는 이 표의 왼쪽 열만 낸다.

| `suggested_action` (분류기) | 컨트롤러 액션 (한송이) | RelocationRequest `action` (김성모) | 비고 |
|---|---|---|---|
| `restart_in_place` | `RETRY_GROUP` | `restartInPlace` | 세대 +1 · 새 JobSet은 공통. **원래 클러스터를 먼저 본다**(같은 노드 보장 아님), Recovery 큐로 4·5·7단계를 밟는다. 원래 클러스터 빈 GPU가 잡 크기보다 적으면 디스패처가 기다리지 않고 다른 클러스터로 보낸다(ADR-38, action 이름은 그대로). 어디에도 자리가 없어 15분을 넘기면 `AdmitDelayed`로 표시하고 계속 지켜보며, JobSet이 사라졌을 때만 Failed (김성모 11/11) |
| `isolate_migrate` | `QUARANTINE_AND_RETRY` | `moveToSpare` | 노드 cordon + `devoops/faulty` → 같은 클러스터 예비로. 예비가 없거나 고장 노드가 여럿이어도 이 action 그대로이고, 자리가 모자라면 디스패처가 다른 클러스터로 보낸다(ADR-38). GPU 노드 여러 대가 사라지면(`source: node`) 노드마다 1건이 나가고, 같은 작업의 둘째 건부터는 컨트롤러가 이미 복구 중이라 STALE로 닫는다(2026-11-24) |
| `reset_then_migrate` | `QUARANTINE_AND_RETRY` | `moveToSpare` | 컨트롤러 입장에선 `isolate_migrate`와 같다. GPU 리셋은 한송이 6절대로 **별도 운영 액션**(격리 뒤 수동 · 후속), 자동 리셋 안 함 |
| `evacuate_preemptive` | `QUARANTINE_AND_RETRY` | `moveToSpare` | 스팟 2분 통보 · 스트래글러. 세대를 먼저 올리고 **마지막 승인본에서 재개**한다 (ADR-19 v5, 컨트롤러 contracts.md). 사전 저장 뒤 옮기는 `PLANNED_MIGRATE`는 **보류**(컨트롤러에서 현재 HOLD). 스팟 회수 예고의 이 순서(승인본 확정 → 세대 +1 → 옛 실행 종료 → 재개)는 ADR-36(11/13)으로 확정됐다. ADR-28은 폐기. **스트래글러는 조건부**(ADR-37, 11/16): 분류기는 느린 상태가 600초 이상 이어지고 「그대로 둘 때 손해 ≥ 2 × 이전 비용 C」일 때만 이 값을 내고, 아니면 `category: straggler` + `hold`로 낸다. C(실측 전 900초 가정)와 안전 계수 2는 **분류기**가 계산해 `straggler_evidence.estimated_gain_seconds`에 순절감으로 넣고, 컨트롤러는 양수인지만 본다. 예비 용량 · 작업당 1회 · 수집 정상 · 정책 활성화(기본 OFF)는 **컨트롤러**가 최종 확인한다 |
| `fence_evacuate` | `FENCE_AND_EVACUATE` | `evacuateCluster` | 클러스터 펜싱(`devoops/fenced=true`) → 옛 실행 권한 차단. 펜싱 확인은 `devoops/old-execution-stopped=true` 어노테이션만이 아니라 **실제 차단 근거**(세대 +1 반영 · 하트비트 끊김)까지. `evacuateCluster`는 이 행 전용 |
| `refuse` | `NEEDS_CONFIG_FIX` (결과 state `REJECTED`) | 없음 (복구 안 함) | 사용자에게 실패 알림. 분류기가 메모리 부족으로 직접 낸 `refuse`(2026-11-26, GPU-222)는 재시도 없이 바로 이 상태가 된다. 자동 복구 한도(3회) 초과 · init 종료 코드 5 · 6도 같은 `REJECTED`로 끝난다. Slack 알림은 HOLD만 있고 `REJECTED` 알림은 협의 중(11/11, 카운터 추가되면 MON-STD-25에 포함). 자동 재시작이 전부 꺼져 있어 JobSet이 Failed → Kueue Workload 종료 → 쿼터 자동 반납될 것으로 봄(김성모, 미검증). 멈춘 채 안 죽는 Job만 별도 처리 |
| `hold` | `HOLD` | 없음 | 상태 저장 · 알림. Kafka `recovery.action` `type=recovery.held`, Slack은 Mimir 룰 `RecoveryHold`(MON-STD-25, ADR-27). HOLD는 그 사건의 최종 상태이고 자동 재개하지 않는다 |
| `none` | `NONE` (결과 state `IGNORED`) | 없음 | 기록만. 컨트롤러는 Kafka에서 읽어도 처리하지 않는다 |

**컨트롤러 액션 이름 (2026-11-11 확인)**: 가운데 열은 K8s 레포 `controllers/recovery-controller/docs/contracts.md`(b017283)의 표와 같다. 11/09에 적어 둔 옛 이름(`RESTART_SAME_CLUSTER` · `RELOCATE_SAME_CLUSTER` · `HOLD_FOR_FENCING` · `REVIEW` · `REJECT`)은 더 이상 쓰이지 않는다(`REVIEW`는 읽을 때 HOLD로 변환). 컨트롤러는 `collector_healthy=false`인 비-site 이벤트를 HOLD로 낮추고, `job_id` · `generation`을 특정하지 못한 신호도 HOLD한다. 분류기 출력(왼쪽 열)은 바뀌지 않는다.

RelocationRequest `action`은 `restartInPlace` · `moveToSpare` · `evacuateCluster` **세 가지뿐이다** (ADR-38, 2026-11-17 v5):
- **`moveToOtherCluster`는 없어졌다.** 예비 소진은 action을 고르는 기준이 아니다. 컨트롤러는 원인만 보고 action을 고르고, 원래 클러스터에 자리가 없을 때 다른 클러스터로 보낼지는 디스패처가 정한다. 다른 클러스터에 배치돼도 action 이름은 바뀌지 않는다. 분류기 출력(왼쪽 열)은 영향이 없다
- **SDC는 이번 프로젝트 범위 밖이다** (ADR-39). v1 `category`에 SDC가 없고, 전용 감지 · HOLD · 롤백 · 다른 클러스터 승격도 만들지 않는다

### 3.2 전달 필드 대응

한송이 4절 "incident_id · job_id · generation · node · gpu_uuid · evidence · action · 룰 버전"과 v1 필드:

| 한송이 필드 | v1 필드 | 비고 |
|---|---|---|
| `incident_id` | `dedup_key` | 같은 사건 묶음 키. 개별 판정은 `event_id`(= trace ID). 재판정(`fault.reclassified`)은 `event_id`가 바뀌고 `dedup_key`는 같다 |
| `job_id` · `generation` · `node` · `gpu_uuid` | 같은 이름 | |
| `evidence` | `raw` + `correlated` + `category` + `confidence` + `source` | 한 필드로 합치지 않는다 |
| `action` | `suggested_action` | 추천. 결정은 컨트롤러 |
| 룰 · 임계값 버전 | **v1: Kafka 메시지 헤더 `rule_version`** | 본문 필드가 없다. 분류기가 헤더에 룰 파일 git SHA(짧은 7자)를 넣고, 컨트롤러가 읽어 판단 기록에 남긴다 (한송이 동의 11/05). 본문 필드 `rule_version` 추가는 김현수와 DB 저장 형식까지 합의한 뒤 v1.1 |

## 4. 컨트롤러가 추천을 바꾸는 경우

| 상황 | 추천 | 컨트롤러의 결정 |
|---|---|---|
| 같은 클러스터에 예비 GPU가 없다 | `isolate_migrate` | 추천을 바꾸지 않는다. `moveToSpare` 그대로 요청하고, 원래 클러스터에 자리가 모자라면 디스패처가 다른 클러스터에 둔다. 어디에도 없으면 복구 큐에서 기다린다 (ADR-38, 3.1절) |
| `confidence`가 낮다 (예: 0.5 미만) | `isolate_migrate` | `hold`로 낮춤 |
| 이 Job이 이미 복구 중이다 | 무엇이든 | 무시 (중복 처리 방지) |
| VPN 단절인지 AZ 장애인지 아직 모른다 | `fence_evacuate` | `hold` 후 VPN 터널을 우회하는 별도 경로(DynamoDB `heartbeat_at` · progress 갱신, EC2 상태 조회)를 보고 결정. 워커는 프라이빗 서브넷이라 공인망에서 직접 probe할 수 없다. 이 경로도 pfSense · 회선은 VPN과 공유하므로, 조회 자체가 실패하면 하트비트가 멈춘 것으로 보지 않고 `hold`를 유지한다 (MON-STD-18) |
| 자동 복구를 3번(확정, 한송이 2026-11-26) 다 쓴 뒤 또 같은 에러 | `restart_in_place` | 4번째 요청은 `REJECTED`로 닫는다. 횟수는 컨트롤러만 센다(분류기는 반복이어도 `restart_in_place` 그대로 보낸다) |

## 5. 예시 목록

| 파일 | 장면 | category → action |
|---|---|---|
| `01-hw-xid79-isolate` | Xid 79 (GPU fallen off the bus) + Pod CUDA 오류 | hw → isolate_migrate |
| `02-hw-xid48-reset` | Xid 48 (DBE ECC) | hw → reset_then_migrate |
| `03-app-xid13-restart` | Xid 13 + Pod CUDA 오류 (잘못된 커널) | app → restart_in_place |
| `04-hwwarn-xid63-none` | Xid 63 (페이지 리타이어) | hw_warn → none |
| `05-infra-spot-evacuate` | 스팟 회수 2분 통보 | infra → evacuate_preemptive |
| `06-site-az-fence` | 클러스터 스크레이프 끊김 + 공인망 프로브 실패 | site → fence_evacuate |
| `07-straggler-evacuate` | 랭크 계산 시간이 중앙값의 1.6배로 720초 지속 + 클럭 스로틀. 손해 2,880초 ≥ 2 × 900 → 순절감 1,980초 | straggler → evacuate_preemptive |
| `08-hold-collector-down` | DCGM 필드 230만 바뀜, 로그 경로 죽음 | hw → hold |
| `09-straggler-hold` | 같은 1.6배 · 720초지만 남은 스텝이 적어 손해 600초 < 2 × 900 → 순절감 −300초 | straggler → hold |
| `10-infra-node-lost-isolate` | GPU 노드가 NotReady가 되거나 Node가 지워짐(스팟 회수 · 인스턴스 종료), CPU 노드의 Agent · KSM은 살아 있음 | infra → isolate_migrate |
| `11-app-oom-refuse` | trainer 컨테이너가 메모리 한도를 넘겨 `OOMKilled`로 끝남 | app → refuse |

01과 08은 같은 사건이다. 로그 경로가 죽어 DCGM 경로(08, 14:02:40)가 먼저 와서 `hold`, 로그 경로가 살아나 커널 로그(01, 14:02:11)가 뒤늦게 와서 `isolate_migrate`. `ts` 차이 29초 < 45초라 01은 08의 키를 재사용한다. action이 달라졌으므로 새 `event_id`로 나가고, 컨트롤러는 새 사건으로 처리한다.

예시의 `confidence` 값은 자리 표시용이다. 실제 값은 룰 표에서 정한다.

## 6. 확인 필요 (v1 확정 전)

- [ ] (한송이) 3절 · 4절 동작이 컨트롤러 설계와 맞나 — 2026-11-05 확인됨: Kafka `gpu.fault` 소비, 봉투 `occurred_at`=판정 시각 · `payload.ts`=원본 시각 구분, exit 75 이벤트 없음, dcgm 5초는 확정값(±45초 창 전제). 나머지는 매핑표(11/12)와 함께
- [x] (한송이) 스트래글러를 `evacuate_preemptive`로 처리해도 되나, 별도 값(`isolate_straggler` 등)이 필요한가 — 2026-11-16 확정: 별도 값 없이 `evacuate_preemptive` + `straggler_evidence`(조건 미충족은 `hold`, 이때도 evidence 포함 가능). ADR-37 채택, 컨트롤러 정책 스위치 기본 OFF
- [x] (한송이) `refuse`는 컨트롤러가 반복 횟수로 올리는 것만 있나, 분류기가 직접 추천할 경우도 있나 — 2026-11-26 확정: 분류기는 메모리 부족(컨테이너 `OOMKilled` · 종료한 trainer 로그의 `CUDA out of memory`)이 확인될 때만 직접 `refuse`를 낸다. Pod가 빨리 지워져 원인을 모르면 `restart_in_place`
- [x] (한송이) 소프트웨어 `restart_in_place` 재시도 상한 — 2026-11-26 확정: 최대 3회, 한도를 다 쓴 뒤 온 4번째 요청은 `REJECTED`. 횟수는 컨트롤러에서만 관리한다
- [ ] (김현수) DB 컬럼 매핑: `raw` · `correlated`는 JSONB 한 칸으로 충분한가
- [ ] (김현수) `dedup_key` 유니크 제약을 DB에도 걸 것인가
- [x] (한송이 · 김성모) 3.1 매핑표 확인 완료 (2026-11-05): `PLANNED_MIGRATE` · `FENCE_AND_EVACUATE` 신설, `rule_version`은 v1 Kafka 헤더, SDC는 컨트롤러 승격, `moveToOtherCluster` 신설 (→ 둘 다 2026-11-17에 없어짐: ADR-38 · ADR-39)
- [x] (나) 3.1 가운데 열을 컨트롤러 contracts.md(K8s b017283) 기준으로 갱신 (2026-11-11): `evacuate_preemptive` → `QUARANTINE_AND_RETRY`(`PLANNED_MIGRATE` 보류), 옛 이름 주의문 삭제. `moveToOtherCluster` 행은 11/09부터 있음(11/11 회의 A0-1 확인 요청에 대한 답)
- [x] (김성모) `moveToOtherCluster` 시나리오 10 실행 확인 — 해당 없음 (2026-11-17): ADR-38로 action이 없어지고 ADR-39로 SDC가 범위 밖이 됨
- [x] (한송이 · 김현수) 컨트롤러 중복 제거 기준 — 2026-11-17 한송이 확인: 컨트롤러는 `event_id`로만 중복을 판정하고 `dedup_key`는 쓰지 않는다. 재판정은 새 `event_id` + 같은 `dedup_key`로 내면 새 사건으로 처리되므로 현수 안건 7 "dedup_key로 거르면 재판정을 놓침"은 생기지 않는다. 2.1절을 이 기준으로 고침(같은 사건 묶기는 분류기 책임)
- [ ] (김성모) `refuse` 때 쿼터 자동 반납 실제 확인 (JobSet Failed → Workload 종료)
