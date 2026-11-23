# 장애 주입 — 학습 프로세스 종료 · Xid 79 (GPU-92)

- 측정: 2026-11-23 14:44 ~ 16:20 KST. EKS는 12:24(`eks-a`) · 12:44(`eks-b`)부터 떠 있었다
- 구성: `eks-a`, t3.large 1대 + g4dn.xlarge 5대(학습 4 · 예비 1). 작업은 SmolLM2-135M + wikitext-2(FSDP), trainer 1.8.2
- 범위: 주입 ①(trainer 워커 `kill -9`) 두 번 · 주입 ③(`dcgmi`로 Xid 79) 한 번. 주입 ②(한송이 시나리오 Xid 79 → 48)는 하지 않았다
- 전제: **복구 컨트롤러는 Kafka 수신 · observe · 실행 꺼짐**(Gitops main `overlays/onprem/config.json`). 그래서 본 것은 「감지 → 분류 → `gpu.fault` 발행」까지이고 복구(T4 · T5)는 없다
- 방법: 주입은 사람이 `kubectl exec`로(절차 `deploy/README.md` 「장애 주입」), 관찰은 4초 간격 조회(Pod 상태 · Alertmanager · 분류기 지표 · Mimir `ALERTS`)와 Loki의 분류기 로그

## 1. 시각표

| 주입 | 작업 · 랭크 수 · 대상 노드 | T0 주입 | T1 원본 신호 | T2 알람 | T3 발행 | 감지 T3 − T0 | 분류 T3 − T1 | 조치 |
|---|---|---|---|---|---|---|---|---|
| ① kill -9 (평가 지연 1분) | `5983f88d` · 2 · `ip-172-28-71-24` | 14:58:06 | 14:58:09 | 14:59:34 | 14:59:34.9 | **88.9초** | 85.9초 | `app` · `restart_in_place` |
| ① kill -9 (평가 지연 15초) | `fb6d20e3` · 2 · `ip-172-28-71-24` | 15:40:46 | 15:40:49 | 15:41:19 | 15:41:19.8 | **33.8초** | 30.8초 | `app` · `restart_in_place` |
| ③ dcgmi Xid 79 (평가 지연 15초) | `391ba25c` · 4 · `ip-172-28-74-28` | 16:11:43 | 16:12:00 | 16:12:19 | 16:12:19.9 | **36.9초** | 19.9초 | `hw` · `isolate_migrate` |

- T0 = 주입 명령 앞 `date` 출력, T1 = `gpu.fault`의 `dedup_key` 끝 숫자(epoch초), T2 = Alertmanager `startsAt`, T3 = 분류기 로그 `event.published`
- 세 건 모두 `job_id`가 채워졌고 `gpu.fault`는 주입마다 한 건만 나갔다. HOLD 0건, Slack 수신 0건(원인 알람은 분류기로만 간다)
- 첫 체크포인트 승인 뒤에 주입했다(1차 14:56:10 step 443)

## 2. 룰 평가 지연 1분 → 15초

1차에서 지표는 종료 12초 뒤(14:58:21) 들어왔는데 알람은 85초 뒤에 울렸다. Mimir가 룰을 `ruler_evaluation_delay_duration`(기본 1분) 전 시점 기준으로 평가하기 때문이다.

| 근거 (최근 3시간, Mimir 재시작 3회 포함) | 값 |
|---|---|
| Agent → Mimir 전송 지연, 평소 | 0 ~ 1초 |
| 전송 지연 최대 | `eks-a` 7초 · `eks-b` 5초 · 모니터링 클러스터 2초 |
| 룰 평가 시간 · 놓친 평가 · 실패 | 0.02초 · 0 · 0 |

15초로 줄인 뒤(v1.24.3) 같은 주입의 감지가 88.9초 → 33.8초가 됐다. Pod 종료는 분류기가 종료 뒤 30초를 기다리므로(다른 Pod 종료를 한 건으로 묶기 위해) 이 값이 하한이다. 2차의 분류 30.8초가 그 대기와 맞는다.

## 3. 주입 ①에서 본 것

| 무엇 | 결과 |
|---|---|
| 죽인 랭크 | `kill -9` 3초 뒤 `trainer`가 `Error`(종료 코드 1). Pod는 `Failed` |
| 다른 랭크 | 7초쯤 뒤 Job이 삭제(`SuccessfulDelete`). `Error`로 끝난 것이 아니라 알람이 안 울리고 `gpu.fault`도 더 나가지 않는다 |
| 죽은 Pod가 남아 있던 시간 | 약 37초. Kueue가 Workload를 끝냄으로 처리하면서 EKS의 JobSet이 같이 지워진다(`jobset failed due to reaching max number of restarts`) |
| `Error` 지표가 수집된 횟수 | 1차 2번 · 2차 3번. **Pod가 15초 안에 지워지면 `FailoverPodExit`가 못 잡는다** |
| 진짜 Xid | `{k8s_cluster_name="eks-a", service_name="kernel"} \|~ "NVRM\|Xid"` 0줄. 워커를 죽여도 Xid 45는 나지 않았다 → 커널 로그 Xid 룰(GPU-218)에 쓸 원문은 이 방법으로 못 얻는다 |
| 컨트롤러 | 5분 뒤에도 `recovery_hold_total` 없음. 하트비트 경과는 작업이 죽은 뒤 계속 오른다 |

## 4. 주입 ③에서 본 것

| 무엇 | 결과 |
|---|---|
| 지표 | 주입 13초 뒤(16:11:56) `DCGM_FI_DEV_XID_ERRORS` = 79 |
| 알람 | `FailoverXidDcgm`(`xid="79"` · `source="dcgm"` · `gpu_uuid`), 1분 울리고 꺼짐 |
| 발행 | `category` `hw` · `action` `isolate_migrate` · `job_id` · `node` · GPU UUID |
| 학습 | 4개 랭크 모두 계속 진행(주입은 엔진 값만 바꾼다). 노드 격리 · taint 없음(observe) |
| 원본 신호 시각 | 사건에는 16:12:00, 지표 첫 샘플은 16:11:56. 룰이 시각을 15초 눈금으로 읽어 최대 15초 늦게 적힌다. 중복 제거 창(±45초) 안 |
| 사용자 중단 | 프런트에서 중지 · 삭제 → 학습 Pod 4개가 사라졌지만 `gpu.fault` 없음 |
| 정리 | 주입한 `dcgm-exporter` Pod 삭제(16:17:30) → Xid 시계열 사라짐 · GPU 지표 복귀. 값이 79에서 없음으로 바뀌는 것은 알람이 아니다 |

## 5. 알람이 한쪽 Alertmanager에 약 70초 더 남는다

- 2차 · 3차에서 알람이 풀릴 때 `alertmanager-1`은 「풀림」으로, `alertmanager-0`은 같은 통지를 「울림」으로 받았다(`alertmanager_alerts_received_total`: 같은 시각에 한쪽은 `resolved` +1, 다른 쪽은 `firing` +1). 1분 뒤 Mimir가 「풀림」을 다시 보내면 정리된다(2차 15:42:09 → 15:43:18, 3차 16:13:20 → 16:14:29)
- 그 사이 `alertmanager-0`이 분류기에 한 번 더 보내고 분류기가 버린다(`classifier_alerts_total{outcome="duplicate"}` 2). `gpu.fault`는 두 번 나가지 않았다
- 추정 원인: 노드 사이 시계 차이 약 0.02초. 풀린 시각이 받는 쪽 시계로 조금 미래면 아직 안 끝난 것으로 읽힌다. 룰을 평가한 Mimir Pod와 같은 노드의 Alertmanager는 바로 풀렸다. 확정은 아니다
- 조치 없음. 알림 정리 때 다시 본다

## 6. 대시보드 「장애 복구」

- PostgreSQL 패널이 실제 DB(`data.v_incidents`)에서 값을 읽는 것을 처음 확인했다(11/20부터 미확인이던 것)
- 「감지」와 「분류」가 항상 같은 값이다. 뷰가 `fault.detected` 한 건의 시각을 두 칸에 모두 쓴다. 주입 시각(T0)을 `data.injections`에 넣지 않으면 시작이 원본 신호 시각(T1)이라 화면 값은 T3 − T1이다(85.8초 · 30.8초)
- 분류 시간을 따로 보려면 분류기가 알람을 받은 시각을 사건에 남겨야 한다(사건 형식 · ADR-15 · Data 뷰 변경, 나중에)
- 단위가 60초를 넘으면 `1.4 mins`로 바뀌던 것을 초 고정으로 고쳤다(v1.24.2)

## 7. dcgm-exporter 메모리 (사이드카로 나눈 뒤 첫 실측)

`max by (cluster, container) (max_over_time(container_memory_working_set_bytes{container=~"dcgm-exporter|nv-hostengine"}[6h]))`, 16:25 조회

| 컨테이너 | 6시간 최대 (`eks-a` · `eks-b`) | 요청 · 한도 |
|---|---|---|
| `dcgm-exporter` | 99MiB · 99MiB | 128Mi · 1Gi |
| `nv-hostengine` | 40MiB · 40MiB | 256Mi · 1Gi |
| Pod 합계 | 138MiB · 136MiB | |

- 재시작 0회. 학습 중인 노드와 예비 노드의 차이는 10MiB 안쪽이다
- 엔진을 내장했을 때(11/19 약 435MiB)의 3분의 1이다. 한도 1Gi는 넉넉하고, 11/19 노트의 「한도 768Mi · 요청 512Mi로 조정」은 이 값 기준으로 다시 정한다(미정)

## 8. `DCGM_FI_DEV_POWER_VIOLATION`

`rate(DCGM_FI_DEV_POWER_VIOLATION{cluster="eks-a"}[10m])`, 16:10(4개 노드 학습 중 · GPU 사용률 99 ~ 100%)

| 노드 | 전력 | 증가율 (ns/s) |
|---|---|---|
| 학습 4대 | 62 ~ 70W | 약 9.7 × 10⁸ |
| 예비 1대 (유휴) | 11.5W | 약 7.9 × 10⁸ |

유휴에도 초당 약 0.8초씩 오른다. T4에서는 전력 제한 신호로 쓸 수 없다(11/19 노트 3절의 질문에 대한 답). 온도 최대는 51°C였다.

## 9. Mimir (같은 날 이중화 4단계와 겹침)

- Agent에 묶음 라벨(`agent_group`)을 붙인 직후 Pod당 시계열 최대 35.1만 · 메모리 최대 1,342MiB(한도 60만 · 3Gi). 16:25에는 21.2만 · 약 1,000MiB, 수신 초당 약 12,900 샘플(EKS 2개 가동)
- 4단계 이후의 Mimir 재시작 2회(v1.22.0 · v1.24.3) 동안 쓰기 실패 0

## 10. 남은 것

- 주입 ②(Xid 79 → 48): 한송이가 실행 모드를 켜는 날
- 주입 시각을 `data.injections`에 넣기(Data `python -m collector.injection add`): 14:58:06 · 15:40:46(`trainer_kill`) · 16:11:43(`xid`), 노드는 1절
- 죽은 Pod가 37초 만에 지워지는 것을 `FailoverPodExit`가 놓치지 않게 할 방법(스케줄링 쪽 정리 시점 확인)
- EKS 감사 로그가 꺼져 있다(`devoops-prod-eks-a` 로그 5종 비활성). Pod를 누가 지웠는지 클러스터에서는 알 수 없다
- dcgm-exporter 요청 · 한도 재조정, 분류 시간 분리(6절)
