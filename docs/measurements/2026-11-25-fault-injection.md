# 장애 주입 — 학습 프로세스 종료 3번, 첫 자동 복구 (GPU-220)

- 측정: 2026-11-25 14:36 ~ 17:25 KST. EKS `eks-a` · `eks-b`는 12:21부터 떠 있었고 14:32쯤 펜싱을 풀었다. 18시쯤 내렸다
- 구성: `eks-a`, CPU 1대 + GPU 5대(학습 4 · 예비 1, T4). 작업은 SmolLM2-135M + wikitext-2(FSDP 2랭크, 3100 step, 443 step마다 저장), trainer 1.13.0
- 범위: 주입 ①(trainer 워커 `kill -9`) 세 번. ② ~ ⑤(Xid · EC2 종료 · 클러스터)는 하지 못했다
- 전제: **복구 컨트롤러 실행 모드**(`mode: execute`). 사이트 알람은 14:31부터 켜져 있었다(v1.37.0 + Gitops #109)
- 방법: 주입은 사람이 `kubectl exec`로(절차 `deploy/README.md` 「장애 주입」), 관찰은 Alertmanager · Loki의 분류기 로그 · Kafka `job.lifecycle` · `gpu.fault` · `recovery.action` 조회

## 1. 시각표

| 주입 | 작업 · 죽인 랭크의 노드 | T0 주입 | T1 원본 신호 | T3 발행 | 감지 T3 − T0 | 잡은 알람 | 재배치 요청 | 새 세대 Pod Ready | 결과 |
|---|---|---|---|---|---|---|---|---|---|
| ① 1차 | `9c730910` · `ip-172-28-66-59` | 16:23:21 | 16:23:32(JobSet Failed) | 없음 | - | **없음** | 없음 | 없음 | **감지 실패**. `gpu.fault` 0건 |
| ① 2차 | `11108000` · `ip-172-28-76-228` | 17:00:02 | 17:00:15(JobSet Failed) | 17:00:49.85 | **47.9초** | `FailoverJobFailed` | 17:19:10 | 17:21:40 | 발행 성공. 컨트롤러가 그 사건에서 죽어 복구는 19분 뒤 |
| ① 3차 | `df6827fc` · `ip-172-28-66-59` | 17:21:11 | 17:21:15(Pod 종료) | 17:21:49.85 | **38.9초** | `FailoverPodExit` | 17:22:07 | 17:22:54 | 첫 자동 재제출. 새 세대 trainer는 시작하지 못함(3절) |

- T0 = 주입 명령 앞 `date` 출력, T1 = `gpu.fault`의 `dedup_key` 끝 숫자(1차는 관리 클러스터 JobSet의 Failed 조건 시각), T3 = 분류기 로그 `event.published`
- 「새 세대 Pod Ready」 = 감시기 `job.resumed`. **학습이 실제로 이어진 시각이 아니다**(3절). 발표 수치로 쓰지 않는다
- 세 건 모두 첫 체크포인트 승인 뒤에 주입했다(1차 16:18:50 step 443, 2 · 3차 step 1329)
- 주입 시각은 `data.injections`에 넣었다(11/25, 사용자)

## 2. 1차: 죽은 Pod가 14초 만에 지워져 못 봤다

| 시각 | 무슨 일 | 누가 |
|---|---|---|
| 16:23:21 | 워커 `kill -9` → trainer 종료(`exitcode -9`) | 주입 |
| 16:23:24 | 사이드카(`ckpt-sidecar`, native sidecar) 정지 시작 | kubelet |
| 16:23:30 | 다른 랭크 Pod 삭제(`SuccessfulDelete`) | Job (`backoffLimit: 0`) |
| 16:23:32 | JobSet `Failed`(`ReachedMaxRestarts`) | JobSet (`maxRestarts: 0`) |
| 16:23:34 | `job.failed` | 감시기 |
| 16:23:39 이전 | EKS의 JobSet · Pod가 사라짐. 관리 클러스터 JobSet은 `Failed`로 남음 | MultiKueue(추정: 삭제 로그는 보지 않았다) |

- `kube_pod_container_status_terminated_reason`이 **한 번도 수집되지 않았다**(수집 주기 15초). `FailoverPodExit` 미발화 → 분류기 수신 0 → `gpu.fault` 0 → 복구 없음
- 컨트롤러의 「`job.failed` 뒤 300초 안에 `gpu.fault`가 없으면 HOLD」도 16:30까지 나오지 않았다(2차 때는 17:05:23에 `unclassified_job_failure`가 나왔다. 차이의 이유는 모른다)
- 11/23에는 죽은 Pod가 약 37초 남았다. 왜 짧아졌는지는 확인하지 못했다. 3차에서는 JobSet `Failed`가 주입 45초 뒤라 Pod가 오래 보였고 `FailoverPodExit`가 잡았다 → **Pod가 남는 시간은 그때그때 다르다**

**고친 것**(v1.39.0 · v1.40.0 · Gitops #112, 16:59 반영): 관리 클러스터 KSM 지표 `kube_customresource_jobset_failed_time` → 룰 `FailoverJobFailed`(실패 시각이 최근 3분 안) → 분류기 신호 `job_failed`(작업 · 세대마다 `restart_in_place` 1건, `source: pod` · `node: null`). 같은 작업 · 세대에 `FailoverPodExit` 사건과 겹치면 먼저 낸 쪽만 사건이다. 2차가 이 길로 잡혔다.

## 3. 2 · 3차: 발행 뒤에 드러난 것

| 무엇 | 본 것 | 담당 · 상태 |
|---|---|---|
| 컨트롤러가 `node` 없는 사건에서 죽음 | 2.5.1이 `ValueError: node is required for a node-scoped recovery`(`recovery/workflow.py`)로 스레드가 죽고 같은 사건을 다시 읽어 반복(17:00:49 ~ 17:19, `gpu.fault` 지연 1). 그동안 다른 복구도 못 나감 | 한송이. 2.5.2(K8s #57)로 해결 |
| 새 세대 trainer가 시작하지 못함 | 재제출본의 대기 줄이 옛 세대 주소 그대로(`until getent hosts job-…-g0001-…`). `--master-addr`만 새 세대. `waiting for …`만 반복, `up` 0, step 0 | 김성모. relocator 2.5.3(K8s #59, GPU-281)으로 고침. **다음 EKS에서 확인** |
| 겉으로는 복구 완료 | `job.resumed`(Pod 전부 Ready 기준)와 재배치 요청 `Done`이 나왔다. 「장애 복구」의 복구 시간이 65초로 찍힌다 | 김현수. 복구 시간의 끝을 「새 세대 `job.progress`의 step > 마지막 승인 step인 첫 사건」으로 바꾸기로(11/25 합의). `job.resumed`는 중간 단계로 둠 |
| Pod 장애인데 예비 노드 사용 | `11108000` 세대 2의 랭크 1이 예비 노드 `ip-172-28-66-129`에 떴다(원래 `…76-228`). 재제출본에 `devoops/spare` toleration이 조치와 상관없이 붙는다. 재배치 요청 → Pod Ready가 150초(원래 노드로 돌아간 `df6827fc`는 47초) | 김성모. 전달함, 아직 그대로 |

- 2차 사건은 `job_failed` 경로라 `node`가 없다. 죽은 랭크 · 노드는 trainer 로그에 남는다: `Root Cause (first observed failure): host : job-…-workers-0-1… rank : 1 exitcode : -9 … Signal 9 (SIGKILL) received by PID 61`. 사건에 채우는 것은 아직 안 했다
- `restart_in_place`는 같은 노드를 보장하지 않는다(원래 클러스터 우선, ADR-38). 컨트롤러도 이 조치에서는 `node`를 재배치 요청에 넣지 않는다(`moveToSpare`만 `excludeNodes`)

## 4. 주입이 아닌 실패 1건 (기록)

14:36 제출한 `61fa5acc`(QLoRA, 4랭크)가 14:44:33 첫 step에서 4랭크 모두 `RuntimeError: expected mat1 and mat2 to have the same dtype, but got: float != c10::Half`(`train.py` LoRA 층)로 죽었다. 학습 코드 문제다.

- 분류기가 랭크마다 1건씩 `restart_in_place` 4건을 냈다(14:45:04, 중복 제거 대상이 Pod라서). 컨트롤러는 HOLD 2건(`other` · `operation_error`)
- 죽은 Pod가 지워진 뒤 대시보드에서 실패를 볼 곳이 없었다 → 「Kubernetes 워크로드」에 「실패한 학습」 표를 넣었다(v1.38.0)

## 5. 남은 것

- ① 재시험: relocator 2.5.3에서 새 세대가 승인 step + 1부터 실제로 이어 가는지, 원래 노드로 돌아가는지
- ④ GPU EC2 종료(`node_lost` 첫 실전 검증) · ⑤ 클러스터 전체(사이트 알람 첫 실전 검증) · ② 같은 Xid 연속(GPU-221) · ③ 새 Xid 판정
- 「복구했는데 학습이 안 도는」 알람: 새 세대 Pod Ready 뒤 N분 동안 step ≤ 승인 step(또는 trainer 지표 없음)
- `job_failed` 사건에 죽은 랭크 · 노드 채우기(Loki trainer 로그)
- 1차에서 컨트롤러 안전망 HOLD가 안 나온 이유(한송이)
