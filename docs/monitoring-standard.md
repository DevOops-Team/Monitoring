# 모니터링 표준 (뼈대 · 1단계)

> 상태: **뼈대**. 표 형식 · 항목 목록 · 근거 출처 종류만 고정했고 값 열은 비어 있다.
> 정본은 Confluence `3. 설계 › 모니터링 표준`, 값과 근거 링크는 이 파일. 룰 파일의 annotation `standard: MON-STD-nn`이 둘을 잇는다.
> 관련: 로드맵 "모니터링 표준 만들기" 절, [장애 이벤트 JSON](fault-event.md).

## 1. 목적과 읽는 법

표준 = **대상 × 지표 × 정상 × 경고 × 위험 × 근거 × 조치**가 한 표에 있고, 모든 알람 룰 · 런북 · 대시보드 패널이 그 표의 번호를 달고 있는 상태.

회사식 운영 기준표와 다른 점 하나: **숫자마다 근거가 있다.**
"CPU 80%"가 아니라 "CPU 80%를 5분 넘기면 데이터 로더가 밀려 스텝 시간이 p50 대비 30% 늘었다(측정일)"처럼, 모든 임계값은 2절의 출처 종류 중 하나와 링크를 가진다. 근거 없는 숫자는 "가설" 열에만 둔다.

## 2. 근거 출처 종류

| 종류 | 뜻 | 채워지는 단계 | 예 |
|---|---|---|---|
| **벤더** | NVIDIA · AWS · Prometheus 공식 문서의 스펙 값 | 지금(뼈대)부터 | `nvidia-smi -q -d TEMPERATURE`의 Slowdown Temp, 스팟 2분 통보, Agent WAL 기본 보존 |
| **기준선** | 데모 Job 설정을 고정하고, 장애 주입 없는 정상 상태 구간(`phase="steady"`)을 **여러 실행에 걸쳐 누적 3시간 이상** 모은 지표별 p50 · p95 · p99 분포. GPU 4장(한 클러스터)이면 충분. 켠 직후 워밍업 10~15분과 주입 구간은 제외. 실행 횟수 · 누적 시간을 함께 기록 | 4주 첫 분포, 6주 재계산 (3단계) | GPU 사용률, 스텝 시간, 체크포인트 저장 시간 |
| **실험** | 부하 · 장애를 일부러 주입해 "어디서 스텝 시간이 꺾이나" 확인한 값 | 5주 (4단계) | stress-ng CPU, `nvidia-smi -lgc` 클럭 제한, tc netem, VPN 단절, dcgmi 주입 |
| **논문 · 트레이스** | 공개 장애 트레이스 · 논문의 빈도 · 분포 | 발표 근거용, 임계값에는 보조 | Llama 3 54일 419회 중단, Philly / Acme 장애 분포 |
| **운영 경험** | exporter 기본 룰 · 커뮤니티 룰 · 일반 운영 관행. 근거가 약하므로 기준선 · 실험으로 대체 대상 | 지금부터, 교체 예정 표시 | postgres_exporter 기본 룰, `absent(up)` 2분, 디스크 80/90% |

기준선을 누적으로 모을 수 있게 학습 Job 지표에 `phase` 라벨(`warmup` = step ≤ warmup_steps · `steady`)을 붙인다 (학습 Job 계측 규약, 노찬경에게 11/05 전달). 주입 구간은 학습 코드가 모르고, 측정 측이 Kafka `fault.injection` 사건의 시각으로 제외한다 (2026-11-05 변경: 이전 `injected` 라벨 계획 폐기). 모델 · 배치 크기 · 데이터셋이 바뀌면 기준선은 다시 모은다.

## 3. 표 열 정의

| 열 | 규칙 |
|---|---|
| 번호 | `MON-STD-nn` / `SLO-n`. 한 번 붙인 번호는 **재사용 · 재배열 금지**(룰 annotation · 런북 · 패널이 참조). 삭제된 행은 번호를 남기고 "폐기"로 표시 |
| 대상 | GPU · 노드 · 학습 Job · 수집 · 알람 · 네트워크/사이트 · 서비스 · 모니터링 서버 · 플랫폼 |
| 지표 | 실제 지표명 또는 LogQL 대상. 이름이 없는 것은 "계측 필요" |
| 정상 / 경고 / 위험 | **확정된 값만**. 비어 있으면 아직 측정 전 |
| 가설 | 로드맵 · 대화에서 나온 미확정 숫자. 실험의 출발점. 확정되면 값 열로 옮기고 비움 |
| 근거 출처 | 2절의 종류. 확정 뒤에는 링크(측정 노트 · 문서 URL) 추가 |
| 조치 | 이벤트 JSON의 `suggested_action` 8개 값 중 하나, 또는 런북 이름. 자유 문장 금지 |
| 상태 | `비움` · `가설` · `측정 중` · `확정` · `측정 못 함` |

## 4. 기준표

### 4.1 GPU

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-01 | GPU | 온도 `DCGM_FI_DEV_GPU_TEMP` | | | | 경고 Slowdown Temp − 10 · 위험 Slowdown Temp | 벤더 (`nvidia-smi -q -d TEMPERATURE`) + 기준선 | 클럭 제한 사유 확인 → MON-STD-02 · 08 | 가설 |
| MON-STD-02 | GPU | 클럭 제한 사유 `DCGM_FI_DEV_CLOCKS_EVENT_REASONS` | | | | 정상 0 · 경고 thermal/power 비트 · 위험 HW slowdown 비트 | 벤더 (DCGM 필드 문서) | `evacuate_preemptive` 입력 (스트래글러) | 가설 |
| MON-STD-03 | GPU | ECC SBE 증가율 `DCGM_FI_DEV_ECC_SBE_VOL_TOTAL` · `DCGM_FI_DEV_RETIRED_PENDING` | | | | 정상 0/h · 위험 Xid 63 기록 또는 RETIRED_PENDING=1 | 벤더 (XID 문서) + 논문 · 트레이스. SBE 빈도는 15장 · 수 시간으로는 분포가 안 나와 기준선 측정 대상 아님 | `evacuate_preemptive` | 가설 |
| MON-STD-04 | GPU | XID. 메인 = journald 커널 로그 `NVRM: Xid` (Loki ruler) · 보조 = `DCGM_FI_DEV_XID_ERRORS` (필드 230, 수집 5초) | | | | 경고 앱 코드 Xid 1회 · 위험 HW Xid 1회 | 벤더 (NVIDIA XID Errors 원인 열) | 분류기 룰 테이블 → [fault-event.md 3절](fault-event.md) | 가설 |
| MON-STD-05 | GPU | 사용률 · 메모리 `DCGM_FI_DEV_GPU_UTIL` · `DCGM_FI_DEV_FB_USED` (학습 중) | | | | 경고 사용률 p50 − 2σ 이하 5분 | 기준선 | 데이터 공급 병목 점검 런북 | 가설 |
| MON-STD-17 | GPU | NVML 사이드카: NVML 호출 지연 히스토그램 · `nvmlDeviceRegisterEvents` XID 카운터 (계측 필요) | | | | | 기준선 | `collector_healthy` 판단 입력 (지연 급증 = 드라이버 응답 이상) | 비움 |

### 4.2 노드

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-06 | GPU 노드 | CPU (g4dn.xlarge 4 vCPU) `node_cpu_seconds_total` | | | | 경고 80% 5분 · 위험 95% | 실험 (stress-ng로 스텝 시간 꺾이는 점) | 데이터 로더 워커 수 조정 런북 | 가설 |
| MON-STD-07 | GPU 노드 | 메모리 · 디스크 · 네트워크 `node_memory_*` · `node_filesystem_*` · `node_network_*` | | | | 위험 디스크 90% | 기준선 + 운영 경험 | 체크포인트 로컬 쓰기 실패 예방 런북 | 가설 |
| MON-STD-16 | CPU 노드 | Prometheus Agent WAL 디스크 · `prometheus_agent_*` (WAL 경로 `node_filesystem_avail_bytes`) | | | | WAL 보존 상한 4h = VPN 단절 복구 상한 | 벤더 (Agent WAL 기본 보존 max 4h) + 실험 (VPN 단절) | VPN 점검 런북 | 가설 |
| MON-STD-24 | Proxmox 호스트 (devoops · pve1~4) | 호스트 OS node-exporter (`job="pve-host"`, 중앙이 LAN 직접 pull): `up` · 루트 디스크 `node_filesystem_avail_bytes` · 온도 `node_hwmon_temp_celsius` (CPU `chip=~"platform_coretemp.*"` / NVMe `chip=~"nvme.*"` 분리) | | | | 호스트 다운 5분 (`up==0`) · 디스크 경고 80% · 위험 90% · **CPU 온도 경고 80°C `for: 5m`** (pve4에 15분 주기 1샘플 60~88°C 스파이크가 있어 인스턴트 값 금지) · **NVMe 온도 경고 70°C** | 운영 경험 + 벤더 (CPU Tjmax 100°C · NVMe 스로틀 70~80°C) + 기준선 ([2026-11-06 1차 측정](measurements/2026-11-06-pve-host-temp.md): 덮개 닫힘 CPU 평균 31~40°C · NVMe 21~58°C, 덮개 열림 재측정 예정) | 호스트 다운 = 그 위 VM(master·worker·monitoring) 전부 영향 → 인프라 담당 호출 런북 | 가설 |

### 4.3 학습 Job

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-08 | 학습 Job | 스트래글러: 랭크별 GPU 계산 시간 `train_rank_compute_seconds` ÷ 랭크 값들의 중앙값 (`quantile by (job_id, generation)`, `on(...) group_left`). 보조 = `train_data_wait_seconds` 같은 식(노드 쪽 원인). **스텝 시간은 FSDP 동기화로 랭크 간 같아 쓰지 않음** ([training-metrics.md 3-1절](training-metrics.md)) | | | | 정상 1.0 ± 측정 · 경고 1.5× 2분 · 위험 2.0×. 분자·분모는 같은 분위수(p50÷p50). A5 promtool 테스트: 정상 랭크 1.00 · 주입 랭크 3.08 | 실험 (클럭 제한 단계별 캘리브레이션) | `evacuate_preemptive` (스트래글러 격리). data_wait만 튀면 노드 문제 → 컨트롤러 힌트 | 가설 |
| MON-STD-09 | 학습 Job | 체크포인트 저장 시간 · 마지막 저장 경과 (학습 Pod 계측) | | | | 경고 저장 주기 × 1.5 · 위험 × 3 | 기준선 (김현수 체크포인트 모듈) | 손실 스텝 상한 런북 | 가설 |
| MON-STD-20 | 학습 Job | Pod 종료 코드 `kube_pod_container_status_last_terminated_reason` · `_exitcode`. **exit 75 = STOP(세대 불일치, 펜싱 정상 동작)** · exit 0 · 사용자 취소는 제외 (ADR-12) | | | | | 운영 경험 (kube-state-metrics 문서) | 분류기 입력 (`restart_in_place` vs `refuse` 구분) | 비움 |

### 4.4 수집 파이프라인

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-10 | 수집 | Agent remote_write 큐 · 샘플 누락 `prometheus_remote_storage_*` · Loki ingestion 지연 | | | | 정상 0 누락 · 경고 큐 지연 30s · 위험 5분 | 실험 (VPN 단절) | VPN 점검 런북 · `collector_healthy=false` → `hold` | 가설 |
| MON-STD-15 | 수집 | 클러스터별 Agent 생존 `absent_over_time(up{job="prometheus-agent", cluster="<c>"}[2m])`, 클러스터마다 룰 1개. `absent()`는 Agent 사망 시 stale 마커가 없어 lookback 5분 뒤에야 참(A5 테스트 9m15s vs 6m15s) → 쓰지 않음 | | | | 경고 2분 (= 범위 2m, `for` 없음) | 운영 경험 (교체 예정: 실험) | 수집 중단 판정 → 해당 클러스터 `collector_healthy=false` | 가설 |

### 4.5 알람 · 분류

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-11 | 알람 | 감지 지연 (주입 → 컨트롤러의 Kafka `gpu.fault` 소비, Tempo span) | | | | 경고 p95 측정값 × 1.5 | 실험 (4주 감지 지연 표) | 룰 `for` · Alertmanager `group_wait` 조정 | 가설 |
| MON-STD-21 | 매니저 | Kueue 대기 Workload 수 `kueue_pending_workloads` · 컨트롤러 결정 지연 (`decide` span) | | | | | 기준선 + 트레이스 (Tempo) | 스케줄링 (김성모) · 컨트롤러 (한송이) 호출 런북 | 비움 |

### 4.6 네트워크 · 사이트

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-18 | 사이트 | 독립 경로(공인 엔드포인트 경유): DynamoDB `devoops-job-fencing.heartbeat_at` 경과 · `devoops-job-progress` 갱신 경과 (분류기가 폴링해 지표로 노출, 계측 필요) + EC2 `DescribeInstanceStatus` + VPN 경로 `up`. blackbox_exporter는 터널 내부 IP · pfSense · 공인 DNS 확인용 | | | | 위험: VPN `up==0` 2분 + 하트비트 경과 > 하트비트 주기 × 3 | 운영 경험 + 실험 (tc netem · VPN 단절 · FIS AZ 중단) | 판정 재료: VPN 끊김 + 하트비트도 끊김 → `fence_evacuate` (SiteDown) / VPN만 끊기고 하트비트 계속 → `hold` (VpnDown). 워커는 프라이빗 서브넷 · EKS API 비공개라 공인망에서 직접 probe 불가 (2026-11-05 정정) | 가설 |
| MON-STD-19 | 인프라 | 스팟 중단 통보 (aws-node-termination-handler 또는 IMDS `instance-action`) | | | | 통보 즉시 위험 (회수까지 2분) | 벤더 (AWS 스팟 2분 규칙) | `evacuate_preemptive` | 가설 |

### 4.7 서비스 층

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-12 | Job API | RED: 5xx 비율 · 지연 p95 (Backend 계측, 노찬경) | | | | 경고 5xx 1% · p95 측정 × 2 · 위험 5xx 5% 5분 | 기준선 (6주) | Backend 재시작 런북 (노찬경) | 가설 |
| MON-STD-13 | PostgreSQL | 연결 수 · 디스크 · `pg_up` (postgres_exporter, 별도 VM) | | | | 경고 연결 80% · 디스크 80% · 위험 up==0 1분 | 운영 경험 (postgres_exporter 기본값) + 기준선 | DB 런북 (김현수) | 가설 |
| MON-STD-14 | 결과 제공 | 결과물 업로드 실패 · 다운로드 오류 (학습 Job 카운터 · Backend 로그) | | | | 경고 1회 · 위험 연속 3회 | 운영 경험 | S3 권한 · 네트워크 점검 런북 | 가설 |
| MON-STD-23 | Kafka (172.30.0.40, 2주차 도입 ADR-7 · 8) | 브로커 지표 JMX Exporter(김현수) + consumer lag `kafka_consumergroup_lag` (kafka_exporter, 모니터링 VM 컨테이너). 그룹: 복구 컨트롤러 · `collector` | | | | 경고 lag 증가 5분 · 위험 브로커 `up==0` 1분 (브로커 1대, 복제 없음) | 운영 경험 (kafka_exporter 기본 룰) | 데이터 담당 호출 런북. 컨트롤러 lag 증가 = 복구 지연 | 가설 |

### 4.8 모니터링 서버 자체

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-22 | 모니터링 클러스터 (11/09 VM → k8s 3노드, ADR-18) | 노드 디스크 (Mimir · Loki · Tempo PVC, 30일 보존) `MonitoringNodeDisk*` · Alertmanager Watchdog 데드맨 수신 · 스크레이프 대상 `MonitoringTargetDown` · 매니저 CronJob 프로브 | | | | 디스크 경고 80% · 위험 90% · Watchdog 미수신 5분 | 운영 경험 + 용량 계산 (6주 10GB 미만 추정) | 모니터링 클러스터 복구 런북 `deploy/README.md` (부 담당이 재설치 가능하게) | 가설 |

### 4.9 플랫폼 SLO

| 번호 | 대상 | 지표 (SLI) | 목표 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|
| SLO-1 | 플랫폼 | 감지 지연 p95 (주입 → 컨트롤러 Kafka 소비) | | 4~5주 측정 뒤 결정 | 실험 | burn-rate 알람 | 비움 |
| SLO-2 | 플랫폼 | 수집 가용성 (`up` · 샘플 누락) | | 99.5% | 기준선 | burn-rate 알람 | 가설 |
| SLO-3 | 플랫폼 | 알람 정확도 (주입 장애 vs 분류 결과 혼동 행렬) | | 95% | 실험 (6주 혼동 행렬) | 분류기 룰 수정 | 가설 |
| SLO-4 (후보) | 서비스 | Job API 가용성 (blackbox + 5xx) | | 99% | 기준선 (6주) | 서비스가 나오면 활성 | 가설 |

## 5. 룰 · 런북 · 대시보드 추적표 (빈 뼈대)

6단계 검증용. 표준의 모든 행이 룰 · 런북 · 패널과 1:1인지 여기서 확인한다. 빠진 행은 "측정 못 함"으로 솔직히 적는다.

| 번호 | 룰 이름 (`alert:` / Loki ruler) | 런북 | 대시보드 패널 | 확인일 |
|---|---|---|---|---|
| MON-STD-01 ~ 24 | | | | |
| SLO-1 ~ 4 | | | | |

## 6. 채우는 순서와 변경 규칙

| 단계 (로드맵) | 채워지는 열 |
|---|---|
| 2 규약 (2~3주) | 조치 열의 런북 이름, 룰 annotation `standard: MON-STD-nn`, 추적표의 룰 이름 |
| 3 기준선 (4~6주 누적) | 정상 열 (근거 = 기준선). 4주에 첫 분포, 6주에 누적분으로 재계산 |
| 4 임계값 (5주) | 경고 · 위험 열 (근거 = 실험). 확정되면 가설 열 비우고 상태 → 확정 |
| 5 SLO (5~6주) | SLO 행 목표 |
| 6 검증 (6주) | 추적표 전 행, 상태 `측정 못 함` 정리 |

- 번호는 재사용 · 재배열하지 않는다. 행 추가는 다음 번호(현재 25).
- 값을 바꾸면 근거 링크를 같이 바꾼다. 근거 없는 변경은 가설 열로만.
- 룰 annotation `standard` 값이 이 표에 없으면 CI에서 실패시킨다 (2단계에서 promtool 테스트에 추가).
