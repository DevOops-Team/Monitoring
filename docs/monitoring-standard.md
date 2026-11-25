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

기준선을 누적으로 모을 수 있게 학습 Job 지표에 `phase` 라벨(`warmup` = step ≤ warmup_steps · `steady`)을 붙인다 (학습 Job 계측 규약, 노찬경에게 11/05 전달). 주입 구간은 학습 코드가 모르고, 측정 측이 런북(`deploy/README.md`) 「장애 주입」 시각 기록표로 제외한다: **T0 주입 ~ (주입 해제와 T5 `job.resumed` 중 늦은 쪽)** (2026-11-05 변경: 이전 `injected` 라벨 계획 폐기. 2026-11-20 변경: Kafka `fault.injection` 토픽 삭제 — 주입은 사람이 CLI로 하고(ADR-33) 그 사건을 내는 곳이 없음). 모델 · 배치 크기 · 데이터셋이 바뀌면 기준선은 다시 모은다.

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

GPU 지표 이름은 **dcgm-exporter 4.8.4** 기준이다 (2026-11-16 결정: 4.8.4 고정). 4.9.0부터 기본 지표 23개의 이름과 11개의 단위가 바뀌므로(예: `DCGM_FI_DEV_GPU_TEMP` → `DCGM_FI_DEV_GPU_TEMP_CELSIUS`, 사용률 % → 0~1) 버전을 올리면 이 절을 같이 고친다.

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-01 | GPU | 온도 `DCGM_FI_DEV_GPU_TEMP`. 한계 = `DCGM_FI_DEV_GPU_MAX_OP_TEMP` · `DCGM_FI_DEV_SLOWDOWN_TEMP` (커스텀 CSV에서 켬) | | | | 경고 Max Operating Temp 이상 · 위험 Slowdown Temp 이상. 숫자를 룰에 적지 않고 GPU가 보고하는 한계 지표와 비교 (T4 실제 값은 GPU 노드에서 확인) | 벤더 (nvidia-smi 문서 Temperature: Max Operating = 소프트웨어가 클럭을 낮추기 시작, Slowdown = 하드웨어가 낮춤, Shutdown = 꺼짐) + 기준선 | 클럭 제한 사유 확인 → MON-STD-02 · 08 | 가설 |
| MON-STD-02 | GPU | 클럭 제한 사유 `DCGM_EXP_CLOCK_EVENTS_COUNT` (사유별 시계열, 커스텀 CSV에서 켬). 보조 = 누적 감속 시간 `DCGM_FI_DEV_THERMAL_VIOLATION` · `DCGM_FI_DEV_POWER_VIOLATION` (ns, T4 지원 여부 확인 필요) | | | | 정상 = 감속 사유 없음 (Idle 사유는 무시: 학습이 없는 GPU는 항상 켜짐) · 경고 SW Power Cap · SW Thermal · 위험 HW Slowdown (HW Thermal · HW Power Brake가 같이 켬). 사유 라벨 값은 GPU 노드에서 확인 | 벤더 (nvidia-smi 문서 Clocks Event Reasons: HW Slowdown = 코어 클럭 2배 이상 감소) | `evacuate_preemptive` 입력 (스트래글러) | 가설 |
| MON-STD-03 | GPU | ECC SBE 증가율 `DCGM_FI_DEV_ECC_SBE_AGG_TOTAL` (volatile `_VOL_`은 드라이버 재로드 때 0으로 돌아가 쓰지 않음) · `DCGM_FI_DEV_RETIRED_PENDING` | | | | 정상 0/h · 위험 Xid 63 기록 또는 RETIRED_PENDING=1 | 벤더 (XID 문서, nvidia-smi 문서 Page Retirement: 폐기됐지만 아직 차단되지 않은 페이지는 여전히 할당될 수 있다) + 논문 · 트레이스. SBE 빈도는 15장 · 수 시간으로는 분포가 안 나와 기준선 측정 대상 아님 | `evacuate_preemptive` | 가설 |
| MON-STD-04 | GPU | XID. 메인 = journald 커널 로그 `NVRM: Xid` (Loki ruler) · 보조 = `DCGM_FI_DEV_XID_ERRORS` (필드 230, 수집 5초) | | | | 경고 앱 코드 Xid 1회 · 위험 HW Xid 1회 | 벤더 (NVIDIA XID Errors 원인 열) | 분류기 룰 테이블 → [fault-event.md 3절](fault-event.md) | 가설 |
| MON-STD-05 | GPU | 사용률 · 메모리 `DCGM_FI_DEV_GPU_UTIL` · `DCGM_FI_DEV_FB_USED` (학습 중) | | | | 경고 사용률 p50 − 2σ 이하 5분 | 기준선 | 데이터 공급 병목 점검 런북 | 가설 |
| MON-STD-17 | GPU | NVML 사이드카: NVML 호출 지연 히스토그램 · `nvmlDeviceRegisterEvents` XID 카운터 (계측 필요) | | | | | 기준선 | `collector_healthy` 판단 입력 (지연 급증 = 드라이버 응답 이상) | 비움 |

### 4.2 노드

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-06 | GPU 노드 | CPU (g4dn.xlarge 4 vCPU · g4dn.2xlarge 8 vCPU, ADR-25. 사용률은 비율이라 임계값은 공통, 기준선은 타입별로 `kube_node_labels` 조인) `node_cpu_seconds_total` | | | | 경고 80% 5분 · 위험 95% | 실험 (`stress-ng --cpu 0 --cpu-load N`으로 스텝 시간 꺾이는 점. `--cpu-load`는 워커당 비율이라 x축은 설정값이 아니라 실제 사용률) | 데이터 로더 워커 수 조정 런북 | 가설 |
| MON-STD-07 | GPU 노드 | 메모리 · 디스크 · 네트워크 `node_memory_*` · `node_filesystem_*` · `node_network_*` | | | | 위험 디스크 90% | 기준선 + 운영 경험 | 체크포인트 로컬 쓰기 실패 예방 런북 | 가설 |
| MON-STD-16 | CPU 노드 | Prometheus Agent WAL 디스크 · `prometheus_agent_*` (WAL 경로 `node_filesystem_avail_bytes`) | | | | WAL 보존 상한 4h = VPN 단절 복구 상한 | 벤더 (Agent WAL 기본 보존 max 4h) + 실험 (VPN 단절) | VPN 점검 런북 | 가설 |
| MON-STD-24 | Proxmox 호스트 (devoops · pve1~4) | 호스트 OS node-exporter (`job="pve-host"`, 모니터링 클러스터 Agent가 LAN 직접 pull, 설치 · 설정은 `ansible/roles/node_exporter`): `up` · 루트 디스크 `node_filesystem_avail_bytes` · 온도 `node_hwmon_temp_celsius` (CPU `chip=~"platform_coretemp.*"` / NVMe `chip=~"nvme.*"` 분리) | `up` 5/5 · 루트 디스크 사용 12~53% (devoops 53%, 나머지 12~18%) · **CPU 온도 24h 평균 38~49°C · p95 41~53°C** (호스트당 VM 2~3대 가동 기준, 덮개 개폐는 ±2°C로 무관) · **NVMe 평균 22~53°C** (devoops만 50°C대, VM 4대 디스크 쓰기) | | | | 호스트 다운 5분 (`up==0`) · 디스크 경고 80% · 위험 90% · **CPU 온도 경고 80°C · 위험 90°C**, 평가는 `avg_over_time([5m])` 또는 `for: 5m` (pve1 · pve3에 하루 19회 1샘플 60~92°C 스파이크가 남아 인스턴트 값 금지) · **NVMe 온도 경고 70°C** | 벤더 (CPU Tjmax 100°C · NVMe 스로틀 70~80°C) + 기준선 **2회 누적 48h** ([1차 2026-11-06 덮개 닫힘 24h](measurements/2026-11-06-pve-host-temp.md) · 2차 2026-11-09~10 덮개 열림 24.1h, 같은 노트 7절) + 운영 경험 (11/02~11/10 pve4 15분 주기 스파이크 = pve4에만 깔린 `prometheus-node-exporter-collectors` 15분 타이머, 11/10 Ansible로 제거 → 11/11 재부팅 뒤에도 0회로 **확정**, 같은 노트 7-4) | 호스트 다운 = 그 위 VM(master·worker·monitoring·kafka·pfsense) 전부 영향 → 인프라 담당 호출 런북 | 측정 중 (정상 확정, 경고 · 위험은 가설) |
| MON-STD-26 | Kafka VM 3대 (.40 ~ .42) · DB VM (.50) · pfSense 2대 (.2 · .3) | 호스트 OS node-exporter (`job="vm-host"` · `job="pfsense-host"`, 모니터링 클러스터 Agent가 LAN 직접 pull, 설치는 `ansible/playbooks/service-vms.yml` · `pfsense.yml`): `up` · 루트 디스크 `node_filesystem_avail_bytes{mountpoint="/"}` (Kafka 로그 · PostgreSQL 데이터가 모두 루트에 있다) | | | | 디스크 경고 80% · 위험 90%, 5분 (룰 `HostDiskWarning` · `HostDiskCritical`. 2026-11-18 사용 3 ~ 7%) · 호스트 다운 5분은 `MonitoringTargetDown` | 운영 경험 (MON-STD-22 · 24와 같은 기준) | Kafka · DB VM = 데이터 담당, pfSense = 인프라 담당 호출 런북 | 가설 |

### 4.3 학습 Job

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-08 | 학습 Job | 스트래글러: 랭크별 GPU 계산 시간 `train_rank_compute_seconds` ÷ 랭크 값들의 중앙값 (`quantile by (job_id, generation)`, `on(...) group_left`). 보조 = `train_data_wait_seconds` 같은 식(노드 쪽 원인). **스텝 시간은 FSDP 동기화로 랭크 간 같아 쓰지 않음** ([training-metrics.md 3-1절](training-metrics.md)) | | | | 정상 1.0 ± 측정 · 경고 1.5× 2분 · 위험 2.0×. 분자·분모는 같은 분위수(p50÷p50). A5 promtool 테스트: 정상 랭크 1.00 · 주입 랭크 3.08 | 실험 (클럭 제한 단계별 캘리브레이션) | `evacuate_preemptive` (스트래글러 격리). data_wait만 튀면 노드 문제 → 컨트롤러 힌트 | 가설 |
| MON-STD-09 | 학습 Job | 체크포인트 저장 시간 · 마지막 저장 경과 (학습 Pod 계측) | | | | 경고 저장 주기 × 1.5 · 위험 × 3 | 기준선 (김현수 체크포인트 모듈) | 손실 스텝 상한 런북 | 가설 |
| MON-STD-20 | 학습 Job | Pod 종료 코드 `kube_pod_container_status_last_terminated_reason` · `_exitcode`. exit 0 · 펜싱 · 사용자 취소는 제외. 펜싱 · 취소는 종료 코드가 아니라 `job.lifecycle`로 구분 (ADR-43, 옛 exit 75는 없어짐) | | | | | 운영 경험 (kube-state-metrics 문서) | 분류기 입력 (`restart_in_place` vs `refuse` 구분) | 비움 |
| MON-STD-28 | 학습 Job (ckpt-sidecar 펜싱 하트비트, ADR-44) | 경과 `recovery_fencing_heartbeat_age_seconds{job_id, generation}` · 조회 상태 `recovery_fencing_heartbeat_scrape_success` (복구 컨트롤러 :8080. Pod 3개가 각자 30초마다 `devoops-prod-fencing`을 Scan. 조회 실패 · 60초 넘은 스냅샷이면 success 0이고 age를 내지 않는다). 하트비트는 **rank 0 Pod의 사이드카만** 10초마다 쓴다 → rank 1 ~ 3 노드 장애는 이 지표로 못 본다(Pod 종료 · Xid 몫). **찍기와 읽기는 경로가 다르다**: 찍기 = EKS → DynamoDB Gateway 엔드포인트(AWS 안), 읽기 = 온프레 → pfSense WAN 인터넷 → 온프레 인터넷 장애는 「못 읽음」이지 「멈춤」이 아니다. 학습 중 판정 = trainer가 Running인 Pod의 `label_devoops_job_id` · `label_devoops_generation` (끝난 작업 · 옛 세대 항목은 테이블에 남아도 제외) | | | | 경고 경과 > 60초가 1분 (룰 `RecoveryHeartbeatStale`. 60초 = 하트비트 10초 × 6, 정상도 10초 + Scan 30초로 최대 약 40초) · 경고 컨트롤러 전부 조회 실패 10분 (룰 `RecoveryHeartbeatReadFailing`. 이 동안 멈춤 알람이 꺼져 있다) | ADR-44 (교체 예정: EKS 정상 구간 경과 실측 · 조회 실패 간격 `read_errors_total`) | 하트비트 멈춤 = 데이터 (김현수) · 조회 실패 = 컨트롤러 (한송이) 호출 런북. `gpu.fault`에는 연결하지 않는다 (ADR-44) | 가설 |
| MON-STD-30 | 학습 Job (진행) | 현재 step `train_current_step{job_id, generation, rank}` (trainer :8000)이 늘고 있는가. 대상 = trainer가 Running인 Pod의 `label_devoops_job_id` · `label_devoops_generation` (MON-STD-28과 같은 판정). 랭크 하나라도 늘면 진행으로 본다. 지표가 아예 안 들어오는 것도 「진행 없음」이다 (복구 뒤 Pod는 전부 Ready인데 학습은 안 도는 경우). **한계**: EKS 수집이 통째로 끊기면(VPN · Agent) 대상도 같이 사라져 안 울린다(사이트 알람 몫) | trainer 시작 뒤 15 ~ 30초 안에 첫 step 증가 · 도중 정체 0초 (2026-11-25 정상 작업 5개, 15초 간격으로 봄) | 진행 없음 10분 (룰 `TrainingStalled`. 「최근 2분 안에 step 변화 없음」이 10분 이어짐) | | 10분 → 5분으로 줄일 수 있는지: 큰 모델 로딩 · 체크포인트 저장이 긴 작업에서 정체가 얼마나 가는지 실측 뒤 | 운영 경험 (2026-11-25 주입 뒤 세대 2 두 개: trainer는 8 ~ 10분 Running인데 step 지표 0건) + 기준선 1회 (같은 날 정상 작업 5개) | 사람 확인 (Slack). `gpu.fault`에는 연결하지 않는다. 볼 것은 `deploy/README.md` 「알람이 울리면」 | 가설 |

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
| MON-STD-25 | 복구 컨트롤러 | HOLD 판정 수 `recovery_hold_total{job_id, reason}` (platform/recovery-controller :8080, 새 HOLD 1회당 +1, 전역 Watchdog HOLD는 `job_id=""`). 첫 HOLD는 시계열이 값 1로 태어나 `increase()`가 0 → 룰은 증가 **또는** 새 시계열(직전 1시간 샘플 없음) **또는** 최근 1시간 안에 재시작한 프로세스(`process_start_time_seconds`)의 새 시계열. **한계**: HOLD 뒤 수집 주기(15초) 안에 컨트롤러가 죽으면 알람 없음(지표 방식의 한계, 기록은 DynamoDB · Kafka `recovery.held`에 남음) · 알람은 발생만 알리고 미처리 건수는 모름 → 둘 다 Grafana HOLD 목록 패널로 보완 | | | | 위험 새 HOLD 1건 (`for` 없음, 약 5분 발화. 꺼짐 = 해결 아님 → resolved 알림 안 보냄) | 운영 경험 (11/11 회의 결정: HOLD는 전부 사람 확인) | 컨트롤러 (한송이) 호출 런북. `reason`별 대응은 컨트롤러 문서 | 가설 |
| MON-STD-29 | 복구 컨트롤러 자체 (관리 클러스터 `platform/recovery-controller`, Pod ×3) | Ready 수 `kube_deployment_status_replicas_available` (KSM) · 재시작 `kube_pod_container_status_restarts_total` (Pod 3개의 15분 증가 합) · `gpu.fault` 소비 지연 `kafka_consumergroup_lag{consumergroup="recovery-controller-v1", topic="gpu.fault"}` (kafka_exporter, 커밋한 적 없는 파티션의 -1은 제외). **한계**: 소비 지연은 kafka_exporter가 죽으면 같이 안 보인다(`MonitoringTargetDown` 몫) | Ready 3 · 재시작 0 · 소비 지연 0 (2026-11-25, 사건 하나는 몇 초 안에 읽는다) | 15분 동안 재시작 합계 3번 이상 (룰 `RecoveryControllerRestarting`. 3 = Pod 3개가 한 바퀴. 버전 교체는 새 Pod라 안 는다) | Ready 0개 2분 (룰 `RecoveryControllerDown`) · `gpu.fault` 소비 지연 > 0이 5분 (룰 `RecoveryControllerStuck`) | 5분 · 3번은 사고 기록에 맞춘 값 → 실제 복구가 도는 동안의 소비 지연 · 재시작을 본 뒤 조정 | 운영 경험 (2026-11-24 ~ 25 사고 기록: 못 읽는 사건 하나로 17시간 동안 15분마다 재시작 6 ~ 9번 · 소비 지연 1 · 11/25 11:00 CrashLoop로 Ready 0. 세 경우 모두 알람이 없었다) | 컨트롤러 (한송이) 호출 런북 `deploy/README.md` 「알람이 울리면」. 컨트롤러가 멈추면 `gpu.fault`가 나가도 복구가 시작되지 않는다 | 가설 |

### 4.6 네트워크 · 사이트

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-18 | 사이트 | VPN 터널을 우회하는 별도 경로(공인 엔드포인트 경유. pfSense · 회선은 VPN과 공유, 물리적 독립 아님): DynamoDB `devoops-job-fencing.heartbeat_at` 경과 · `devoops-job-progress` 갱신 경과 (분류기가 폴링해 지표로 노출, 계측 필요) + EC2 `DescribeInstanceStatus` + VPN 경로 `up`. blackbox_exporter는 터널 내부 IP · pfSense(`172.30.0.1`) · 공인 DNS(`1.1.1.1`) 확인용 | | | | 위험: VPN `up==0` 2분 + **조회에 성공한** 하트비트의 경과 > 하트비트 주기 × 3 | 운영 경험 + 실험 (tc netem · VPN 단절 · FIS AZ 중단) | 판정 재료: VPN 끊김 + 조회 성공 + 하트비트 멈춤 → `fence_evacuate` (SiteDown) / VPN만 끊기고 하트비트 계속 → `hold` (VpnDown) / VPN 끊김 + **DynamoDB 조회 실패** → AWS 상태를 알 수 없으므로 `hold`: 공인 DNS ping도 실패하면 온프레 인터넷 장애, 성공하면 AWS 쪽 의심. 인터넷 복구 뒤 하트비트를 다시 읽어 재판정. 워커는 프라이빗 서브넷 · EKS API 비공개라 공인망에서 직접 probe 불가 (2026-11-05 정정). 온프레 → DynamoDB가 VPN이 아닌 pfSense WAN 인터넷 경로임은 2026-11-15 박기현 확인(AWS VPN 삭제 상태, 재구축 뒤 재확인 예정) | 가설 |
| MON-STD-19 | 인프라 | 스팟 중단 통보 (aws-node-termination-handler 또는 IMDS `instance-action`) | | | | 통보 즉시 위험 (회수까지 2분) | 벤더 (AWS 스팟 2분 규칙) | `evacuate_preemptive` | 가설 |

### 4.7 서비스 층

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-12 | Job API | RED: 5xx 비율 · 지연 p95 (Backend 계측, 노찬경) | | | | 경고 5xx 1% · p95 측정 × 2 · 위험 5xx 5% 5분 | 기준선 (6주) | Backend 재시작 런북 (노찬경) | 가설 |
| MON-STD-13 | PostgreSQL (DB VM 172.30.0.50, 데이터베이스 `gpuf_data`) | `pg_up` · 연결 수 `pg_stat_activity_count` ÷ `pg_settings_max_connections` (postgres_exporter, 모니터링 클러스터 Pod ×1, `job="postgres-exporter"`). VM 디스크는 MON-STD-26 | | | | 위험 `pg_up == 0` 2분 (룰 `PostgresDown`) · 경고 연결 80% (룰 없음, 대시보드 타일 색만) | 운영 경험 (postgres_exporter 기본값) + 기준선 | DB 런북 (김현수) | 가설 |
| MON-STD-14 | 결과 제공 | 결과물 업로드 실패 · 다운로드 오류 (학습 Job 카운터 · Backend 로그) | | | | 경고 1회 · 위험 연속 3회 | 운영 경험 | S3 권한 · 네트워크 점검 런북 | 가설 |
| MON-STD-23 | Kafka (브로커 3대 172.30.0.40 ~ .42, KRaft 컨트롤러 겸함, 2주차 도입 ADR-7 · 8) | 브로커 지표 JMX Exporter(김현수, :7071, `job="kafka-jmx"`) + consumer lag `kafka_consumergroup_lag` (kafka_exporter, 모니터링 클러스터 Pod ×1). 그룹: 복구 컨트롤러 · 수집기 `data-collector` | | | | 경고 `data-collector` 토픽별 lag 합 > 1000 10분 (룰 `CollectorLagHigh`. 1000은 임시값, 2026-11-18 평소 0 → 실험에서 평소 lag을 본 뒤 조정. 커밋한 적 없는 파티션의 -1은 제외) · 위험 브로커 `up==0` 1분 (2026-11-12 확인: 브로커 3대 · 파티션 복제 3벌. 「브로커 1대, 복제 없음」 전제로 정한 값이라 재검토 필요) | 운영 경험 (kafka_exporter 기본 룰) | 데이터 담당 호출 런북. 컨트롤러 lag 증가 = 복구 지연 | 가설 |

### 4.8 모니터링 서버 자체

| 번호 | 대상 | 지표 | 정상 | 경고 | 위험 | 가설 (미확정) | 근거 출처 | 조치 | 상태 |
|---|---|---|---|---|---|---|---|---|---|
| MON-STD-22 | 모니터링 클러스터 (11/09 VM → k8s 3노드, ADR-18) | 노드 디스크 (Mimir · Loki · Tempo PVC, 30일 보존) `MonitoringNodeDisk*` · Alertmanager Watchdog 데드맨 수신 · 스크레이프 대상 `MonitoringTargetDown` · 매니저 CronJob 프로브 | | | | 디스크 경고 80% · 위험 90% · Watchdog 미수신 5분 | 운영 경험 + 용량 계산 (6주 10GB 미만 추정) | 모니터링 클러스터 복구 런북 `deploy/README.md` (부 담당이 재설치 가능하게) | 가설 |
| MON-STD-27 | 분류기 (모니터링 클러스터 `monitoring/classifier`, Pod ×2 = 대장 1 + 대기 1) | 처리 중 `classifier_leader` (1 = Lease를 잡고 기억 복원을 끝낸 대장. Agent가 Pod를 찾아 긁는다. Pod가 없으면 시계열 자체가 없어 `absent(classifier_leader == 1)`로 본다) · 대장의 `classifier_lifecycle_healthy` (`job.lifecycle`을 읽고 있는가. 대기 쪽은 읽지 않아 늘 0이라 대장만 본다. 0이면 Pod 종료 신호가 전부 `hold`, 메시지 반영 실패로 0이 되면 재시작 전까지 안 돌아온다) | `up` 1 · `classifier_lifecycle_healthy` 1 (2026-11-19, Pod ×1 때) | | | 위험 대장 없음(Pod 없음 · 긁기 실패 포함) 3분 (룰 `ClassifierDown`. 3분 = 대장 교체 · Agent 재시작 구간에 안 울리게 둔 여유) · 위험 대장의 `classifier_lifecycle_healthy == 0` 2분 (룰 `ClassifierLifecycleUnhealthy`) | 운영 경험 (교체 예정: EKS 테스트에서 버전 올리기 · 브로커 재시작 때 끊기는 시간 실측) | 분류기 런북 `deploy/README.md` 「알람이 울리면」. 분류기가 멈추면 `gpu.fault`가 안 나가 자동 복구가 시작되지 않는다 | 가설 |

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
| MON-STD-06 · 07 | | | 「Kubernetes 노드」 CPU · 메모리 · 루트 디스크 · 네트워크 (지금은 온프레 노드만, EKS GPU 노드는 overlay 연결 뒤) | 2026-11-13 |
| MON-STD-10 | | | 「수집 파이프라인」 타일 전송 실패 · 전송 지연, 그래프 실패 샘플 · 전송 지연(30초 선) · 대기 샘플 | 2026-11-13 |
| MON-STD-15 | (`absent_over_time` 룰 미구현) | | 「수집 파이프라인」 타일 Agent | 2026-11-13 |
| MON-STD-16 | | | 「수집 파이프라인」 WAL 크기 | 2026-11-13 |
| MON-STD-18 | | | 「모니터링 스택」 ping 성공 여부 · 응답 시간 (pfSense · 인터넷만, VPN 대상은 AWS 재구축 뒤) | 2026-11-13 |
| MON-STD-20 | | | 「Kubernetes 워크로드」 마지막 종료 · 학습 Pod | 2026-11-13 |
| MON-STD-22 | `Watchdog` · `MonitoringNodeDiskWarning` · `MonitoringNodeDiskCritical` · `MonitoringTargetDown` (`deploy/base/mimir/rules/self.yml`) | | 「모니터링 스택」 타일 다운 대상 · Watchdog · 디스크 최대, 그래프 노드 루트 디스크 사용률(80 · 90% 선) | 2026-11-13 |
| MON-STD-13 | `PostgresDown` (`deploy/base/mimir/rules/infra.yml`, 테스트 `tests/infra_test.yml`) | `deploy/README.md` 「알람이 울리면」 | 「PostgreSQL」 전체 | 2026-11-18 |
| MON-STD-23 | `CollectorLagHigh` (`infra.yml`, 테스트 `tests/infra_test.yml`). 브로커 다운 룰은 미구현 | `deploy/README.md` 「알람이 울리면」 | 「Kafka 브로커」 전체 · 「수집기 lag」 행(1000 선) | 2026-11-18 |
| MON-STD-24 | | | 「Proxmox 호스트」 전체 (온도는 5분 평균, 80 · 90°C · NVMe 70°C 선) | 2026-11-13 |
| MON-STD-26 | `HostDiskWarning` · `HostDiskCritical` (`infra.yml`, 테스트 `tests/infra_test.yml`) | `deploy/README.md` 「알람이 울리면」 | 「Kafka · DB VM」 · 「pfSense」 타일 디스크 최대, 그래프 루트 디스크 사용률(80 · 90% 선) | 2026-11-18 |
| MON-STD-25 | `RecoveryHold` (`deploy/base/mimir/rules/recovery.yml`, 테스트 `tests/recovery_test.yml`) | | 「장애 복구」 타일 HOLD 누적, HOLD 목록(PostgreSQL 뷰 `data.v_recovery_held`) · HOLD 누적 (지표) | 2026-11-20 |
| MON-STD-27 | `ClassifierDown` · `ClassifierLifecycleUnhealthy` (`deploy/base/mimir/rules/classifier.yml`, 테스트 `tests/classifier_test.yml`) | `deploy/README.md` 「알람이 울리면」 | 「장애 복구」 타일 분류기, 그래프 받은 알람 · 발행한 사건 · 버린 것 · 상태 | 2026-11-20 |
| MON-STD-28 | `RecoveryHeartbeatStale` · `RecoveryHeartbeatReadFailing` (`deploy/base/mimir/rules/recovery.yml`, 테스트 `tests/recovery_test.yml`) | `deploy/README.md` 「알람이 울리면」 | 「장애 복구」 하트비트 경과(60초 선) · 하트비트 조회 | 2026-11-20 |
| MON-STD-29 | `RecoveryControllerDown` · `RecoveryControllerRestarting` · `RecoveryControllerStuck` (`deploy/base/mimir/rules/recovery.yml`, 테스트 `tests/recovery_test.yml`) | `deploy/README.md` 「알람이 울리면」 | 「장애 복구」 타일 컨트롤러 · 소비 지연, 그래프 Ready · 재시작 · 소비 지연 | 2026-11-25 |
| MON-STD-30 | `TrainingStalled` (`deploy/base/mimir/rules/training.yml`, 테스트 `tests/training_test.yml`) | `deploy/README.md` 「알람이 울리면」 | 「학습 작업」 타일 정체 작업(지표가 들어오는 작업만 센다), 그래프 현재 step · 체크포인트 step · 진행 속도 | 2026-11-25 |
| MON-STD-01 | | | 「GPU」 타일 온도 여유, 그래프 GPU 온도(한계 3단 점선) | 2026-11-20 |
| MON-STD-02 | | | 「GPU」 타일 감속 GPU, 그래프 감속 사유 · 감속 시간 비율 · SM 클럭 | 2026-11-20 |
| MON-STD-03 | | | 「GPU」 타일 ECC DBE · 폐기 대기, 그래프 ECC SBE 증가 · 페이지 폐기 | 2026-11-20 |
| MON-STD-04 | (Loki 룰 본문 미구현, GPU-218) · 보조 `FailoverXidDcgm` (`deploy/base/mimir/rules/failover.yml`) | | 「GPU」 타일 Xid GPU, 그래프 Xid · 커널 Xid 로그 (Xid가 난 적이 없어 비어 있음) | 2026-11-20 |
| MON-STD-05 | | | 「GPU」 GPU 사용률 · GPU 메모리 | 2026-11-20 |
| MON-STD-08 | | | 「학습 작업」 타일 스트래글러 · 데이터 대기, 그래프 스트래글러 비율(1.5 · 2.0 선) · 랭크 계산 · 통신 · 데이터 대기(0.1초 선) | 2026-11-20 |
| 나머지 09 · 11 · 12 · 14 · 17 · 19 · 21 | | | | |
| SLO-1 ~ 4 | | | | |

## 6. 채우는 순서와 변경 규칙

| 단계 (로드맵) | 채워지는 열 |
|---|---|
| 2 규약 (2~3주) | 조치 열의 런북 이름, 룰 annotation `standard: MON-STD-nn`, 추적표의 룰 이름 |
| 3 기준선 (4~6주 누적) | 정상 열 (근거 = 기준선). 4주에 첫 분포, 6주에 누적분으로 재계산 |
| 4 임계값 (5주) | 경고 · 위험 열 (근거 = 실험). 확정되면 가설 열 비우고 상태 → 확정 |
| 5 SLO (5~6주) | SLO 행 목표 |
| 6 검증 (6주) | 추적표 전 행, 상태 `측정 못 함` 정리 |

- 번호는 재사용 · 재배열하지 않는다. 행 추가는 다음 번호(현재 31).
- 값을 바꾸면 근거 링크를 같이 바꾼다. 근거 없는 변경은 가설 열로만.
- 룰 annotation `standard` 값이 이 표에 없으면 CI에서 실패시킨다 (2단계에서 promtool 테스트에 추가).
