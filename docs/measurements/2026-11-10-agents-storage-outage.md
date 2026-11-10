# 수집 층 유실 시험 — 저장소(Loki · Tempo) 2분 중단 (GPU-108)

- 측정: 2026-11-10 14:31 ~ 14:37 KST
- 구성: kind `agents-test`(1노드)에 `deploy/agents/overlays/kind`(Fluent Bit · OTel 게이트웨이 ×1, 큐는 hostPath) → 실제 모니터링 클러스터 저장소 `172.30.0.35`(Loki 3100 · Tempo 4317). 노트북 → Tailscale → LAN
- 방법: 모니터링 클러스터에서 `kubectl scale sts/loki sts/tempo --replicas=0`, 약 2.5분 뒤 `--replicas=1`. 그동안 kind에 5초마다 한 줄 찍는 Pod(`loss-test`)와 30초마다 테스트 span 1개(게이트웨이 4318로 직접). 게이트웨이 큐 길이는 `:8888` `otelcol_exporter_queue_size`
- 목적: "저장소가 죽어 있는 동안의 로그 · 트레이스는 게이트웨이 file_storage 큐가 지킨다"(deploy/agents 설계 전제, 11/10 결정) 확인. 스크립트는 세션 scratchpad `loss-test.sh`(레포 밖)

## 1. 타임라인

| 시각 (KST) | 일 | 큐 (Loki / Tempo) |
|---|---|---|
| 14:31:41 | 시작, 기준 span 1개 | 1 / 0 |
| 14:32:02 | Loki · Tempo scale 0 명령 | |
| 14:32:35 | 두 Pod 소멸 (graceful 종료 33초) | |
| 14:33:06 | span #1 | 12 / 0 |
| 14:33:36 | span #2 | 18 / 1 |
| 14:34:06 | span #3 | 24 / 2 |
| 14:34:36 | scale 1 명령 | |
| 14:35:20 | 두 Pod Ready (**중단 154초**) | 39 / 3 |
| 14:36:00 | 큐 0 (**복귀 뒤 40초에 전부 전송**) | 0 / 0 |

## 2. 결과

| 신호 | 중단 구간에 만들어진 양 | 저장소에 도착한 양 | 유실 |
|---|---|---|---|
| 로그 (`{k8s_cluster_name="kind-test", k8s_container_name="loss-test"}` 14:32:02 ~ 14:34:36) | 5초마다 1줄 → 기대 30~31 | **31줄**, 05:32:02Z ~ 05:34:32Z 5초 간격 빈틈 없음 | 0 |
| 트레이스 (trace_id 5개: 전 1 · 중 3 · 후 1) | 5 | Tempo `/api/traces/<id>`로 **5개 전부** 조회 | 0 |

- Loki 큐가 Tempo 큐보다 빨리 불어난 이유: 로그는 Fluent Bit이 1초마다 flush(배치 처리기가 5초마다 묶음) → 배치 수가 많음. 큐 단위는 "요청(배치)"이라 줄 수가 아님
- 게이트웨이 큐 상한은 5000배치. 이 시험 속도(약 15배치/분)면 수 시간치. Fluent Bit 쪽 버퍼(storage.total_limit_size 1G)는 게이트웨이가 죽을 때만 쓰이므로 이번 시험엔 비어 있었음
- Alertmanager: `MonitoringTargetDown`은 `for: 5m`이라 154초 중단엔 발화하지 않음. Slack 알람 없음(의도치 않은 부작용 없음)
- 모니터링 클러스터는 시험 뒤 Pod 전부 Running, 활성 알람 Watchdog만

## 3. 읽는 법

- **저장소만 죽으면 로그 · 트레이스 유실 0** — 게이트웨이 ×1 + hostPath 큐로 충분하다는 11/10 결정의 근거. 지표는 Agent WAL이 같은 역할(11/09 `docs/monitoring-cluster.md` 4.1절)
- 지키지 못하는 것은 **게이트웨이 자신이 죽은 동안의 span**(SDK가 ~1분 재시도 뒤 버림)과 **Pod가 다른 노드로 옮겨질 때 옛 노드에 남는 큐**. 전자는 3중화 때 재검토, 후자는 저장소와 게이트웨이가 같은 시간에 죽어야 생기는 양
- 복귀 뒤 40초 안에 밀어 넣으므로 Loki 룰(Xid 등)은 그만큼 늦게 평가된다. 중단 2.5분 + 지연 40초 ≈ 3분이 로그 기반 감지의 최악 지연(MON-STD-11 기대값에 반영 후보)
- 미확인: 관리 클러스터 실 노드(hostPath가 진짜 디스크, 여러 노드), 커널 로그(journald) 경로, 수 시간 중단 시 큐 상한 동작
