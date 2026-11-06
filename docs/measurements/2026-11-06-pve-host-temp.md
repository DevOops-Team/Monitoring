# Proxmox 호스트 온도 기준선 (MON-STD-24) — 1차: 덮개 닫힘

- 측정: 2026-11-06 17:44 KST, 중앙 Prometheus `job="pve-host"` (node-exporter `node_hwmon_temp_celsius`)
- 조건: 노트북 4대(pve1~4) **덮개 닫은 채 약 1주일(10/30경~) 연속 가동**, 평소 부하(관리 클러스터 VM·pfSense 유휴). 주입 없음. 즉 1차 값은 "열이 충분히 쌓인 정상 상태"이지 켠 직후가 아님
- 목적: 사용자가 노트북 표면이 뜨겁다고 느껴 센서값 확인. 주말에 **덮개 열고** 같은 쿼리로 재측정해 비교
- 24h 창에는 pve2 재부팅(11/06 오전, exporter 복구 전 공백 약 18h)이 포함됨 → pve2 24h 값은 샘플 326개 기준

## 1. 값

| 호스트 | CPU 현재 | CPU 24h 최고 | CPU 24h 평균 (temp1) | NVMe 현재 | NVMe 24h 최고 |
|---|---|---|---|---|---|
| devoops (데스크톱, VM 4개) | 39 | 49 | 31 | 58 | 60 |
| pve1 (master2·worker2) | 43 | 75 | 35 | 29 | 30 |
| pve2 (master3·worker3) | 41 | 51 | 40 | 39 | 55 |
| pve3 (monitoring) | 40 | 74 | 40 | 27 | 31 |
| pve4 (pfSense) | 38 | 88 | 40 | 21 | 30 |

단위 °C. CPU = `chip=~"platform_coretemp.*"` 센서 중 최대, NVMe = `chip=~"nvme.*"` 중 최대.

## 2. 발견: pve4 15분 주기 1샘플 스파이크

`max_over_time` 24h 최고(pve4 88 · pve1 75 · pve3 74)는 지속 고온이 아니라 **한 샘플(15초 scrape 1회)만 튄 것**. 1분 step 시계열로 보면:

| 시각 (KST) | pve4 CPU 온도 | CPU 사용 (코어, 1m rate) |
|---|---|---|
| 15:37:34 | 67 | 0.05 |
| 15:52:34 | 79 | 0.05 |
| 16:07:34 | 64 | 0.04 |
| 16:22:34 | 66 | 0.05 |
| 16:37:34 | 83 | 0.06 |
| 16:52:34 | 82 | 0.05 |
| 17:07:34 | 63 | 0.05 |
| 17:22:34 | 78 | 0.05 |
| 17:37:34 | 76 | 0.04 |

- 정확히 **15분 주기(:07·:22·:37·:52)**, 한 샘플 뒤 바로 40°C 안팎으로 복귀
- 스파이크 순간 CPU 사용은 평소와 같음(0.05코어) → 지속 부하 아님. 15초 안에 끝나는 단일 코어 짧은 버스트(터보)이거나 센서 읽기 특성으로 추정. **원인은 미확인** → 박기현 확인 항목 (pve4 쪽 15분 주기 작업: Proxmox 스케줄러·pfSense cron·smartctl 등)
- pve1·pve3도 60~75°C 1샘플 스파이크가 하루 1~2회 있음. devoops·pve2는 없음

## 3. 표준에 주는 영향 (MON-STD-24)

1. **CPU 온도 알람은 인스턴트 값으로 걸면 15분마다 오탐**. `for: 5m`(연속 20회 평가) 또는 `avg_over_time(...[5m])`을 써야 함. A5 6절 "for가 다른 근거"의 실측 사례
2. **NVMe는 CPU와 기준이 다름**(스로틀 70~80°C vs CPU Tjmax 100°C) → 행을 CPU/NVMe로 나눔. devoops NVMe 58~60°C는 VM 4개(Kafka·DB 포함)의 디스크 쓰기 때문으로 보이며, Kafka·DB 부하가 늘면 올라갈 후보
3. 평상시 기준선(덮개 닫힘): CPU 31~40°C, NVMe 21~58°C. 경고 80°C 가설과 거리 멀어 가설 유지

## 4. 비교용 쿼리 (주말 재측정 때 그대로)

```promql
# 호스트별 현재 CPU 최고
max by (host) (node_hwmon_temp_celsius{job="pve-host", chip=~"platform_coretemp.*"})
# 24h 최고 · 평균
max by (host) (max_over_time(node_hwmon_temp_celsius{job="pve-host", chip=~"platform_coretemp.*"}[24h]))
max by (host) (avg_over_time(node_hwmon_temp_celsius{job="pve-host", chip=~"platform_coretemp.*", sensor="temp1"}[24h]))
# NVMe
max by (host) (node_hwmon_temp_celsius{job="pve-host", chip=~"nvme.*"})
# 스파이크 확인 (Grafana Explore, 1m step)
max by (host) (node_hwmon_temp_celsius{job="pve-host", host="pve4", chip=~"platform_coretemp.*"})
```

**2차 조건 시작: 2026-11-06 17:52 KST, 노트북 4대 덮개 열림 (전부 가동 유지).** 재측정 절차: 24h 이상 지난 뒤 위 쿼리 → 2절 표와 같은 형식으로 "2차: 덮개 열림" 절 추가 → 평균 차이로 덮개 영향 판단. 그 뒤 Confluence 5. 트러블슈팅 › 인프라에 전후 비교 1장.
