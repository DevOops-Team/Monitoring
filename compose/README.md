# 모니터링 VM 스택 (docker compose)

VM `monitoring` 172.30.0.30 (Ubuntu 24.04, 6 vCPU · 16GB · 200GB). 이 문서가 MON-STD-22 "모니터링 VM 복구 런북"의 초안이다. 부 담당이 이 절차만으로 재설치할 수 있어야 한다.

## 구성 (1차)

| 서비스 | 이미지 | 포트 | 역할 |
|---|---|---|---|
| prometheus | prom/prometheus:v3.15.1 | 9090 | 중앙 TSDB. 워커·매니저 Agent의 remote_write 수신, 보존 30일 · 60GB 상한 |
| alertmanager | prom/alertmanager:v0.34.2 | 9093 | Watchdog → healthchecks.io, 나머지 → Slack #alerts-gpu |
| grafana | grafana/grafana:13.3.0 | 3000 | 데이터소스 Prometheus · Alertmanager 프로비저닝 |
| node-exporter | prom/node-exporter:v1.12.1 | 9100 (host) | VM 자체 지표 |
| cadvisor | ghcr.io/google/cadvisor:v0.60.6 | 내부만 | 컨테이너 자원 지표 |

외부 수집 대상: Proxmox 호스트 5대(`job: pve-host`) · Kafka JMX Exporter 172.30.0.40:7071(`job: kafka-jmx`, 현수 설치, 1만 줄 중 필요한 것만 metric_relabel로 남김).

2차 예정: Loki · Alloy(수신) · Tempo · blackbox-exporter(VPN 터널 · pfSense · 공인 DNS 확인용) · 분류기(Alertmanager webhook 수신 → Kafka `gpu.fault` 발행, DynamoDB 하트비트 · EC2 상태 폴링) · kafka_exporter. 설계는 `docs/fault-event.md` · `docs/worker-agents.md`.

## 처음 설치

1. 노트북에서 동기화 (Docker 없어도 파일은 올라감)
   ```
   scripts/deploy.sh --no-lint
   ```
2. VM에 Docker 설치 (sudo 비밀번호 필요, 1회)
   ```
   ssh -t monitoring@172.30.0.30 'sudo bash ~/monitoring/scripts/install-docker.sh'
   ```
3. VM에 시크릿 작성 (레포엔 없음)
   ```
   ssh monitoring@172.30.0.30
   cd ~/monitoring/compose
   cp .env.example .env && vi .env                # GRAFANA_ADMIN_PASSWORD
   mkdir -p secrets
   printf '%s' 'https://hooks.slack.com/services/...' > secrets/slack_webhook_url
   printf '%s' 'https://hc-ping.com/<uuid>'         > secrets/healthchecks_ping_url
   chmod 755 secrets && chmod 644 secrets/*   # Alertmanager 컨테이너는 nobody(65534)로 돌아 600이면 못 읽음
   ```
   healthchecks.io 체크: period 1분 · grace 4분 (→ 미수신 5분에 알림), 통합 = Slack #alerts-gpu.
4. 기동
   ```
   scripts/deploy.sh
   ```

## 확인

| 무엇 | 어떻게 | 기대 |
|---|---|---|
| 타깃 | http://172.30.0.30:9090/targets | VM 5개 + pve-host 5개 + kafka-jmx 1개 모두 UP |
| Watchdog | http://172.30.0.30:9093/#/alerts | Watchdog 1건 상시 |
| 데드맨 | healthchecks.io 대시보드 | 1분마다 핑 수신 |
| Slack | `docker compose exec alertmanager amtool alert add test severity=warning --annotation=summary=테스트` | #alerts-gpu에 [FIRING] test |
| Grafana | http://172.30.0.30:3000 (admin) → Connections → Data sources → Prometheus → Test | OK |

## 설정 변경

`compose/` 아래를 고치고 `scripts/deploy.sh`. Prometheus 룰·설정만 바꿨으면 재시작 없이 재적용:
```
curl -X POST http://172.30.0.30:9090/-/reload
```

## 전부 지우고 다시

```
ssh monitoring@172.30.0.30 'cd ~/monitoring/compose && docker compose down -v'   # 데이터 볼륨까지 삭제
scripts/deploy.sh
```
