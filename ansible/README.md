# ansible/ — 쿠버 밖 호스트의 모니터링 에이전트

Proxmox 호스트(5대)처럼 k8s 밖에 있는 서버의 에이전트는 여기 플레이북으로 설치 · 설정한다. 손으로 깔지 않는다(11/08 결정, [[monitoring-iac-first]]). 실행은 ADR-11대로 개인 PC에서 수동.

| 플레이북 | 대상 | 하는 일 |
|---|---|---|
| `playbooks/pve-hosts.yml` | Proxmox 호스트 5대 (`inventory/hosts.yaml`) | node-exporter 패키지 확인 · 기동 인자 통일(`0.0.0.0:9100`) · textfile 수집기 패키지와 그 15분 타이머 제거 · 응답 확인<br>Fluent Bit 5.1.1 설치 · 호스트 journald 전체를 Loki로 전송 · 응답 확인 |

## 실행

IaC 레포의 Ansible venv(`~/gpu-failover/iac/ansible/.venv`, ansible 12)를 그대로 쓴다. Proxmox 호스트는 root 비밀번호로 붙는다(`-k`).

```bash
cd ~/gpu-failover/monitoring/ansible
A=~/gpu-failover/iac/ansible/.venv/bin/ansible-playbook
$A playbooks/pve-hosts.yml -k --check --diff   # 바뀔 것만 본다(패키지 제거 · 설정 파일 diff)
$A playbooks/pve-hosts.yml -k                  # 적용. 멱등이라 다시 돌려도 된다
```

확인(모니터링 클러스터 Mimir):
```bash
V=172.30.0.35
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=up{job="pve-host"}'          # 5개 1
curl -sG "http://$V:9009/prometheus/api/v1/query" --data-urlencode 'query=apt_upgrades_pending'       # 결과 없음이 정상(수집기 제거)
# 호스트 로그: 5대 × (kernel · journal). 조용한 호스트는 5분 안에 kernel 줄이 없을 수 있다
curl -sG "http://$V:3100/loki/api/v1/query" --data-urlencode 'query=sum by (service_instance_id, service_name) (count_over_time({service_namespace="pve-host"}[5m]))'
```

## 호스트 로그 (Fluent Bit, 11/17)

호스트의 Fluent Bit(systemd 서비스)가 journald 전체를 Loki OTLP(`172.30.0.35:3100/otlp/v1/logs`)로 직접 보낸다. 게이트웨이가 없으므로 Loki 라벨이 되는 리소스 속성을 Fluent Bit가 직접 붙인다.

| Loki 라벨 | 값 | 비고 |
|---|---|---|
| `service_namespace` | `pve-host` | Mimir `job`과 같은 값 |
| `service_instance_id` | `devoops` · `pve1`~`pve4` | Mimir `host`와 같은 값 |
| `service_name` | `kernel` · `journal` | 커널이 쓴 줄 / 나머지 전부(systemd 유닛 · Proxmox 서비스) |

- 유닛 이름은 라벨이 아니다. 리소스 속성에는 고정 값만 넣을 수 있어서(레코드 필드 참조 불가) 구조화 메타데이터로 조회한다: `{service_namespace="pve-host", service_name="journal"} | _SYSTEMD_UNIT="corosync.service"`
- `_CMDLINE` · `_EXE`(실행 명령줄 · 경로)는 보내지 않는다. 인자로 넘긴 비밀번호 · 토큰이 Loki에 남는 것을 막는다
- Fluent Bit 자기 로그도 보내지 않는다(전송 실패 로그가 다시 수집돼 도는 것 방지). 상태는 호스트에서 `curl 127.0.0.1:2020/api/v1/health` · `journalctl -u fluent-bit`
- 설치는 apt 저장소 없이 deb를 버전 · sha256 고정으로 받는다. 올릴 땐 `roles/fluent_bit/defaults/main.yml`의 버전 · sha256과 DaemonSet 이미지(`deploy/agents/base/fluent-bit`)를 같이 바꾼다
- Loki가 죽어 있는 동안은 `/var/lib/monitoring-agents/fluent-bit/storage`에 쌓고(상한 1G) 살아나면 보낸다

## 왜 이렇게 됐나 (11/10)

- 11/02 node-exporter를 손으로 깔 때 4대는 VDI 때 남아 있던 것을 썼고 pve4만 `apt install`로 새로 깔았다. 그때 Recommends로 `prometheus-node-exporter-collectors`(apt · nvme · smartmon textfile 수집기 + 부팅 기준 15분 systemd 타이머)가 pve4에만 들어왔다
- 그 타이머가 돌 때마다 pve4 CPU 온도가 1샘플 60~95°C로 튀었다(`docs/measurements/2026-11-06-pve-host-temp.md` 2 · 7절). 스파이크 18회 중 17회가 타이머 실행 5초 뒤
- 11/06엔 pve2 재부팅 때 node-exporter가 자기 IP에 묶여 있어 네트워크보다 먼저 떠 영구 실패 → 4대만 `0.0.0.0:9100`으로 고쳤고 pve4는 기본값 그대로였다
- 이 두 차이를 플레이북 한 번으로 없앤다. 앞으로 호스트 쪽 변경은 이 role을 고쳐서 한다

## 다음

- Kafka · DB VM inventory 그룹(node-exporter · Fluent Bit) — 현수 합의 뒤
