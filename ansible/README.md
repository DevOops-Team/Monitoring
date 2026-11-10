# ansible/ — 쿠버 밖 호스트의 모니터링 에이전트

Proxmox 호스트(5대)처럼 k8s 밖에 있는 서버의 에이전트는 여기 플레이북으로 설치 · 설정한다. 손으로 깔지 않는다(11/08 결정, [[monitoring-iac-first]]). 실행은 ADR-11대로 개인 PC에서 수동.

| 플레이북 | 대상 | 하는 일 |
|---|---|---|
| `playbooks/pve-hosts.yml` | Proxmox 호스트 5대 (`inventory/hosts.yaml`) | node-exporter 패키지 확인 · 기동 인자 통일(`0.0.0.0:9100`) · textfile 수집기 패키지와 그 15분 타이머 제거 · 응답 확인 |

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
```

## 왜 이렇게 됐나 (11/10)

- 11/02 node-exporter를 손으로 깔 때 4대는 VDI 때 남아 있던 것을 썼고 pve4만 `apt install`로 새로 깔았다. 그때 Recommends로 `prometheus-node-exporter-collectors`(apt · nvme · smartmon textfile 수집기 + 부팅 기준 15분 systemd 타이머)가 pve4에만 들어왔다
- 그 타이머가 돌 때마다 pve4 CPU 온도가 1샘플 60~95°C로 튀었다(`docs/measurements/2026-11-06-pve-host-temp.md` 2 · 7절). 스파이크 18회 중 17회가 타이머 실행 5초 뒤
- 11/06엔 pve2 재부팅 때 node-exporter가 자기 IP에 묶여 있어 네트워크보다 먼저 떠 영구 실패 → 4대만 `0.0.0.0:9100`으로 고쳤고 pve4는 기본값 그대로였다
- 이 두 차이를 플레이북 한 번으로 없앤다. 앞으로 호스트 쪽 변경은 이 role을 고쳐서 한다

## 다음

- `fluent_bit` role(호스트 journald → Loki OTLP 직접, ADR-18) — Kafka · DB VM은 현수 합의 뒤
- Kafka · DB VM inventory 그룹
