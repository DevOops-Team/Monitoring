# 모니터링 클러스터 (monitoring1~3, Kubespray) 구축 기록

ADR-18의 저장 층(Mimir · Loki · Tempo ×3, Alertmanager ×2, 분류기 ×2, Grafana ×1)을 올릴 **전용 k8s 3노드**. VM 1대 docker compose(`compose/`)를 대체한다. 이 문서는 클러스터 자체(VM · Kubespray · VIP)만 다룬다. 스택 배포는 `deploy/`(예정)를 본다.

- 구축: 2026-11-09 12:53 ~ 13:10 KST (Kubespray 1회, 실패 0)
- 역할: VM 생성 · VIP 예약 · inventory = 박기현(IaC PR #7 · #8 · #9) / Kubespray 실행 · 운영 = 이태림
- 관련: ADR-18, IaC 레포 `ansible/inventory/monitoring/README.md`, 로드맵 "11/08 반영" 절

## 1. 노드 · 주소

| 노드 | 호스트 | IP | VMID | 사양 | k8s 역할 |
|---|---|---|---|---|---|
| monitoring1 | pve3 | 172.30.0.31 | 131 | 4 vCPU · 10GB · 60GB | control-plane + etcd + worker |
| monitoring2 | pve3 | 172.30.0.32 | 132 | 같음 | 같음 |
| monitoring3 | pve1 | 172.30.0.33 | 133 | 같음 | 같음 |

| 주소 | 용도 | 뒤에 있는 것 |
|---|---|---|
| **172.30.0.30:6443** | API VIP (kube-vip, ARP) | kube-apiserver 3개. kubeconfig · ArgoCD 등록 주소 |
| **172.30.0.35** | 서비스 VIP (kube-vip services) | `type: LoadBalancer` Service들이 포트로 공유 (Mimir 9009 · Loki 3100 · OTLP 4317 · AM 9093 · Grafana 3000) |

세 노드는 사양 · 역할 · 구성물이 같다(ADR-18, 어느 노드가 죽어도 무상태 Pod를 받아야 함). taint 없음(worker 겸용). 2노드가 pve3 한 호스트라 pve3 장애 = etcd 과반 상실은 팀 전제(11/05 회고)로 받아들임.

## 2. 버전 · 설정

| 항목 | 값 | 출처 |
|---|---|---|
| Kubespray | v2.32.0 (커밋 9751ea6, `.vendor/`) | IaC `scripts/setup-kubespray.sh` |
| Kubernetes | 1.35.8, containerd 2.3.5, Calico, kube-proxy IPVS + strict ARP | `group_vars/k8s_cluster/k8s_cluster.yml` |
| kube-vip | v1.0.3, control-plane + services, ARP, 리더 선출, `enableServicesElection: false`(공유 IP는 한 노드만 광고) | `group_vars/k8s_cluster/ha.yml` |
| CIDR | Service 10.234.0.0/18 · Pod 10.234.64.0/18 (관리 클러스터 10.233.x와 분리) | 같은 파일 |
| 예약 자원 | kube/system reserved 각 512Mi · 300m → allocatable 3400m · 8.6GiB | PR #8 |
| etcd | host, 3멤버, `experimental_initial_corrupt_check: true` | PR #8 |
| 없음 | StorageClass(local-path 미설치), Ingress, 메트릭 서버 | Helm 이전 계획에서 결정 |

## 3. 실행 절차 (재실행도 같음)

IaC 레포 로컬 클론 `~/gpu-failover/iac`. sudo는 비밀번호 방식이라 Kubespray · kubeconfig 배포는 **사람이 터미널에서** `--ask-become-pass`로 실행한다(11/09 결정, NOPASSWD 안 함).

```bash
cd ~/gpu-failover/iac/ansible && PYTHON_BIN=python3.12 ./scripts/setup-kubespray.sh   # venv + Kubespray 벤더링, 1회
root="$(pwd)" && cd .vendor/kubespray
export ANSIBLE_HOST_KEY_CHECKING=True ANSIBLE_SSH_ARGS='-o ControlMaster=auto -o ControlPersist=60s'
"$root/.venv/bin/ansible-playbook" -i "$root/inventory/monitoring/hosts.yaml" cluster.yml --become --ask-become-pass
"$root/.venv/bin/ansible-playbook" -i "$root/inventory/monitoring/hosts.yaml" "$root/playbooks/k8s/admin-kubeconfig.yml" --become --ask-become-pass
```

- **`-i inventory/monitoring/hosts.yaml`을 빠뜨리면 관리 클러스터(`inventory/onprem`)에 돌아간다.** `scripts/run-cluster.sh`는 onprem 고정이라 쓰지 않는다
- **`ANSIBLE_SSH_ARGS` 덮어쓰기 필수**: Kubespray 자체 `ansible.cfg`가 `UserKnownHostsFile=/dev/null`이라 호스트 키 검증을 켜면 전부 `Host key verification failed`. run-cluster.sh와 같은 방식
- kubeconfig: 로컬 `~/gpu-failover/iac/ansible/inventory/monitoring/artifacts/admin.conf`(gitignore), 노드 계정 `~/.kube/config`. server는 API VIP
- Kubespray는 멱등. 중간 실패는 같은 명령 재실행. 설정 변경(예약 자원 등)도 재실행으로 반영
- 롤백은 `reset.yml`이 아니라 VM 재생성(박기현)

## 4. 검증 결과 (2026-11-09 13:00~13:15 KST)

| 확인 | 결과 |
|---|---|
| Kubespray RECAP | 3노드 failed=0 unreachable=0 (monitoring1 ok=635 changed=134) |
| `kubectl get nodes` | 3 Ready, control-plane, v1.35.8, taint 없음 |
| API VIP | `https://172.30.0.30/healthz` ok, `/readyz` ok. kubeconfig server = VIP |
| kube-vip | 3노드 static Pod Running(v1.0.3), 리더 monitoring1(`plndr-cp-lock` · `plndr-svcs-lock`) |
| kube-system | 22 Pod Running (calico · coredns · nodelocaldns · dns-autoscaler · 제어 평면) |
| 예약 자원 | capacity 4 CPU · 9.7GiB → allocatable 3400m · 8.6GiB |
| 서비스 VIP | nginx `type: LoadBalancer` + `kube-vip.io/loadbalancerIPs: 172.30.0.35` → EXTERNAL-IP .35, 노트북에서 `curl http://172.30.0.35` 200, Service 삭제 뒤 .35 ARP 사라짐 |
| 미완 | `verify-etcd-ha.yml`(sudo). 재부팅 전환은 4.1절(자동 복구 확인, 전환 시간은 미측정) |

알려진 점:
- **kube-vip static Pod의 미러 Pod가 API에 없다.** `kubectl get pods -A`에 안 보이지만 kubelet은 안다: `kubectl get --raw /api/v1/nodes/<노드>/proxy/pods`, 로그는 `.../proxy/containerLogs/kube-system/kube-vip-<노드>/kube-vip`. 리스 갱신 · VIP 동작은 정상. 원인은 미확인(Kubespray 템플릿 vs kubelet 1.35), 운영엔 영향 없음
- ~~VM 디스크가 게스트에서 `ROTA=1`~~ → 11/09 16:11 ~ 16:20 박기현이 ssd=1 적용, 세 노드 rotational 0(4.1절). etcd fsync 지연 비교는 아직 안 함

### 4.1 노드 순차 재부팅 실측 (2026-11-09 16:11 ~ 16:20 KST)

박기현이 VM 디스크에 ssd=1을 적용하며 세 노드를 한 대씩 재부팅했다. 클러스터에는 사람이 손대지 않았다. 당시 스택은 복제 1(Mimir = monitoring3, Agent · Tempo = monitoring1, Loki = monitoring2), Alertmanager · Grafana는 설치 전.

| 노드 | 내려가 있던 구간 (Agent가 본 kubelet `up == 0`) | 부팅 시각 (`node_boot_time_seconds`) |
|---|---|---|
| monitoring2 | 16:11:42 ~ 16:13:42 (약 2분) | 16:13:32 |
| monitoring3 | 16:15:25 ~ 16:17:10 (약 1분 45초) | 16:17:02 |
| monitoring1 | Agent가 이 노드에 있어 관측 없음 | 16:19:03 |

| 확인 | 결과 |
|---|---|
| 복구 | 3 Ready, Pod 전부 Running(재시작 1회), PVC는 같은 노드에서 다시 붙음. 수동 조치 없음 |
| 디스크 | 세 노드 `node_disk_info{device="sda"}` rotational 0 (적용 전 ROTA=1) |
| kube-vip 리더 | 둘 다 monitoring1 → API VIP monitoring2 · 서비스 VIP monitoring3 (리스 전환 각 1회) |
| Mimir 다운(복제 1) | `up{job="mimir"} == 0` 16:15:32 ~ 16:17:02, 다음 정상 샘플 16:18:17. 그동안 룰 평가 · 조회 중단 |
| Mimir 다운 중 수집 | 유실 없음. 그 구간 `pve-host` 샘플이 15초 간격으로 전부 있음 = Agent WAL이 Mimir 복귀 뒤 밀어 넣음 |
| Agent 쪽 공백 | 16:17:02 ~ 16:19:42에 스크레이프 간격이 60 ~ 90초로 벌어짐(2회, 평소 15초). 샘플이 0개인 1분 창은 없음. monitoring1 재부팅과 겹치나 앞쪽 60초 간격의 원인은 미확인 |

읽는 법:
- 복제 1이라도 **저장소가 죽은 동안의 지표는 Agent WAL이 지킨다.** 지키지 못하는 것은 그동안의 룰 평가 · 알람(저장소 3복제에서 해소)과 Agent 자신이 죽은 동안의 스크레이프(Agent 2개 + HA tracker에서 해소)다
- **VIP 전환에 걸린 시간은 재지 못했다.** 밖에서 VIP를 찌르는 기록이 없었다. 다음 재부팅 때 노트북에서 1초 간격으로 `:6443/healthz` · `:9009/ready`를 찔러 잰다
- 재부팅 간격이 빠듯했다: monitoring3 kubelet이 16:17:10에 돌아왔고 monitoring1이 16:19:03에 부팅됐다(중단은 그 전). etcd 과반이 유지됐는지는 따로 확인하지 못했다. 다음에는 `kubectl get nodes` Ready와 etcd 멤버 상태를 본 뒤 다음 노드로 넘어간다

## 5. 다음

1. ~~Helm 이전~~ → 11/09 완료(복제 1). 스택 구성 · 적용 · 확인은 [`deploy/README.md`](../deploy/README.md). StorageClass `local-path`(기본)도 그때 설치. 서비스 VIP는 Mimir 9009 · Loki 3100 · Tempo 4317/4318 · Alertmanager 9093 · Grafana 3000이 공유
2. ArgoCD 원격 등록(노찬경, 후순위): server `https://172.30.0.30:6443`
3. Monitoring 레포 `ansible/` roles(node_exporter · fluent_bit) → Proxmox 호스트 · Kafka/DB VM
4. VIP 전환 시간 측정(다음 노드 재부팅 때 1초 간격 probe) → 4.1절에 추가
5. ~~kubelet serving 인증서를 클러스터 CA로~~ → 11/09 하지 않기로 결정(`deploy/README.md` 보안 메모)
