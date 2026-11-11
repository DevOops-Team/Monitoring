# 클러스터 수집 층 배치 규약 — 워커 EKS · 관리 클러스터

워커 EKS 3개(AZ별)와 관리 클러스터(온프레미스)에 올릴 모니터링 구성 요소가 **어느 노드에, 어떤 toleration으로** 뜨는지 한 장. 매니페스트는 `deploy/agents/`(base 공통 + overlays/{onprem,kind}, EKS overlay는 재구축 뒤). ADR-18(11/08)로 Alloy는 폐기, **Fluent Bit DaemonSet + OTel Collector 게이트웨이**로 바뀌었다. 노드 라벨 · taint는 김성모 "쿠버네티스 아키텍처 초안" 6절(2026-11-05 v5, 권장안)과 ADR-2 · 4를 따른다. 매니페스트는 이 표대로 쓴다.

## 1. 노드 종류와 taint

| 노드 | 라벨 | taint | 누가 뜨나 |
|---|---|---|---|
| 시스템 노드 (온디맨드 CPU) | `devoops/node-group=system` | 없음 | Kueue · JobSet · Karpenter · CoreDNS · **Prometheus Agent · kube-state-metrics · OTel 게이트웨이** |
| GPU 노드 (g4dn 스팟) | `devoops/gpu=true` | `nvidia.com/gpu=present:NoSchedule` | 학습 Pod · **device plugin · dcgm-exporter · node-exporter · Fluent Bit · NVML 사이드카** |
| 예비 GPU 노드 | 위 + `devoops/spare=true` | 위 + `devoops/spare=true:NoSchedule` | 자리 표시 Pod · 복구 Pod · **GPU 노드와 같은 DaemonSet 전부** |

- 예비 taint 키는 `devoops/spare`다. 이전 설계의 `gpu-failover/spare`는 폐기 (2026-11-05).
- 학습 Pod의 GPU · spare toleration은 템플릿이 아니라 Kueue ResourceFlavor가 넣는다. 우리 매니페스트와 무관.
- 라벨 · taint 키는 김성모가 확정함 (2026-11-05, Slack). Terraform 반영은 인프라(박기현).

## 2. 구성 요소별 배치

| 구성 요소 | 종류 | nodeSelector | tolerations | 이유 |
|---|---|---|---|---|
| dcgm-exporter | DaemonSet | `devoops/gpu: "true"` | `[{operator: Exists}]` | GPU 노드 전부(예비 포함). Xid 필드 230 수집 5초 (fault-event ±45초 창 전제) |
| NVML 사이드카 (호출 지연 · Xid 이벤트) | DaemonSet | `devoops/gpu: "true"` | `[{operator: Exists}]` | dcgm과 같은 노드 |
| node-exporter | DaemonSet | 없음 | `[{operator: Exists}]` | 시스템 · GPU · 예비 전부 |
| Fluent Bit (Pod 로그 · journald 커널 로그) | DaemonSet | 없음 | `[{operator: Exists}]` | 전 노드. 호스트 `/var/log` · `/run/log` 읽기 전용, 책갈피 db · 미전송 버퍼는 hostPath `/var/lib/monitoring-agents/fluent-bit`. 커널 로그는 systemd 입력 `_TRANSPORT=kernel`(cursor 책갈피). 출력은 OTLP → 게이트웨이(저장소 주소를 모른다) |
| OTel Collector 게이트웨이 (로그 · 앱 span 수신) | Deployment ×1 | `devoops/node-group: system` | 없음 | 클러스터의 출구 한 곳. file_storage 영속 큐로 저장소 · VPN 중단을 버팀. 트레이스는 SDK에 디스크 버퍼가 없어 이게 유일한 보호. 클러스터 이름 라벨(`k8s.cluster.name`)을 여기서 붙임 |
| Prometheus Agent | Deployment ×1 | `devoops/node-group: system` | 없음 | 스팟 회수 때 수집 파이프 생존. `external_labels: {cluster: worker-a}`(env `CLUSTER_NAME`). WAL은 EKS는 PVC(gp3), 관리 클러스터는 hostPath |
| kube-state-metrics | Deployment ×1 | `devoops/node-group: system` | 없음 | 노드 allocatable · Pod 종료 코드 |
| 스팟 통보 (aws-node-termination-handler IMDS 모드) | DaemonSet | `devoops/gpu: "true"` | `[{operator: Exists}]` | GPU 노드만 스팟 |

`operator: Exists`(키 생략)는 모든 taint를 허용한다. GPU taint와 spare taint 둘 다 먹어야 예비 노드에도 뜨므로 키를 하나씩 적는 대신 이렇게 둔다. device plugin DaemonSet(인프라 소유, ns `nvidia-device-plugin`)도 같은 toleration이어야 예비 GPU가 광고된다 (B4 학습 페이지).

## 3. 신호 경로 (ADR-18 기준, 저장소 = 모니터링 클러스터 서비스 VIP 172.30.0.35)

```
GPU 노드: dcgm-exporter · node-exporter · NVML 사이드카 ──/metrics──▶ Prometheus Agent(시스템 노드, WAL) ──remote_write(VPN)──▶ Mimir :9009
전 노드: journald 커널 로그 · Pod 로그 ──Fluent Bit(책갈피 · 디스크 버퍼)──OTLP──▶ OTel 게이트웨이(시스템 노드, 디스크 큐) ──▶ Loki :3100/otlp
앱 SDK(분류기 · 컨트롤러 · 사이드카 span) ──OTLP──▶ OTel 게이트웨이 ──▶ Tempo :4317
Loki ──ruler 알람──▶ Alertmanager :9093 ──webhook(pipeline=failover)──▶ 분류기(모니터링 클러스터) ──Kafka gpu.fault(172.30.0.40:9092, 키=node)──▶ 복구 컨트롤러 · 수집기
분류기 ──공인망──▶ DynamoDB heartbeat_at · progress, EC2 DescribeInstanceStatus   ← VPN 단절 vs AZ 장애 독립 경로 (MON-STD-18)
```

- 분류기 출력은 HTTP가 아니라 Kafka다 (ADR-7). kind · 단위 테스트에서만 HTTP 스텁.
- 독립 경로는 워커에 직접 닿는 probe가 아니다. 워커는 프라이빗 서브넷(공인 IP 없음), EKS API는 비공개 엔드포인트. 사이드카(rank 0)가 공인 엔드포인트로 갱신하는 DynamoDB 하트비트가 "워커가 살아 있다"의 증거다. 분류기(모니터링 클러스터)가 DynamoDB 두 테이블 읽기 · EC2 `DescribeInstanceStatus`를 하려면 온프레 → AWS 자격 증명(Roles Anywhere) 필요 (박기현).
- blackbox_exporter는 VPN 터널 내부 IP(169.254.x) · pfSense · 공인 DNS 확인용으로만 남긴다.

## 3-1. 관리 클러스터(온프레미스)는 어떻게 다른가

같은 base에 `deploy/agents/overlays/onprem`만 다르다. kind(`overlays/kind`)에서 실제 저장소로 데이터가 들어가는 것까지 검증했다(11/10).

| 항목 | 관리 클러스터 | 이유 |
|---|---|---|
| 1개짜리(Agent · KSM · 게이트웨이) nodeSelector | `platform-worker: "true"` | 플랫폼 앱과 같은 관례(김성모). 마스터 3대는 taint |
| Fluent Bit | 전 노드, toleration Exists | 마스터의 apiserver · etcd · scheduler 로그(`/var/log/pods`)와 kubelet · containerd · 커널(journald)이 있다. EKS는 마스터가 없어 이 차이가 안 보인다 |
| cluster 라벨 | `onprem-manager` (노드 라벨 `platform-cluster`와 같은 값), site `onprem` | overlay가 Deployment env로 넣음 |
| 버퍼 디스크 | hostPath `/var/lib/monitoring-agents/{prometheus-agent,otel-gateway,fluent-bit}` | StorageClass 없음. Pod 재시작은 버티고, 다른 노드로 옮겨질 때 잃는 것은 "저장소도 같이 죽어 있던 동안 쌓인 양"뿐 |
| 복제 수 | Agent 1 · 게이트웨이 1 | Agent ×2 + Mimir HA tracker는 모니터링 클러스터 3중화 때 두 클러스터를 같이. 게이트웨이는 push 수신이라 2개여도 죽은 대의 큐는 못 지킴 → 1 유지 |
| 적용 | ArgoCD Application `gpu-monitoring-agents`(K8s 레포), automated · prune · selfHeal, Monitoring 레포 **main**의 `deploy/agents/overlays/onprem` | main 머지 리뷰가 안전장치. AppProject `gpu-platform`에 Monitoring 레포 source · `monitoring` destination 필요(노찬경) |
| 네임스페이스 | `monitoring`은 K8s 레포 `gpu-foundation`이 만듦 | 우리 매니페스트에 Namespace 없음(FailOnSharedResource 충돌 방지) |

유실 실측(`docs/measurements/2026-11-10-agents-storage-outage.md`): Loki · Tempo 154초 중단에 로그 31/31 · 트레이스 5/5 도착, 복귀 뒤 40초에 큐 비움.

## 4. 분류기가 이벤트를 만들지 않는 것

| 신호 | 왜 |
|---|---|
| 학습 컨테이너 exit 75 | STOP. 사이드카가 세대 불일치 · 승인 거부로 멈춘 것. 펜싱 정상 동작 (ADR-12) |
| exit 0 · 사용자 취소 | 정상 종료 |
| Xid 45 | 앞선 오류의 뒤처리. 컨트롤러 조치가 만드는 Xid라 루프 방지 |
| Kueue 재큐 · MultiKueue 재디스패치 | 둘 다 꺼져 있음(ADR-12). 생기면 감시기(김현수)가 `job.failed`로 올리고 컨트롤러가 처리 |

## 5. 확인 필요

- [ ] (박기현) 노드 그룹 라벨 `devoops/gpu=true` · `devoops/node-group=system` · spare 라벨/taint를 Terraform에 (B4 때 요청한 `nvidia.com/gpu.present=true`는 이 권장안으로 대체)
- [ ] (박기현) AMI journald 디스크 저장 여부 (`/var/log/journal` 존재)
- [ ] (박기현) 분류기용 Roles Anywhere 역할: DynamoDB `devoops-job-fencing` · `devoops-job-progress` 읽기, `ec2:DescribeInstanceStatus` (모니터링 S3 역할 3개와 같은 방식)
- [x] (김성모) 예비 taint 키 `devoops/spare=true:NoSchedule` · GPU 라벨 `devoops/gpu=true` 확정 (2026-11-05). monitoring ns 자원 예산 0.5코어 · 1Gi, nodeSelector `platform-worker=true`(관리 클러스터)도 그대로
- [ ] (나) dcgm-exporter Helm values: `customMetrics` 전체 목록(B3 초안) + Xid 230 주기 5초, tolerations Exists
- [ ] (나) EKS overlay: nodeSelector `devoops/node-group: system`, Agent WAL · 게이트웨이 큐를 gp3 PVC로, cluster 라벨 worker-a/b/c (워커별 ConfigMap `devoops-cluster-info`와 맞출지 확인)
- [ ] (나) 관리 클러스터 적용 뒤 커널 로그 확인 `{k8s_cluster_name="onprem-manager", service_name="kernel"}` (kind엔 kernel journal이 없어 미확인)
- [ ] (노찬경) AppProject `gpu-platform`에 Monitoring 레포 · `monitoring` destination, ArgoCD UI 읽기 계정
