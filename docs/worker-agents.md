# 워커 EKS 모니터링 에이전트 배치 규약 (2차 스택)

워커 EKS 3개(AZ별)에 올릴 모니터링 구성 요소가 **어느 노드에, 어떤 toleration으로** 뜨는지 한 장. 노드 라벨 · taint는 김성모 "쿠버네티스 아키텍처 초안" 6절(2026-11-05 v5, 권장안)과 ADR-2 · 4를 따른다. 매니페스트는 이 표대로 쓴다.

## 1. 노드 종류와 taint

| 노드 | 라벨 | taint | 누가 뜨나 |
|---|---|---|---|
| 시스템 노드 (온디맨드 CPU) | `devoops/node-group=system` | 없음 | Kueue · JobSet · Karpenter · CoreDNS · **Prometheus Agent · kube-state-metrics** |
| GPU 노드 (g4dn 스팟) | `devoops/gpu=true` | `nvidia.com/gpu=present:NoSchedule` | 학습 Pod · **device plugin · dcgm-exporter · node-exporter · Alloy · NVML 사이드카** |
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
| Alloy (Pod 로그 · journald 커널 로그 · OTLP 수신) | DaemonSet | 없음 | `[{operator: Exists}]` | 전 노드. 커널 로그는 `loki.source.journal` cursor, `_TRANSPORT=kernel`. 호스트 `/var/log/journal` 마운트 |
| Prometheus Agent | Deployment ×1 | `devoops/node-group: system` | 없음 | 스팟 회수 때 수집 파이프 생존. `external_labels: {cluster: worker-a}` |
| kube-state-metrics | Deployment ×1 | `devoops/node-group: system` | 없음 | 노드 allocatable · Pod 종료 코드 |
| 스팟 통보 (aws-node-termination-handler IMDS 모드) | DaemonSet | `devoops/gpu: "true"` | `[{operator: Exists}]` | GPU 노드만 스팟 |

`operator: Exists`(키 생략)는 모든 taint를 허용한다. GPU taint와 spare taint 둘 다 먹어야 예비 노드에도 뜨므로 키를 하나씩 적는 대신 이렇게 둔다. device plugin DaemonSet(인프라 소유, ns `nvidia-device-plugin`)도 같은 toleration이어야 예비 GPU가 광고된다 (B4 학습 페이지).

## 3. 신호 경로 (2차 스택 기준)

```
GPU 노드: dcgm-exporter · node-exporter · NVML 사이드카 ──/metrics──▶ Prometheus Agent(시스템 노드) ──remote_write(VPN)──▶ 모니터링 VM Prometheus
GPU 노드: journald 커널 로그 · Pod 로그 ──Alloy(push, VPN)──▶ 모니터링 VM Loki ──ruler 알람──▶ Alertmanager
Alertmanager ──webhook(pipeline=failover)──▶ 분류기(모니터링 VM) ──Kafka gpu.fault(172.30.0.40:9092, 키=node)──▶ 복구 컨트롤러 · 수집기
분류기 ──공인망──▶ DynamoDB heartbeat_at · progress, EC2 DescribeInstanceStatus   ← VPN 단절 vs AZ 장애 독립 경로 (MON-STD-18)
```

- 분류기 출력은 HTTP가 아니라 Kafka다 (ADR-7). kind · 단위 테스트에서만 HTTP 스텁.
- 독립 경로는 워커에 직접 닿는 probe가 아니다. 워커는 프라이빗 서브넷(공인 IP 없음), EKS API는 비공개 엔드포인트. 사이드카(rank 0)가 공인 엔드포인트로 갱신하는 DynamoDB 하트비트가 "워커가 살아 있다"의 증거다. 모니터링 VM에 DynamoDB 두 테이블 읽기 · EC2 `DescribeInstanceStatus` IAM 필요 (박기현).
- blackbox_exporter는 VPN 터널 내부 IP(169.254.x) · pfSense · 공인 DNS 확인용으로만 남긴다.

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
- [ ] (박기현) 모니터링 VM IAM: DynamoDB `devoops-job-fencing` · `devoops-job-progress` 읽기, `ec2:DescribeInstanceStatus`
- [x] (김성모) 예비 taint 키 `devoops/spare=true:NoSchedule` · GPU 라벨 `devoops/gpu=true` 확정 (2026-11-05). monitoring ns 자원 예산 0.5코어 · 1Gi, nodeSelector `platform-worker=true`(관리 클러스터)도 그대로
- [ ] (나) dcgm-exporter Helm values: `customMetrics` 전체 목록(B3 초안) + Xid 230 주기 5초, tolerations Exists
