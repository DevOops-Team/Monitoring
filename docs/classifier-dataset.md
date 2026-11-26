# 분류기 모델 데이터셋 · 라벨 대응표 (ADR-45, GPU-191)

분류기의 AI 판정 모델(ADR-45)을 학습 · 평가할 데이터를 어디서 얻고, 각 출처의 라벨을 우리 `category`로 어떻게 옮기는지 적는다.
모델 출력은 `hw` · `driver` · `app` · `infra` · `network` · `none`(장애 아님) 여섯 가지다. `hw_warn` · `site` · `straggler`는 룰 전용이라 모델에 없다.

- 확인한 날: 2026-11-26. 줄 수 · 라벨 값은 **파일을 받아 센 값**이다. 못 센 칸은 「미확인」으로 적었다.
- 대응이 분명하지 않은 것은 「미정」으로 두고 이유를 적었다.

## 1. 샘플 형식

```json
{"source": "kernel|pod|nccl", "text": "...", "context": ["..."], "label": "hw|driver|app|infra|network|none", "origin": "ours|gf-bench|denpex|synthetic", "run_id": "..."}
```

- `text` · `context`의 줄은 모두 `classifier/classifier/normalize.py`의 `normalize()`를 거친다(`docs/classifier.md` 5절). 외부 데이터도 같은 함수를 쓴다.
- 학습 · 평가는 줄이 아니라 `run_id` 단위로 나눈다. **평가는 `origin: ours`만** 쓴다.

## 2. 외부 데이터셋

| 데이터셋 | 실제로 든 것 (센 값) | 원문 줄 | 쓰임 | 라이선스 |
|---|---|---|---|---|
| [GF-Bench](https://github.com/cuiboyuan/nccl_log_rca_bench) | **예시 실행 8개뿐**(정상 4 · OOM 2 · 스트래글러 2), 파일 40개 · 3,212줄. NCCL 줄 2,092개는 전부 `INFO` · `CALL` 수준 | 있음 | 보조 학습 (`app` · `none`) | 저장소에 없음 → 저자 확인 전에는 재배포 안 함 |
| [Denpex fixtures](https://github.com/denpexzero/denpex-gpu-failure-fixtures) | 공개 이슈 25건, 라벨 18종, 오류 문구 354줄(건당 1 ~ 46줄) | 있음 | 보조 학습 (`app`) | MIT |
| [Delta A100 · H100](https://doi.org/10.5281/zenodo.15287639) | 정규식 매칭 뒤 집계 피클 5개 + Xid 정규식 표 143행. **원문 로그 줄 없음**(원본 300GB+는 비공개) | 없음 | 학습 안 씀. Xid 분류 참고 · 합성 줄 형식 참고 | CC-BY-4.0 |
| [ClusterWise](https://huggingface.co/datasets/MachaParfait/ClusterWise) | parquet 145개 · 221GB. 온도 · 전력 · 작업 기록 + 숫자 `xid` 열. **로그 문구 없음** | 없음 | 안 씀 (받지 않음, README · 파일 목록만 확인) | CC-BY-4.0 |
| [ORNL Summit DBE](https://doi.ccs.ornl.gov/dataset/56c244d2-d273-5222-8f4b-f2324282fab8) | V100 메모리 2비트 오류(Xid 48) 때의 GPU 상태: Xid 기록 · 1Hz 측정값 · 재부팅 로그 · 작업 기록 (설명 페이지 기준, Globus로만 받을 수 있어 받지 않음) | 미확인 | 안 씀. 있어도 `hw` 한 종류(Xid 48)이고 Xid는 룰 몫 | 미확인 |
| [Loghub](https://github.com/logpai/loghub) | 시스템 16종(Android · Apache · BGL · HDFS · HPC · Hadoop · Linux · OpenSSH · OpenStack · Spark · Thunderbird · Zookeeper 등)의 일반 로그. **GPU 로그 없음** | 있음 | 안 씀 (사전 학습 비교에만 쓸 수 있음) | 저장소 표기 불명확 |

ADR-45 v2와 다른 점 (파일을 받아 보고 알게 된 것):

- **GF-Bench 저장소에는 220회 전체가 없다.** 예시 8회만 들어 있고, 전체는 GPU 2장으로 스크립트를 돌려 다시 만들어야 한다.
- **Delta는 라이선스가 있다(CC-BY-4.0).** 대신 원문 줄이 없어 줄 분류 학습에는 못 쓴다.
- **ClusterWise도 원문 줄이 없다.** `hw` 보강에 쓸 문구가 없다.

Delta 피클은 열지 않았다(피클은 여는 순간 코드가 실행될 수 있다). `pickletools`로 안에 든 문자열만 훑어 원문 줄이 없음을 확인했다.

### 2.1 GF-Bench 라벨 → category

라벨은 실행 단위(`dataset/labels/*.csv`의 `fault_type`)다. 줄 단위로 다시 붙여야 한다.

| `fault_type` | 실행 수 | category | 붙이는 줄 |
|---|---|---|---|
| `no_fault` | 4 | `none` | 전체 줄 |
| `cuda_oom` | 2 | `app` | `stdout.log` · `stderr.log`의 `CUDA out of memory` 줄과 그 traceback. 나머지 줄은 `none` |
| `gpu_straggler` | 2 | 안 씀 | 스트래글러는 룰 전용(ADR-37). 남는 줄도 주입기가 직접 찍은 `injecting delay of N seconds`뿐이라 배울 것이 없다 |

- NCCL 줄은 `호스트:PID:TID [랭크] NCCL INFO …` 형식이다. 우리 trainer 로그에는 이 형식이 없다(NCCL 디버그를 켜지 않음) → 지금 `normalize()`는 이 머리말을 지우지 않는다. **쓰기로 하면 패턴 추가 필요.**
- 환경이 다르다: A40 2장 · 컨테이너 1개 · PyTorch 2.4.1 · NCCL 2.20.5 (우리는 T4 · Pod마다 랭크 1개 · NCCL 2.21.5).

### 2.2 Denpex `failure_type` → category

| `failure_type` (건수) | category | 이유 |
|---|---|---|
| `OOM_FRAGMENTATION`(3) · `PINNED_HOST_MEMORY_ALLOCATION_LIMIT`(1) | `app` | 메모리 부족 |
| `GRADIENT_EXPLOSION`(2) · `NAN_LOSS`(1) | `app` | 학습 발산 |
| `STATE_DICT_SHAPE_MISMATCH`(1) · `TENSOR_DEVICE_MISMATCH`(1) · `VERSION_MISMATCH`(3) · `DEEPSPEED_ZERO3_PARTITIONED_CHECKPOINT_SHAPE_MISMATCH`(1) | `app` | 코드 · 설정 오류 |
| `FSDP_ERROR`(1) · `DDP_ERROR`(1) · `DEEPSPEED_ERROR`(2) · `TORCH_COMPILE_DYNAMO`(1) · `TORCH_COMPILE_AOTI`(1) · `TORCH_COMPILE_INDUCTOR`(1) | `app` | 프레임워크 안의 예외 |
| `SILENT_HANG`(2) | 미정 | 오류 줄 없이 멈춘 경우. 줄 분류 대상이 아닐 수 있다 |
| `CHECKPOINT_CORRUPTION`(1) · `DEEPSPEED_NVME_CHECKPOINT_RACE`(1) | 미정 | 저장소 문제(`infra`)인지 코드 문제(`app`)인지 문구를 읽고 정해야 한다 |
| `DEVICE_ASSERT`(1) | 미정 | 잘못된 메모리 접근. `app`이 유력하나 Xid를 동반하면 룰 몫 |

25건 중 21건이 `app`, 4종 5건이 미정이다. `infra` · `network` · `hw` · `driver`는 없다.

### 2.3 Delta Xid 분류 (참고만)

`regex_with_new_gpu.tsv`가 Xid 번호마다 `Category`를 붙인다. 우리 Xid 판정은 `rules.yaml`(NVIDIA Xid Catalog 기준)이 정하므로 **라벨로 쓰지 않고**, 표에 없는 Xid 번호를 모델로 넘길 때 참고한다.

| Delta `Category` (행 수) | 우리 category |
|---|---|
| `GPU_HARDWARE`(9) · `GPU_MEM`(1) · `GPU_PCIE`(9) | `hw` |
| `GPU_DRIVERS`(12) | `driver` |
| `GPU_USER_APP`(3) | `app` |
| `GPU_HARDWARE_DRIVERS`(40) · `GPU_USER_DRIVERS`(27) · `GPU_MULTI`(7) | 미정 (원인 후보가 둘 이상) |
| `GPU_RESERVED`(20) · `GPU_UNUSED`(15) | 안 씀 |

### 2.4 찾아봤지만 안 쓰는 것 (2026-11-26 검색)

Hugging Face(검색어 25개: `nccl` · `xid` · `gpu failure` · `cuda error` · `cuda out of memory` · `log anomaly` · `syslog` · `incident log` 등, 169건)와 Kaggle(검색어 5개)에서 이름이 가까운 것을 열어 봤다. 전수 확인은 아니다.

| 데이터셋 | 실제 내용 | 안 쓰는 이유 |
|---|---|---|
| HF `emgena/nccl_multi_gpu_allreduce_timeout_watchdog_teaser` | 500행. LLM이 만든 장애 대응 대화, 유료 상품의 맛보기판 | 실제 로그가 아님. CC-BY-NC |
| HF `akankshakshurai/cuda-error-resolution-analysis` | PyTorch 포럼 글 덤프 1.5GB | 원인 라벨 없음. 오류 문구는 진짜지만 대부분 `app` |
| HF `Swapnil007-Curious/kanvas-gpu-failure` | 작업 18,890건의 스케줄링 · 측정 수치와 실패 여부 | 로그 문구 없음 |
| HF `project-vajra/nccl-*` | NCCL all-reduce 성능 측정 CSV | 장애 로그 아님 |
| HF `bolu61/loghub_2` | Loghub 2.0 사본 5GB | GPU 로그 아님 |
| HF `lumicero/xids-dataset` | 침입 탐지 데이터 | 이름만 비슷함 (Xid 아님) |
| HF `denpex/denpex-gpu-failure-fixtures` | GitHub와 같은 25건 | 2.2절에 이미 반영 |
| Kaggle `ramprasadatluri/gpu-cluster-straggler-nccl-sync-telemetry` | 스크립트(`generate_data.py`)로 만든 합성 수치 85,000행 | 합성 · 로그 문구 없음 |
| Kaggle `beatsprom/autonomous-cloud-gpu-and-distributed-serving-2026` | LLM 학습용 합성 대화 2,500행 | 실제 로그가 아님 |
| Kaggle `stanislavkublitskiy/t4-gpu-telemetry-baseline-kaggle` | T4 2장의 `nvidia-smi` 측정값, 정상 동작만 | 장애 없음 · 로그 문구 없음 |

**없는 이유**: 운영 로그는 개인정보 · 규정 때문에 공개되지 않고(Delta가 원본 300GB를 뺀 이유), 원인 라벨은 누군가 일부러 장애를 주입해야 생긴다(GF-Bench가 그렇게 했지만 OOM · 스트래글러 두 종류뿐). `network` · `infra` 줄은 우리가 직접 주입해 만든다.

## 3. 우리 장애 주입 → 라벨

주입 종류가 곧 라벨이다. 주입 시각 ~ 복구 구간의 줄에 붙인다. 절차는 `deploy/README.md` 「장애 주입」.

| 주입 | category | 남는 줄 | 상태 |
|---|---|---|---|
| ① trainer 강제 종료 | **미정** (`app` 또는 `network`) | 죽은 Pod: `failed (exitcode: -9)` · 살아 있던 랭크: `NCCL error: remote process exited or there was a network error` | 실행함 (11/23 · 11/25) |
| ② · ③ `dcgmi` Xid 주입 | 라벨할 줄 없음 | `dcgmi` 주입은 커널 로그를 남기지 않는다 | 실행함 |
| ④ GPU 노드 종료 | `infra` (`rules.yaml` `node_lost`) | 미확인 | 미실행 |
| ⑤ 클러스터 전체 | 안 씀 (`site`는 룰 전용) | — | 미실행 |
| ⑥ OOM 두 가지 | `app` | `CUDA out of memory` · 컨테이너 `OOMKilled` | 미실행 |
| ⑦ 스트래글러 | 안 씀 (룰 전용) | — | 미실행 |
| (계획) NaN · 예외 | `app` | traceback | 장애 주입 CLI에 없음 |
| (계획) NCCL 포트 차단 · 패킷 손실 | `network` | NCCL timeout | 장애 주입 CLI에 없음 |
| (계획) S3 권한 회수 · 디스크 채우기 | `infra` | `AccessDenied` · `No space left` | 장애 주입 CLI에 없음 |
| (계획) `/dev/kmsg`에 `NVRM: Xid` 줄 | `hw` (`origin: synthetic`) | 합성 Xid 줄 | 장애 주입 CLI에 없음 |

**①이 미정인 이유**: ADR-45 주입 표는 「rank 하나 SIGKILL」을 `network` 주입으로 적었지만, 분류기 룰(`pod_exit`)은 같은 장애를 `app`으로 낸다. 살아 있던 랭크에 남는 `NCCL error: remote process exited or there was a network error`는 진짜 통신 장애에서도 똑같이 찍히는 문구라, 이 줄만으로는 둘을 가를 수 없다. 포트 차단 주입을 해 보고 두 경우의 줄이 실제로 다른지 본 뒤 정한다.

## 4. 지금 가진 양 (2026-11-26)

| category | 우리 주입 로그 | 외부 보조 |
|---|---|---|
| `app` | 주입 ① 5회(11/23 2회 · 11/25 3회) + 주입이 아닌 코드 오류 1건(11/25 dtype 불일치) | GF-Bench OOM 2회 · Denpex 21건 |
| `none` | 정상 실행 로그 (Loki 30일 보존) | GF-Bench 정상 4회 |
| `network` | 0 (①의 NCCL 줄은 미정) | 0 |
| `infra` | 0 | 0 |
| `hw` | 0 (실제 `NVRM: Xid` 줄이 한 건도 없음, GPU-218) | 0 (합성만 가능) |
| `driver` | 0 | 0 |

**`app` · `none` 말고는 학습할 줄이 없다.** 여섯 클래스 모델을 만들려면 `network` · `infra` 주입이 먼저 있어야 하고, `hw` · `driver`는 합성 줄뿐이다.
