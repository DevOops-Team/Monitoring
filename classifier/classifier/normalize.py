"""로그 줄 정규화 (ADR-45, GPU-191).

매번 달라지지만 원인과 상관없는 값(시각 · 호스트 · PID …)을 자리표시자로 바꾼다.
모델 학습 스크립트와 분류기 추론이 이 함수 하나를 같이 쓴다(전처리가 어긋나면 모델이 배운 적 없는 줄을 받는다).
아직 판정 경로에는 연결하지 않았다(docs/classifier.md 5절).

이름을 아는 패턴만 바꾼다. 그 밖의 숫자(종료 코드 · 시그널 · Xid 번호 · 버전 · 소스 줄 번호)는 판정에 쓰일 수 있어 남긴다.
패턴은 Loki 실제 줄(2026-11-21 ~ 11-26, trainer · ckpt-sidecar · EKS 커널)로 잡았다. 새 패턴이 보이면 여기에 추가한다.
"""
from __future__ import annotations

import re

_HEX = r"[0-9a-fA-F]"

# 위에서부터 차례로 적용한다. 순서를 바꾸면 결과가 달라진다(시각 → 이름 → 주소 → 번호).
_PATTERNS: list[tuple[re.Pattern[str], str]] = [(re.compile(p), r) for p, r in [
    # 시각: ISO("ts": "2026-11-23T06:53:38.100Z") · torchrun 요약(2026-11-25_05:44:34)
    (r"\d{4}-\d{2}-\d{2}[T_ ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?", "<TS>"),
    # 시각: glog(W1123 07:17:37.869000). 앞 글자(I · W · E · F)는 로그 수준이라 남긴다
    (r"\b([IWEF])\d{4} \d{2}:\d{2}:\d{2}\.\d+", r"\1<TS>"),
    # 시각: Kafka 클라이언트(librdkafka) 줄 머리의 epoch 초(%4|1795416750.404|GETPID|…). 앞 숫자는 로그 수준
    (r"^(%\d\|)\d+\.\d+(?=\|)", r"\1<TS>"),
    # EC2 노드 이름(ip-172-28-69-78.us-east-2.compute.internal)
    (r"\bip(?:-\d{1,3}){4}(?:\.[a-z0-9-]+)*", "<HOST>"),
    # 학습 Pod 이름 · DNS 이름(job-61fa5acc-g0001-workers-0-1.job-61fa5acc-g0001.training.svc.cluster.local)
    (r"\bjob-[0-9a-f]{8}-g\d{4}[a-z0-9-]*(?:\.[a-z0-9-]+)*", "<HOST>"),
    # 작업 ID가 JSON 값으로 찍힌 곳(ckpt-sidecar "key": "11108000")
    (r'("(?:key|job_id)": ")[0-9a-f]{8}(")', r"\1<JOB>\2"),
    # GPU UUID(GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7)와 그 밖의 UUID
    (rf"\b(?:GPU-)?{_HEX}{{8}}(?:-{_HEX}{{4}}){{3}}-{_HEX}{{12}}\b", "<UUID>"),
    (rf"\b{_HEX}{{2}}(?::{_HEX}{{2}}){{5}}\b", "<MAC>"),
    # PCI 주소: 커널(0000:00:06.0) · Xid 줄(PCI:0000:00:1e).
    # TODO(GPU-218): 실제 NVRM: Xid 줄이 아직 없어 NVIDIA 문서 형식으로만 맞췄다
    (rf"\b{_HEX}{{4}}:{_HEX}{{2}}:{_HEX}{{2}}(?:\.[0-7])?\b", "<PCI>"),
    (r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{2,5})?\b", "<IP>"),
    # NCCL이 상대 이름 뒤에 붙이는 포트(Connection closed by remote peer …svc.cluster.local<38056>)
    (r"(<HOST>|<IP>)<\d+>", r"\1<PORT>"),
    # 메모리 주소 · 오프셋(frame #0: … + 0x96 (0x7fd645db9446 in …))
    (rf"\b0x{_HEX}+\b", "<ADDR>"),
    # PID: (pid: 62) · pid=1234 · Sending process 61 · Process 1 got signal
    (r"\b((?:pid[:=]\s*)|(?:[Pp]rocess ))\d+", r"\1<PID>"),
    # 랭크: [rank1]: · Rank 0] · rank : 1 (local_rank: 0) · "rank": 3
    (r'\b((?:local_)?[Rr]ank"?\s*[:=]?\s*)\d+', r"\1<RANK>"),
    # 메모리 크기(15785MB · 2.00 GiB)
    (r"\b\d+(?:\.\d+)?\s?(?:[KMGT]i?B)\b", "<SIZE>"),
    # 이름이 붙은 일련번호 · 크기
    (r"\b((?:SeqNum|NumelIn|NumelOut)=)\d+", r"\1<SEQ>"),
    (r"\b((?:at work|NCCL work): )\d+", r"\1<SEQ>"),
    (r'("(?:offset|partition|step|approved_step|bytes|upload_ms)": )\d+', r"\1<SEQ>"),
    # trainer 진행 줄(Data 레포 train.py가 찍는다. 문구가 바뀌면 숫자가 다시 남는다).
    # 장애 줄의 앞뒤 문맥으로 같이 들어가므로 step 번호 · 걸린 시간을 지운다. 랭크 수 · 배치 설정(fsdp x 4)은 남긴다
    (r"\b(step[- ])\d+", r"\1<SEQ>"),
    (r"\b\d+(?= (?:samples|blocks|steps/epoch|steps|saves)\b)", "<SEQ>"),
    (r"\b((?:step time|interval|in) )\d+(?:\.\d+)?s\b", r"\1<DUR>"),
    (r"\b(expected )\d+(?:\.\d+)? min\b", r"\1<DUR>"),
]]


def normalize(line: str) -> str:
    """로그 줄 하나를 정규화한다. 앞뒤 문맥 줄도 줄마다 이 함수를 거친다."""
    for pattern, repl in _PATTERNS:
        line = pattern.sub(repl, line)
    return line
