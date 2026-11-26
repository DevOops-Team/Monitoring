import pytest

from classifier.normalize import normalize

# 입력은 Loki 실제 줄(2026-11-21 ~ 11-26). Xid 줄만 NVIDIA 문서 형식(실제 줄 없음, GPU-218)
CASES = [
    # torchrun: 학습 프로세스가 죽음. 종료 코드는 남는다
    ("E1125 07:23:24.110000 1 site-packages/torch/distributed/elastic/multiprocessing/api.py:869] "
     "failed (exitcode: -9) local_rank: 0 (pid: 61) of binary: /opt/conda/bin/python3.11",
     "E<TS> 1 site-packages/torch/distributed/elastic/multiprocessing/api.py:869] "
     "failed (exitcode: -9) local_rank: <RANK> (pid: <PID>) of binary: /opt/conda/bin/python3.11"),
    ("  exitcode  : 1 (pid: 62)", "  exitcode  : 1 (pid: <PID>)"),
    ("  rank      : 1 (local_rank: 0)", "  rank      : <RANK> (local_rank: <RANK>)"),
    ("  time      : 2026-11-25_05:44:34", "  time      : <TS>"),
    ("  host      : job-61fa5acc-g0001-workers-0-1.job-61fa5acc-g0001.training.svc.cluster.local",
     "  host      : <HOST>"),
    # 시그널 번호 · 이름은 남는다
    ("torch.distributed.elastic.multiprocessing.api.SignalException: Process 1 got signal: 15",
     "torch.distributed.elastic.multiprocessing.api.SignalException: Process <PID> got signal: 15"),
    ("W1123 07:17:37.869000 1 site-packages/torch/distributed/elastic/multiprocessing/api.py:897] "
     "Sending process 61 closing signal SIGTERM",
     "W<TS> 1 site-packages/torch/distributed/elastic/multiprocessing/api.py:897] "
     "Sending process <PID> closing signal SIGTERM"),
    # NCCL: 일련번호는 지우고 Timeout · 버전은 남긴다
    ("[rank0]:[E1123 06:40:49.705784326 ProcessGroupNCCL.cpp:542] [Rank 0] Collective WorkNCCL(SeqNum=44909, "
     "OpType=_REDUCE_SCATTER_BASE, NumelIn=3540096, NumelOut=1770048, Timeout(ms)=600000) raised the following "
     "async exception: NCCL error: remote process exited or there was a network error, NCCL version 2.21.5",
     "[rank<RANK>]:[E<TS> ProcessGroupNCCL.cpp:542] [Rank <RANK>] Collective WorkNCCL(SeqNum=<SEQ>, "
     "OpType=_REDUCE_SCATTER_BASE, NumelIn=<SEQ>, NumelOut=<SEQ>, Timeout(ms)=600000) raised the following "
     "async exception: NCCL error: remote process exited or there was a network error, NCCL version 2.21.5"),
    ("[rank0]:[E1123 05:58:08.631128300 ProcessGroupNCCL.cpp:1785] [PG ID 0 PG GUID 0(default_pg) Rank 0] Exception "
     "(either an error or timeout) detected by watchdog at work: 50043, last enqueued NCCL work: 50043, "
     "last completed NCCL work: 50042.",
     "[rank<RANK>]:[E<TS> ProcessGroupNCCL.cpp:1785] [PG ID 0 PG GUID 0(default_pg) Rank <RANK>] Exception "
     "(either an error or timeout) detected by watchdog at work: <SEQ>, last enqueued NCCL work: <SEQ>, "
     "last completed NCCL work: <SEQ>."),
    ("frame #0: c10::Error::Error(c10::SourceLocation, std::string) + 0x96 "
     "(0x7fd645db9446 in /opt/conda/lib/python3.11/site-packages/torch/lib/libc10.so)",
     "frame #0: c10::Error::Error(c10::SourceLocation, std::string) + <ADDR> "
     "(<ADDR> in /opt/conda/lib/python3.11/site-packages/torch/lib/libc10.so)"),
    # 소스 줄 번호 · 오류 문구는 남는다
    ('[rank1]:   File "/data/code/train.py", line 111, in forward',
     '[rank<RANK>]:   File "/data/code/train.py", line 111, in forward'),
    ("[rank1]: RuntimeError: expected mat1 and mat2 to have the same dtype, but got: float != c10::Half",
     "[rank<RANK>]: RuntimeError: expected mat1 and mat2 to have the same dtype, but got: float != c10::Half"),
    # ckpt-sidecar JSON
    ('{"ts": "2026-11-25T08:00:14.609Z", "event": "events.delivered", "topic": "job.lifecycle", '
     '"key": "11108000", "partition": 2, "offset": 1032}',
     '{"ts": "<TS>", "event": "events.delivered", "topic": "job.lifecycle", '
     '"key": "<JOB>", "partition": <SEQ>, "offset": <SEQ>}'),
    ('{"ts": "2026-11-23T07:11:12.666Z", "event": "sidecar.uploaded", "step": 517, "rank": 3, "files": 1, '
     '"bytes": 432928628, "upload_ms": 3629}',
     '{"ts": "<TS>", "event": "sidecar.uploaded", "step": <SEQ>, "rank": <RANK>, "files": 1, '
     '"bytes": <SEQ>, "upload_ms": <SEQ>}'),
    # EKS 커널
    ("ena 0000:00:06.0: Elastic Network Adapter (ENA) found at mem c1510000, mac addr 06:ff:f5:b9:a6:e5",
     "ena <PCI>: Elastic Network Adapter (ENA) found at mem c1510000, mac addr <MAC>"),
    ("zram0: system has too much memory (15785MB), limit is 800MB, ignoring.",
     "zram0: system has too much memory (<SIZE>), limit is <SIZE>, ignoring."),
    # Xid 번호는 남는다
    ("NVRM: Xid (PCI:0000:00:1e): 79, pid=1234, name=python3, GPU has fallen off the bus.",
     "NVRM: Xid (PCI:<PCI>): 79, pid=<PID>, name=python3, GPU has fallen off the bus."),
    # NCCL 소켓: 상대 이름 뒤 포트
    ("socketProgress: Connection closed by remote peer "
     "job-5983f88d-g0001-workers-0-1.job-5983f88d-g0001.training.svc.cluster.local<38056>",
     "socketProgress: Connection closed by remote peer <HOST><PORT>"),
    # ckpt-sidecar의 Kafka 클라이언트 줄: 머리의 epoch 시각. 브로커 번호(/3) · 대기 시간은 남는다
    ("%4|1795501416.117|FAIL|job-d00dfb6b-g0002-workers-0-0#producer-1| [thrd:172.30.0.42:9092/3]: "
     "172.30.0.42:9092/3: Connection setup timed out in state CONNECT (after 30026ms in state CONNECT)",
     "%4|<TS>|FAIL|<HOST>#producer-1| [thrd:<IP>/3]: "
     "<IP>/3: Connection setup timed out in state CONNECT (after 30026ms in state CONNECT)"),
    # trainer 진행 줄: step 번호 · 걸린 시간은 지우고 랭크 수 · 배치 설정은 남긴다
    ("step 517: saved step-00000517 in 4.4s", "step <SEQ>: saved step-<SEQ> in <DUR>"),
    ("step time 1.332s, expected 137.8 min, interval 594s -> save every 478 steps (13 saves)",
     "step time <DUR>, expected <DUR>, interval <DUR> -> save every <SEQ> steps (<SEQ> saves)"),
    ("fsdp x 4: 2485 samples, 155 steps/epoch, 1550 steps",
     "fsdp x 4: <SEQ> samples, <SEQ> steps/epoch, <SEQ> steps"),
    ("fsdp qlora FULL_SHARD x 4: 9766 blocks, micro batch 4 x accum 4, 152 steps/epoch, 304 steps",
     "fsdp qlora FULL_SHARD x 4: <SEQ> blocks, micro batch 4 x accum 4, <SEQ> steps/epoch, <SEQ> steps"),
    ("rank 0: STOP at step 43", "rank <RANK>: STOP at step <SEQ>"),
    ("resumed from step-00000500", "resumed from step-<SEQ>"),
    # 그 밖
    ("node ip-172-28-69-78.us-east-2.compute.internal GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7 at 172.30.0.35:9009",
     "node <HOST> <UUID> at <IP>"),
    ("torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB. GPU 0 has a total capacity of 14.57 GiB",
     "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate <SIZE>. GPU 0 has a total capacity of <SIZE>"),
]


@pytest.mark.parametrize("line, expected", CASES)
def test_normalize(line, expected):
    assert normalize(line) == expected
    assert normalize(expected) == expected      # 이미 정규화한 줄은 그대로


def test_lines_differing_only_in_noise_become_equal():
    a = "E1125 05:44:34.219000 1 api.py:869] failed (exitcode: 1) local_rank: 0 (pid: 62)"
    b = "E1125 07:01:02.000001 1 api.py:869] failed (exitcode: 1) local_rank: 3 (pid: 9871)"
    assert normalize(a) == normalize(b)


def test_exit_code_difference_survives():
    a = "E1125 05:44:34.219000 1 api.py:869] failed (exitcode: 1) local_rank: 0 (pid: 62)"
    b = "E1125 05:44:34.219000 1 api.py:869] failed (exitcode: -9) local_rank: 0 (pid: 62)"
    assert normalize(a) != normalize(b)
