"""스팟 회수 예고를 지표로 낸다 (GPU-219). 표준 라이브러리만 쓴다.

EC2는 스팟 인스턴스를 회수하기 2분 전에 그 인스턴스의 IMDS spot/instance-action에 {"action", "time"}을 올린다(평소엔 404).
5초마다(AWS 권장 주기) 읽어 :9105/metrics로 연다. Agent → Mimir 룰 FailoverSpotNotice → Alertmanager → 분류기.

  spot_interruption_notice_timestamp_seconds     예고를 처음 본 시각. 예고가 있을 때만 나온다(룰의 event_ts)
  spot_interruption_scheduled_timestamp_seconds  AWS가 알려 준 회수 시각. 예고가 있을 때만 나온다
  spot_notice_imds_ok                            마지막 조회가 됐는가(404도 된 것). 0이면 예고를 못 본다
"""
import json
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

IMDS = os.environ.get("IMDS_URL", "http://169.254.169.254")    # 로컬 시험에서만 바꾼다
state = {"ok": None, "first_seen": None, "scheduled": None}     # ok: 첫 조회 전 None


def fetch():
    """예고가 있으면 dict, 없으면(404) None."""
    token_req = urllib.request.Request(f"{IMDS}/latest/api/token", method="PUT",
                                       headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    token = urllib.request.urlopen(token_req, timeout=2).read().decode()
    req = urllib.request.Request(f"{IMDS}/latest/meta-data/spot/instance-action",
                                 headers={"X-aws-ec2-metadata-token": token})
    try:
        return json.load(urllib.request.urlopen(req, timeout=2))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def poll():
    while True:
        try:
            notice = fetch()
            if notice is None:
                state.update(first_seen=None, scheduled=None)
            elif state["first_seen"] is None:      # 처음 본 시각은 예고가 있는 동안 바꾸지 않는다(분류기 중복 제거 기준)
                scheduled = datetime.fromisoformat(notice["time"].replace("Z", "+00:00")).timestamp()
                state.update(first_seen=time.time(), scheduled=scheduled)
                print(f"spot notice: {notice}", flush=True)
            state["ok"] = 1
        except Exception as e:      # IMDS가 안 닿아도 죽지 않고 imds_ok 0으로 알린다
            if state["ok"] != 0:    # 계속 실패하는 동안은 처음 한 번만 적는다
                print(f"imds failed: {e!r}", flush=True)
            state["ok"] = 0
        time.sleep(5)


def render():
    lines = ["# TYPE spot_notice_imds_ok gauge", f"spot_notice_imds_ok {state['ok'] or 0}"]
    if state["first_seen"] is not None:
        lines += ["# TYPE spot_interruption_notice_timestamp_seconds gauge",
                  f"spot_interruption_notice_timestamp_seconds {state['first_seen']:.3f}",
                  "# TYPE spot_interruption_scheduled_timestamp_seconds gauge",
                  f"spot_interruption_scheduled_timestamp_seconds {state['scheduled']:.3f}"]
    return ("\n".join(lines) + "\n").encode()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = render()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):   # 15초마다 긁히는 줄을 로그에 남기지 않는다
        pass


if __name__ == "__main__":
    threading.Thread(target=poll, daemon=True).start()
    ThreadingHTTPServer(("", 9105), Handler).serve_forever()
