from datetime import datetime, timedelta, timezone

from classifier.intended import Lifecycle

NOW = datetime(2026, 11, 18, 14, 0, 0, tzinfo=timezone.utc)


def event(type_, job_id="3f9a1c2e", generation=3):
    return {"type": type_, "job_id": job_id, "generation": generation}


def test_intended_and_latest():
    life = Lifecycle()
    for e in (event("job.submitted", generation=1), event("job.fenced", generation=1),
              event("job.submitted", generation=2), event("job.failed", generation=2)):
        life.apply(e, NOW)
    assert life.intended("3f9a1c2e", 1) and not life.intended("3f9a1c2e", 2)
    assert life.latest("3f9a1c2e") == 2 and life.latest("other") is None


def test_out_of_order_does_not_lower_latest():
    life = Lifecycle()
    life.apply(event("job.submitted", generation=3), NOW)
    life.apply(event("job.fenced", generation=2), NOW)
    assert life.latest("3f9a1c2e") == 3


def test_events_without_job_or_generation_are_skipped():
    life = Lifecycle()
    for e in ({"type": "experiment.started"}, event("job.fenced", job_id=None), event("job.fenced", generation="3"),
              event("job.fenced", generation=True)):
        life.apply(e, NOW)
    assert life.latest("3f9a1c2e") is None


def test_prune_after_24h():
    life = Lifecycle()
    life.apply(event("job.fenced"), NOW)
    life.prune(NOW + timedelta(hours=23))
    assert life.intended("3f9a1c2e", 3)
    life.prune(NOW + timedelta(hours=25))
    assert not life.intended("3f9a1c2e", 3) and life.latest("3f9a1c2e") is None


def progress(step, total_steps=None, generation=3):
    payload = {"status": "RUNNING", "step": step, **({} if total_steps is None else {"total_steps": total_steps})}
    return {**event("job.progress", generation=generation), "payload": payload}


def test_remaining_steps_from_progress():
    life = Lifecycle()
    assert life.remaining_steps("3f9a1c2e", 3) is None          # 아직 진행률을 못 봄
    life.apply(progress(1200, 2080), NOW)
    assert life.remaining_steps("3f9a1c2e", 3) == 880
    life.apply(progress(1300), NOW)                             # total_steps가 빠진 사건은 앞 값을 유지
    assert life.remaining_steps("3f9a1c2e", 3) == 780
    life.apply(progress(1250, 2080), NOW)                       # 순서가 뒤바뀐 옛 값은 무시
    assert life.remaining_steps("3f9a1c2e", 3) == 780
    assert life.remaining_steps("3f9a1c2e", 2) is None          # 다른 세대의 값은 쓰지 않는다


def test_progress_of_new_generation_replaces_old():
    life = Lifecycle()
    life.apply(progress(1200, 2080), NOW)
    life.apply(progress(700, generation=4), NOW)                # 새 세대인데 total_steps를 아직 모름
    assert life.remaining_steps("3f9a1c2e", 4) is None
    life.apply(progress(710, 2080, generation=4), NOW)
    assert life.remaining_steps("3f9a1c2e", 4) == 1370
    life.apply({**event("job.progress"), "payload": {"step": "x"}}, NOW)   # 형식이 틀린 payload는 무시
    assert life.remaining_steps("3f9a1c2e", 4) == 1370
    life.prune(NOW + timedelta(hours=25))
    assert life.remaining_steps("3f9a1c2e", 4) is None
