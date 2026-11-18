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
