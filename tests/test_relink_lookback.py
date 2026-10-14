"""filterModifiedGt uses the earlier of the bookmark and now - relink_lookback_hours."""

from datetime import datetime, timedelta, timezone

from tap_claricopilot.tap import TapClariCopilot


def _since(bookmark):
    tap = TapClariCopilot(config={"api_key": "k", "api_password": "p", "start_date": "2026-01-01T00:00:00Z", "relink_lookback_hours": 72})
    calls = tap.streams["calls"]
    calls.get_starting_timestamp = lambda context: bookmark
    raw = calls.get_url_params(None, None)["filterModifiedGt"]
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def test_recent_bookmark_rewinds_to_lookback():
    now = datetime.now(timezone.utc)
    assert abs(_since(now - timedelta(minutes=15)) - (now - timedelta(hours=72))) < timedelta(minutes=1)


def test_old_bookmark_wins():
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert _since(old) == old
