"""The calls stream re-pulls recent calls by call time and skips ids already yielded."""

from unittest.mock import patch

from singer_sdk.streams import RESTStream

from tap_claricopilot.tap import TapClariCopilot


def test_lookback_pass_dedupes_and_filters_by_call_time():
    tap = TapClariCopilot(config={"api_key": "k", "api_password": "p", "start_date": "2026-01-01T00:00:00Z"})
    calls = tap.streams["calls"]
    seen_params = []

    def fake(self, context):
        seen_params.append(self.get_url_params(context, None))
        if self._lookback_pass:
            return iter([{"id": "a"}, {"id": "relinked"}])
        return iter([{"id": "a"}, {"id": "b"}])

    with patch.object(RESTStream, "request_records", fake):
        ids = [r["id"] for r in calls.request_records(None)]

    assert ids == ["a", "b", "relinked"]
    assert "filterTimeGt" not in seen_params[0]
    assert "filterTimeGt" in seen_params[1] and "filterModifiedGt" not in seen_params[1]
    assert calls._lookback_pass is False
