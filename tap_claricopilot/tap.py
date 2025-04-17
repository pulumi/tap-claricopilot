"""ClariCopilot tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_claricopilot import streams


class TapClariCopilot(Tap):
    """ClariCopilot tap class."""

    name = "tap-claricopilot"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="API Key",
            description="The API key for Clari Copilot (found in workspace settings > integrations)",
        ),
        th.Property(
            "api_password",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Password",
            description="The API password for Clari Copilot (found in workspace settings > integrations)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://rest-api.copilot.clari.com",
            description="The url for the API service",
        ),
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.ClariCopilotStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CallsStream(self),
            streams.CallDetailsStream(self),
        ]


if __name__ == "__main__":
    TapClariCopilot.cli()
