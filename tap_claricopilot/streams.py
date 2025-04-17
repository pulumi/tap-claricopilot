"""Stream type classes for tap-claricopilot."""

from __future__ import annotations

import json
import typing as t
import decimal
from datetime import datetime
from importlib import resources
from urllib.parse import urljoin

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.helpers.types import Context
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_claricopilot.client import ClariCopilotStream

# Create a custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that can handle Decimal types."""
    
    def default(self, o):
        """Convert Decimal to float for JSON serialization.
        
        Args:
            o: The object to encode
            
        Returns:
            A JSON serializable version of the object
        """
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ClariPaginator(BaseAPIPaginator):
    """Paginator for Clari Copilot API that uses skip/limit pagination."""

    def __init__(
        self,
        start_value: int = 0,
        page_size: int = 50,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the paginator.

        Args:
            start_value: Initial skip value
            page_size: Number of records per page
            *args: Variable length argument list to pass to the base class
            **kwargs: Arbitrary keyword arguments to pass to the base class
        """
        super().__init__(start_value, *args, **kwargs)
        self._limit = page_size
        self._current_skip = start_value

    def get_next(self, response):
        """Get the next page token from the response.

        Args:
            response: API response object

        Returns:
            The next page token or None if no more pages
        """
        data = response.json()
        calls = data.get("calls", [])
        
        # If fewer records than limit were returned, we've reached the end
        if len(calls) < self._limit:
            return None
        
        # Calculate next skip value
        self._current_skip += self._limit
        return self._current_skip

    @property
    def limit(self) -> int:
        """Return the page size limit.

        Returns:
            The maximum number of records per page
        """
        return self._limit


class CallsStream(ClariCopilotStream):
    """Stream for Clari Copilot calls."""

    name = "calls"
    path = "/calls"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "last_modified_time"
    records_jsonpath = "$.calls[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Unique call identifier"),
        th.Property("source_id", th.StringType, description="Source identifier from the calling platform"),
        th.Property("title", th.StringType, description="Call title/topic"),
        th.Property(
            "users", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("userId", th.StringType),
                    th.Property("userEmail", th.StringType),
                    th.Property("isOrganizer", th.BooleanType),
                    th.Property("personId", th.IntegerType),
                )
            ), 
            description="Internal users in the call"
        ),
        th.Property(
            "externalParticipants", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("phone", th.StringType),
                    th.Property("personId", th.IntegerType),
                )
            ), 
            description="External participants invited to the call"
        ),
        th.Property(
            "joinedParticipants", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("phone", th.StringType),
                    th.Property("personId", th.IntegerType),
                )
            ), 
            description="Participants who joined the call"
        ),
        th.Property("status", th.StringType, description="Call status"),
        th.Property("bot_not_join_reason", th.ArrayType(th.StringType), description="Reasons the bot didn't join"),
        th.Property("type", th.StringType, description="Call type (e.g., ZOOM, MS_TEAMS)"),
        th.Property("time", th.DateTimeType, description="Call start/scheduled time"),
        th.Property("icaluid", th.StringType, description="iCal UID for the event"),
        th.Property("calendar_id", th.StringType, description="Calendar ID"),
        th.Property("recurring_event_id", th.StringType, description="ID for recurring events"),
        th.Property("original_start_time", th.DateTimeType, description="Original start time for rescheduled calls"),
        th.Property("last_modified_time", th.DateTimeType, description="When the call was last modified"),
        th.Property("audio_url", th.StringType, description="URL to call audio recording"),
        th.Property("video_url", th.StringType, description="URL to call video recording"),
        th.Property("disposition", th.StringType, description="Call disposition"),
        th.Property("deal_name", th.StringType, description="Associated deal name"),
        th.Property("deal_value", th.StringType, description="Associated deal value"),
        th.Property("deal_close_date", th.DateTimeType, description="Associated deal close date"),
        th.Property("deal_stage_before_call", th.StringType, description="Deal stage before the call"),
        th.Property("account_name", th.StringType, description="Associated account name"),
        th.Property("contact_names", th.ArrayType(th.StringType), description="Associated contact names"),
        th.Property(
            "crm_info", 
            th.ObjectType(
                th.Property("source_crm", th.StringType),
                th.Property("deal_id", th.StringType),
                th.Property("account_id", th.StringType),
                th.Property("contact_ids", th.ArrayType(th.StringType)),
            ),
            description="CRM information associated with the call"
        ),
        th.Property("bookmark_timestamps", th.ArrayType(th.DateTimeType), description="Bookmark timestamps"),
        th.Property("metrics", th.StringType, description="Call metrics and analytics as a JSON string", nullable=True),
        th.Property("call_review_page_url", th.StringType, description="URL to review the call"),
    ).to_dict()

    def get_new_paginator(self) -> ClariPaginator:
        """Create a new pagination helper instance.

        Returns:
            A pagination helper instance.
        """
        return ClariPaginator(page_size=100)  # Use max page size for efficiency

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page token (skip value).

        Returns:
            A dictionary of URL parameters.
        """
        params: dict = {
            "limit": 100,  # Maximum allowed limit
            "includePagination": "false",  # Improve performance as mentioned in docs
            "includePrivate": "false",  # Exclude private calls
            "filterStatus": ["PROCESSED", "POST_PROCESSING_DONE"]  # Only get calls that have been processed
        }

        # Add skip parameter for pagination
        if next_page_token:
            params["skip"] = next_page_token

        # Add filterModifiedGt parameter if we have a state bookmark
        if self.replication_key and self.get_starting_timestamp(context):
            start_time = self.get_starting_timestamp(context)
            if start_time:
                # Format as ISO 8601
                params["filterModifiedGt"] = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        return params
    
    def get_child_context(self, record: dict, context: t.Optional[Context]) -> dict:
        """Return a context dictionary for child streams.

        Args:
            record: The current record.
            context: The stream context.

        Returns:
            A context dictionary for child streams.
        """
        return {
            "call_id": record["id"],
        }
    
    def parse_response(self, response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP response object.

        Yields:
            Each record from the source.
        """
        # Extract records using jsonpath
        for record in super().parse_response(response):
            # Convert string timestamps to datetime objects for proper state tracking
            if "last_modified_time" in record and record["last_modified_time"]:
                # Convert string to datetime if it's a string
                if isinstance(record["last_modified_time"], str):
                    try:
                        record["last_modified_time"] = datetime.fromisoformat(
                            record["last_modified_time"].replace("Z", "+00:00")
                        )
                    except ValueError:
                        self.logger.warning(f"Could not parse last_modified_time timestamp: {record['last_modified_time']}")
                        
            # Handle metrics - either convert to string or remove if can't be serialized
            if "metrics" in record:
                if record["metrics"] is not None:
                    try:
                        # Store metrics object as JSON string using custom encoder for Decimal types
                        metrics_obj = record["metrics"]
                        record["metrics"] = json.dumps(metrics_obj, cls=DecimalEncoder)
                    except (TypeError, ValueError) as e:
                        # If we can't serialize it, remove it to avoid schema validation errors
                        self.logger.warning(f"Failed to serialize metrics to JSON - removing field: {e}")
                        del record["metrics"]
            
            yield record


class CallDetailsStream(ClariCopilotStream):
    """Stream for Clari Copilot call details."""

    name = "call_details"
    # The correct endpoint is /call-details with callId as a query parameter
    path = "/call-details"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None  # This is a child stream, no need for incremental load
    parent_stream_type = CallsStream
    records_jsonpath = "$.call"
    ignore_parent_replication_keys = False
    
    def validate_response(self, response):
        """Handle API response validation.
        
        Args:
            response: The HTTP response object
            
        Raises:
            FatalAPIError: If the request is not retriable
            RetriableAPIError: If the request should be retried
        """
        if response.status_code == 404:
            # Log the 404 but don't raise an exception - consider it a valid response
            self.logger.info(f"Call details not found for call {response.request.url}, skipping")
            return
            
        # For other status codes, use the parent class validation
        super().validate_response(response)

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Unique call identifier"),
        th.Property("source_id", th.StringType, description="Source identifier from the calling platform"),
        th.Property("title", th.StringType, description="Call title/topic"),
        th.Property(
            "users", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("userId", th.StringType),
                    th.Property("userEmail", th.StringType),
                    th.Property("isOrganizer", th.BooleanType),
                    th.Property("personId", th.IntegerType),
                )
            ), 
            description="Internal users in the call"
        ),
        th.Property(
            "externalParticipants", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("phone", th.StringType),
                    th.Property("personId", th.IntegerType),
                )
            ), 
            description="External participants invited to the call"
        ),
        th.Property(
            "joinedParticipants", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("phone", th.StringType),
                    th.Property("personId", th.IntegerType),
                )
            ), 
            description="Participants who joined the call"
        ),
        th.Property("status", th.StringType, description="Call status"),
        th.Property("bot_not_join_reason", th.ArrayType(th.StringType), description="Reasons the bot didn't join"),
        th.Property("type", th.StringType, description="Call type (e.g., ZOOM, MS_TEAMS)"),
        th.Property("time", th.DateTimeType, description="Call start/scheduled time"),
        th.Property("icaluid", th.StringType, description="iCal UID for the event"),
        th.Property("calendar_id", th.StringType, description="Calendar ID"),
        th.Property("recurring_event_id", th.StringType, description="ID for recurring events"),
        th.Property("original_start_time", th.DateTimeType, description="Original start time for rescheduled calls"),
        th.Property("last_modified_time", th.DateTimeType, description="When the call was last modified"),
        th.Property("audio_url", th.StringType, description="URL to call audio recording"),
        th.Property("video_url", th.StringType, description="URL to call video recording"),
        th.Property("disposition", th.StringType, description="Call disposition"),
        th.Property("deal_name", th.StringType, description="Associated deal name"),
        th.Property("deal_value", th.StringType, description="Associated deal value"),
        th.Property("deal_close_date", th.DateTimeType, description="Associated deal close date"),
        th.Property("deal_stage_before_call", th.StringType, description="Deal stage before the call"),
        th.Property("deal_stage_live", th.StringType, description="Deal stage during the call"),
        th.Property("account_name", th.StringType, description="Associated account name"),
        th.Property("contact_names", th.ArrayType(th.StringType), description="Associated contact names"),
        th.Property(
            "crm_info", 
            th.ObjectType(
                th.Property("source_crm", th.StringType),
                th.Property("deal_id", th.StringType),
                th.Property("account_id", th.StringType),
                th.Property("contact_ids", th.ArrayType(th.StringType)),
            ),
            description="CRM information associated with the call"
        ),
        th.Property("bookmark_timestamps", th.ArrayType(th.DateTimeType), description="Bookmark timestamps"),
        th.Property("metrics", th.StringType, description="Call metrics and analytics as a JSON string", nullable=True),
        th.Property("call_review_page_url", th.StringType, description="URL to review the call"),
        th.Property(
            "transcript", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("text", th.StringType),
                    th.Property("start", th.NumberType),
                    th.Property("end", th.NumberType),
                    th.Property("personId", th.NumberType),
                    th.Property(
                        "annotations", 
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tracker", th.StringType),
                                th.Property("phrase", th.StringType),
                                th.Property("category", th.StringType),
                            )
                        )
                    ),
                )
            ),
            description="Call transcript segments"
        ),
        th.Property(
            "summary",
            th.ObjectType(
                th.Property("full_summary", th.StringType, description="Complete call summary"),
                th.Property(
                    "topics_discussed", 
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("start_timestamp", th.StringType),
                            th.Property("end_timestamp", th.StringType),
                            th.Property("summary", th.StringType),
                        )
                    ),
                    description="Topics discussed during the call"
                ),
                th.Property(
                    "key_action_items", 
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("action_item", th.StringType),
                            th.Property("speaker_name", th.StringType),
                            th.Property("start_timestamp", th.StringType),
                            th.Property("end_timestamp", th.StringType),
                        )
                    ),
                    description="Action items identified in the call"
                ),
            ),
            description="Call summary information"
        ),
        th.Property(
            "competitor_sentiments", 
            th.ArrayType(
                th.ObjectType(
                    th.Property("competitor_name", th.StringType),
                    th.Property("sentiment", th.StringType),
                    th.Property("reasoning", th.StringType),
                    th.Property("personId", th.StringType),
                    th.Property("turn_start_time", th.StringType),
                )
            ),
            description="Competitor mentions and sentiment analysis"
        ),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page token (skip value).

        Returns:
            A dictionary of URL parameters.
        """
        params: dict = {
            "includeTranscript": "true",
            "includeSummary": "true",
        }
        
        # Get the call ID from the parent record's context
        if context and context.get("call_id"):
            # The parameter is named "id" in the API, not "callId"
            params["id"] = context["call_id"]
            self.logger.info(f"Fetching details for call_id: {context['call_id']}")
        else:
            self.logger.warning("No call_id in context, cannot fetch call details")
        
        return params
    
    def parse_response(self, response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP response object.

        Yields:
            Each record from the source.
        """
        # Handle 404 responses
        if response.status_code == 404:
            self.logger.info(f"No details found for call, skipping")
            return
            
        # For successful responses, process the record
        try:
            response_json = response.json()
            
            # Log the keys in the response to understand what's coming back
            self.logger.info(f"Response keys: {list(response_json.keys() if isinstance(response_json, dict) else [])}")
            
            # Get the call object from the response
            if isinstance(response_json, dict) and "call" in response_json:
                record = response_json["call"]
            else:
                self.logger.warning("No 'call' key found in response")
                return
                
            # Convert metrics to JSON string
            if "metrics" in record and record["metrics"] is not None:
                try:
                    # Store the metrics object as JSON string using custom encoder for Decimal types
                    metrics_obj = record["metrics"]
                    record["metrics"] = json.dumps(metrics_obj, cls=DecimalEncoder)
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"Failed to serialize metrics to JSON - removing field: {e}")
                    # If we can't serialize it, remove it to avoid schema validation errors
                    del record["metrics"]
                        
            yield record
            
        except Exception as e:
            self.logger.error(f"Error processing call details response: {e}")
            self.logger.error(f"Response text: {response.text[:500]}")  # Log the first 500 chars of the response
            return
