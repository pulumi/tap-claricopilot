# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
This is a Singer tap for Clari Copilot, built with the Meltano Singer SDK. The tap extracts data from the Clari Copilot API, focusing on call data and call details. 

## API Details
- Base URL: `https://rest-api.copilot.clari.com`
- Authentication: Requires both X-Api-Key and X-Api-Password headers
- Endpoints:
  - `/calls` - Lists calls with filtering and pagination
  - `/call-details` - Gets detailed information about a specific call using the id parameter

## Build & Test Commands
- Install dependencies: `uv sync`
- Run all tests: `uv run pytest`
- Run single test: `uv run pytest tests/test_core.py::TestTapClariCopilot::test_standard_tap_tests`
- Code linting: `uv run ruff lint`
- Type checking: `uv run mypy`
- Run tap directly: `uv run tap-claricopilot --config CONFIG`
- Run extraction: `meltano elt tap-claricopilot target-jsonl`

## Configuration
The tap requires the following configuration parameters:
- `api_key`: The API key for Clari Copilot (found in workspace settings > integrations)
- `api_password`: The API password for Clari Copilot
- `api_url`: The URL for the API (defaults to https://rest-api.copilot.clari.com)
- `start_date`: The earliest date to extract data from

## Streams
1. **calls**: Lists all calls with pagination and filtering
   - Primary key: id
   - Replication key: last_modified_time
   - Filters: Includes only PROCESSED/POST_PROCESSING_DONE calls by default

2. **call_details**: Gets detailed information for each call
   - Parent stream: calls
   - Uses call id from parent stream
   - Contains transcript, summary, and metrics data

## Code Style Guidelines
- Python 3.9+ compatibility required
- Use typing annotations for all functions (Singer SDK style)
- Follow Google docstring style convention
- Import order: standard lib, 3rd party libs, local modules
- Use `from __future__ import annotations`
- Type check conditionals with `if t.TYPE_CHECKING:`
- Use t.ClassVar for class variables in type hints
- Ruff linting with "ALL" selectors enforced
- Proper error handling with SDK patterns
- RESTStream class pattern for API interactions

## Special Handling
- Decimal values in metrics need to be serialized with a custom JSON encoder
- Error handling added for 404 responses when call details aren't found
- Complex object structure for transcript and summary fields