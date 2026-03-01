"""Load NYC taxi trip data from a paginated REST API into DuckDB using dlt."""

from typing import Iterator, List, Dict, Any, Optional

import dlt
import requests


API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="taxi_trips", write_disposition="replace")
def taxi_trips(max_pages: Optional[int] = None) -> Iterator[List[Dict[str, Any]]]:
    """Yield taxi trip records page by page until the API returns no data."""
    page = 1

    while True:
        if max_pages is not None and page > max_pages:
            break

        response = requests.get(API_URL, params={"page": page}, timeout=30)
        response.raise_for_status()
        records = response.json()

        if not isinstance(records, list):
            raise ValueError("Unexpected API response: expected a JSON list of records")

        if not records:
            break

        yield records
        page += 1


@dlt.source(name="taxi_source")
def taxi_source(max_pages: Optional[int] = None) -> Iterator[Any]:
    """dlt source containing NYC taxi trip resources."""
    yield taxi_trips(max_pages=max_pages)


def run_pipeline(max_pages: Optional[int] = None) -> dlt.common.pipeline.LoadInfo:
    """Create and run the dlt pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
    )

    return pipeline.run(taxi_source(max_pages=max_pages))


if __name__ == "__main__":
    load_info = run_pipeline()
    print(load_info)  # noqa: T201

