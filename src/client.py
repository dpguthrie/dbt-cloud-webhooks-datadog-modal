# stdlib
import os
from typing import Optional

# third party
import requests

# first party
from src.logger import logger


class DiscoveryApiClient:
    DEFAULT_URL = "https://metadata.cloud.getdbt.com/graphql"

    def __init__(self, environment_id: int):
        self.environment_id = environment_id
        self.token = os.getenv("DBT_CLOUD_SERVICE_TOKEN", None)
        self.url = os.getenv("DBT_CLOUD_METADATA_URL", self.DEFAULT_URL)
        if self.token is None:
            raise ValueError("DBT_CLOUD_SERVICE_TOKEN is not set")

    def _run_query_with_cursor(
        self,
        query: str,
        variables: Optional[dict],
        after_cursor: Optional[str],
        limit: int,
    ):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "content-type": "application/json",
        }

        if variables is None:
            variables = {}

        variables.update(
            {
                "environmentId": self.environment_id,
                "limit": limit,
                "after": after_cursor,
                "first": limit,
            }
        )

        json_data = {"query": query, "variables": variables}

        logger.info(f" - Querying API with after_cursor: {after_cursor}")

        response = requests.post(self.url, json=json_data, headers=headers)

        response.raise_for_status()
        return response.json()

    def _get_next_page_cursor(self, api_response: dict) -> Optional[str]:
        resources = api_response["data"]["environment"]["applied"]["resources"]
        page = resources["pageInfo"]

        if page.get("hasNextPage", False):
            return page["endCursor"]

        return None

    def _extract_query_results(self, api_response: dict) -> list[dict]:
        resources = api_response["data"]["environment"]["applied"]["resources"]
        return resources["edges"]

    def run_query(
        self, query: str, variables: Optional[dict], limit: int = 500
    ) -> list[dict]:
        cursor = None

        all_results = []
        page = 0
        while True:
            api_response = self._run_query_with_cursor(query, variables, cursor, limit)
            cursor = self._get_next_page_cursor(api_response)
            results = self._extract_query_results(api_response)
            all_results.extend(results)
            page += 1

            if not cursor:
                break

        return all_results
