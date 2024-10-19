# stdlib
import enum
import hashlib
import hmac
import json
import os

# third party
import modal
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from fastapi import HTTPException, Request

# first party
from src.client import DiscoveryApiClient
from src.logger import logger


class WebhookEventType(enum.Enum):
    """
    The type of event that triggered the webhook.
    """

    ERROR = "job.run.errored"
    COMPLETED = "job.run.completed"
    STARTED = "job.run.started"


NODE_TYPES = ["Model", "Seed", "Snapshot", "Test"]
DATADOG_MAX_LIST_SIZE = 1000


def get_run_metadata(edges: list[dict], run_id: int) -> list[dict]:
    def get_execution_info(node) -> tuple[bool, str]:
        resource_type = node.get("resourceType", "").lower()
        execution_info_key = f"{resource_type}ExecutionInfo" if resource_type else None
        in_run = (
            execution_info_key
            and node.get(execution_info_key)
            and node.get(execution_info_key).get("lastRunId") == run_id
        )
        return in_run, execution_info_key

    all_nodes = []
    for edge in edges:
        in_run, execution_info_key = get_execution_info(edge["node"])
        if in_run:
            node = {
                **{k: v for k, v in edge["node"].items() if k != execution_info_key},
                "executionInfo": edge["node"][execution_info_key],
            }
            all_nodes.append(node)

    return all_nodes


def chunker(seq):
    """Ensure that the log array is <= to the DATADOG_MAX_LIST_SIZE)"""
    size = DATADOG_MAX_LIST_SIZE
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


QUERY = """
query Applied($environmentId: BigInt!, $filter: AppliedResourcesFilter!, $first: Int, $after: String) {
  environment(id: $environmentId) {
    applied {
      resources(filter: $filter, first: $first, after: $after) {
        edges {
          node {
            ... on ModelAppliedStateNode {
              modelExecutionInfo: executionInfo {
                compileCompletedAt
                compileStartedAt
                executeCompletedAt
                executeStartedAt
                executionTime
                lastJobDefinitionId
                lastRunError
                lastRunGeneratedAt
                lastRunId
                lastRunStatus
                lastSuccessJobDefinitionId
                lastSuccessRunId
                runElapsedTime
                runGeneratedAt
              }
              access
              alias
              database
              environmentId
              fqn
              group
              language
              materializedType
              modelingLayer
              name
              packageName
              projectId
              uniqueId
              schema
              resourceType
            }
            ... on TestAppliedStateNode {
              testExecutionInfo: executionInfo {
                compileCompletedAt
                compileStartedAt
                executeCompletedAt
                executeStartedAt
                executionTime
                lastJobDefinitionId
                lastRunError
                lastRunFailures
                lastRunGeneratedAt
                lastRunId
                lastRunStatus
                lastSuccessJobDefinitionId
                lastSuccessRunId
                runElapsedTime
                runGeneratedAt
              }
              environmentId
              fqn
              name
              projectId
              uniqueId
              resourceType
            }
            ... on SeedAppliedStateNode {
              seedExecutionInfo: executionInfo {
                compileCompletedAt
                compileStartedAt
                executeCompletedAt
                executeStartedAt
                executionTime
                lastJobDefinitionId
                lastRunError
                lastRunGeneratedAt
                lastRunId
                lastRunStatus
                lastSuccessJobDefinitionId
                lastSuccessRunId
                runElapsedTime
                runGeneratedAt
              }
              alias
              database
              environmentId
              fqn
              name
              packageName
              projectId
              uniqueId
              schema
              resourceType
            }
            ... on SnapshotAppliedStateNode {
              snapshotExecutionInfo: executionInfo {
                compileCompletedAt
                compileStartedAt
                executeCompletedAt
                executeStartedAt
                executionTime
                lastJobDefinitionId
                lastRunError
                lastRunGeneratedAt
                lastRunId
                lastRunStatus
                lastSuccessJobDefinitionId
                lastSuccessRunId
                runElapsedTime
                runGeneratedAt
              }
              alias
              database
              environmentId
              fqn
              name
              packageName
              projectId
              uniqueId
              schema
              resourceType
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
          hasPreviousPage
          startCursor
        }
        totalCount
      }
    }
  }
}
"""


image = modal.Image.debian_slim().pip_install_from_requirements("requirements.txt")

app = modal.App("dbt-cloud-webhook-datadog", image=image)


async def verify_signature(request: Request):
    payload = await request.body()
    signature = request.headers.get("authorization")
    secret = os.getenv("DBT_CLOUD_WEBHOOK_SECRET", None)

    if not signature or not secret:
        raise HTTPException(status_code=400, detail="Missing signature or secret")

    computed_signature = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_signature, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")


@app.function(secrets=[modal.Secret.from_dotenv()])
@modal.web_endpoint(method="POST")
async def webhook_handler(request: Request):
    await verify_signature(request)

    # Get the webhook payload
    payload = await request.json()
    logger.info(" - webhook_handler - payload: %s", payload)

    # Get the webhook data
    data = payload["data"]
    event_type = payload["eventType"]

    if event_type != WebhookEventType.STARTED:
        logs = []

        tags = {
            "project_name": data["projectName"],
            "environment_name": data["environmentName"],
            "job_name": data["jobName"],
            "run_id": data["runId"],
            "webhook_name": payload["webhookName"],
            "run_reason": data["runReason"],
        }

        client = DiscoveryApiClient(environment_id=data["environmentId"])
        try:
            edges = client.run_query(
                query=QUERY,
                variables={"filter": {"types": NODE_TYPES}},
            )
        except Exception as e:
            logger.error(f"Error getting run metadata: {e}")
            raise e

        nodes = get_run_metadata(edges, int(data["runId"]))

        for node in nodes:
            logs.append(
                HTTPLogItem(
                    ddsource="dbt_cloud",
                    ddtags=",".join("{}:{}".format(*i) for i in tags.items()),
                    hostname=client.url,
                    message=json.dumps(node),
                    service="dbt_cloud_webhooks",
                )
            )

        responses = []
        for log_items in chunker(logs):
            with ApiClient(Configuration()) as api_client:
                logs_api = LogsApi(api_client)
                body = HTTPLog(log_items)
                response = logs_api.submit_log(body=body)
                responses.append(response)
                logger.info(f"Datadog log submission response: {response}")

        # if any(not response.ok for response in responses):
        #     raise HTTPException(
        #         status_code=500, detail="Failed to submit logs to Datadog"
        #     )

    return {"status": "success"}
