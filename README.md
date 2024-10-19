# dbt Cloud Webhooks and Datadog Integration

This repository contains code to send logs to Datadog upon the completion of jobs within your dbt Cloud account.

## Overview

This integration allows you to monitor your dbt Cloud jobs by sending logs to Datadog for the following dbt resource types:

- Model
- Seed
- Snapshot
- Test

## Prerequisites

- A [Datadog](https://www.datadoghq.com/) account
- A [dbt Cloud](https://www.getdbt.com/product/dbt-cloud/) account
- A [Modal](https://modal.com) account

## Setup

1. Create an account at [modal.com](https://modal.com) if you haven't already. Modal is used as a web endpoint that receives the webhook from dbt Cloud.

2. Clone this repository:

   ```
   git clone https://github.com/dpguthrie/dbt-cloud-datadog-integration.git
   cd dbt-cloud-datadog-integration
   ```

3. Copy the contents of the `.env.example` file to a new `.env` file:

   ```
   cp .env.example .env
   ```

4. Fill in the required environment variables in the `.env` file:

   ```
   DD_SITE=your_datadog_site
   DD_API_KEY=your_datadog_api_key
   DBT_CLOUD_SERVICE_TOKEN=dbt_cloud_service_token
   DBT_CLOUD_METADATA_URL=your_metadata_url
   DBT_CLOUD_WEBHOOK_SECRET=your_webhook_secret
   ```

   Check out [this table](https://docs.getdbt.com/docs/dbt-cloud-apis/discovery-querying#discovery-api-endpoints) to find your dbt Cloud metadata URL. Additionally, the webhook secret is used to validate the requests to the endpoint are actually coming from dbt Cloud (more info [here](https://docs.getdbt.com/docs/deploy/webhooks#validate-a-webhook)).

5. Set up the webhook in dbt Cloud. You can use any placeholder endpoint for the time being - once we deploy our modal app, we can come back here and edit the webhook with the appropriate endpoint. When you save, ensure that you grab the secret and place it in your `.env` file. Additional documentation can be found [here](https://docs.getdbt.com/docs/deploy/webhooks).

6. Deploy your Modal endpoint. You can deploy locally using the command below:

   ```
   modal deply src/app.py
   ```

   Once deployed, update your webhook's endpoint created in step 5 with the url of your application.

7. **[Optional]** Deploy your application via Github Action. Just move the action at `example_cicd/deploy.yml` to `.github/workflows/deploy.yml` and it will deploy as code is merged to your main branch.

## Usage

Once set up, the integration will automatically send logs to Datadog whenever a job completes in dbt Cloud. You can then use Datadog's features to monitor, alert, and analyze your dbt Cloud job performance.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[MIT License](LICENSE)

## Support

If you encounter any problems or have any questions, please open an issue in this repository.
