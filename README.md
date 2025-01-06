# Augur

## Description
Augur is an observability tool that allows users to further understand the following three classes of metrics:
1. **[Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)**: These metrics are used to understand the resource utilization of a system.
   They include CPU usage, memory usage, disk usage, and network usage. Prometheus is an example of a tool that can be
   used to collect these metrics, and often times they are simply referred to as Prometheus metrics.
2. **[Logs](https://opentelemetry.io/docs/concepts/signals/logs/)**: Logs are used to understand the behavior of a system. They are often used to debug issues and to understand
   the flow of a system. Loki is an example of a tool that can be used to collect logs.
3. **[Traces](https://opentelemetry.io/docs/concepts/signals/traces/)**: Traces are used to understand the flow of a system, and oftentimes overlap with logs w.r.t. their use case.
   Jaeger is an example of a tool that can be used to visualize and analyze traces.

## Installation

### Back-end

#### Prerequisites
Make sure the following are installed on your machine:
- **Go**: Download from [Go website](https://go.dev/doc/install) (This repo uses `Go v1.22.2`)
- **Docker**: Download from [Docker website](https://docs.docker.com/get-docker/) (This repo uses `Docker 25.0.3`)
- **Docker Compose**: Download from [Docker Compose website](https://docs.docker.com/compose/install/) (This repo uses `Docker Compose v2.24.6-desktop.1`)

#### Steps
1. Install the dependencies. Note that all commands are run from the root of the repository.
    ```bash
    cd ./backend
    go mod tidy
    ```
2. Build either the OTel (OpenTelemetry) Scraper or Query Service (Optional).

The OTel Scraper is not a scraper at all, but rather provides a gRPC server for an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
that will receive logs, spans, and metrics from the collector. It will also process that data, clustering and
analyzing them for observability purposes. To build the OTel Scraper (or Scraper) app, you can run the following
command.
```bash
cd ./backend
go build -o ${DESIRED_OUTPUT_NAME} ./cmd/otel_scraper/main.go
```
where `DESIRED_OUTPUT_NAME` is the name of the output file. Meanwhile, the Query Service app is meant to provide a
webserver backend to allow a consumer to understand the analyzed data from the Scraper. To build the Query Service app,
you can run the following command
```bash
cd ./backend
go build -o ${DESIRED_OUTPUT_NAME} ./cmd/query_server/main.go
```
Once you have built the desired app, you can run it by executing the output file. Both the scraper and query server
apps require an ElasticSearch instance to be running on the default port (`http://localhost:9200`).

3. Run the DB, query_server, scraper, and a fake server using Docker Compose
```bash
cd ./fake_svc
docker-compose up
```
It may take some time for ElasticSearch to stabilize. Once that has happened, you can peek into the database using the 
various commands in the `./fake_svc/elasticsearch_queries` folder. Note that nothing will enter the database until 
you interact with the fake server in the following step. The Scraper and the Query Server will both attempt to boostrap 
the database with some indices. These indices are as follows:

- `log_index`: This index will contain logs from the fake server. See the definition in
  `./backend/internal/db/elasticsearch/bootstrapper/logs.go` for the schema.
- `span_index`: This index will contain spans from the fake server. See the definition in
  `./backend/internal/db/elasticsearch/bootstrapper/spans.go` for the schema.
- `count_index`: This index will contain more advanced metrics, considering the relationships between clusters
  of logs, spans, and metrics. See the definition in `./backend/internal/db/elasticsearch/bootstrapper/count.go` for the schema.
- `cluster_index`: This index will define the causal relationship (what causes what) between the clusters of logs, spans, and metrics.
  See the definition in `./backend/internal/db/elasticsearch/bootstrapper/cluster.go` for the schema.

4. Interacting with the API 

For now, the fake server is merely hosting a fake login endpoint on a default of `http://localhost:8080`. Any username 
and every password except for `fake_password` will fail, emitting the associated logs and spans. Feel free to attempt to send a sample payload below.
```bash
curl -X POST http://localhost:8080/accounts/login -d '{"username": "fake_username", "password": "fake_password"}'
```
Sending enough of these messages will trigger the database buffer to flush, allowing you to see the logs and spans in the
database. You can then query the database using the various commands in the `./fake_svc/elasticsearch_queries` folder.

5. Creating the Swagger JSON

To generate the swagger.json and swagger.yaml files, you can run the following command
```bash
cd ./backend
swag init -g ./cmd/query_server/main.go
```
These will be consumed by the front-end to generate the API functions.

### Front-end

#### Prequisites
Make sure the following are installed on your machine:
- **Node.js**: Download from [Node.js website](https://nodejs.org/en/download/) (This repo uses `Node v20.13.1`)
- **npm**: Comes with Node.js (This repo uses `npm v10.5.2`)
- **OpenAPI Generator CLI**: Install globally with npm
  ```bash
  npm install -g @openapitools/openapi-generator-cli
  ```

#### Steps

1. Install the dependencies
    ```bash
    cd ./frontend
    npm install
    ```
2. Generate the API functions

    ```bash
    cd ./backend
    openapi-generator-cli generate -i docs/swagger.json -g typescript-fetch -o ../frontend/src/backend_api
    ```

   This will generate the API functions in the `./frontend/src/backend-api` directory.


3. Start the React development server
    ```bash
    cd ./frontend
    npm run dev
    ```
   This will start the React development server on port `5173`. You can navigate to http://localhost:5173 to view the
   front-end. The front-end will be able to communicate with the Query Server running on port `8081`. Note that this is
   hardcoded in the `vite.config.ts` file in the `frontend` directory.

Note that the front-end only has one working component: the navigation page. You can immediately click on `Search` to
receive the list of logs and spans you triggered in Back-end Step 4. You can expand the logs and spans to see the
JSONized version of all the details of that specific log or span. If you scroll in the table to the right, you can show the Chain
of Events in a DAG below the table by clicking on the `Show CoE` button. This will show the causal relationships between the logs
and spans in the database. Note that the CoE is not yet fully functional, and is a work in progress. To receive the best results,
heavily stagger (2s between each curl command) the login attempts in Back-end Step 4.