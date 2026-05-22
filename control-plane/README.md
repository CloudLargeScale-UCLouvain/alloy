# Alloy Control Plane

The Alloy control plane intercepts SQL query submissions destined for Apache Flink. It compiles the query into an operator graph, extracts selection and projection operations from it, generates a configuration for the Alloy proxies (deployed on Envoy alongside Kafka brokers), and submits a simplified query plan back to Flink — all transparently, without user involvement.

## Overview

The control plane operates in three modes, controlled by the `EXECUTION_MODE` environment variable:

| Mode | Description |
|---|---|
| `COMPILE` (default) | Compile the SQL query, extract the Alloy plan, and write proxy config (`alloy_plan.yaml`) and simplified Flink plan (`alloy_plan.json`). Does not submit to Flink. |
| `RUN` | `COMPILE` + submit the simplified plan to Flink for execution. |
| `RUN_ALLOY` | Submit a previously compiled Alloy plan (`alloy_plan.json`) to Flink directly. |

## Configuration

All configuration is passed via environment variables:

| Variable | Default | Description |
|---|---|---|
| `EXECUTION_MODE` | `COMPILE` | Execution mode (see above) |
| `JOBMANAGER_RPC_ADDRESS` | `control-plane-jobmanager-1` | Hostname of the Flink Job Manager |
| `QUERY_FILENAME` | `q11.sql` | SQL file (in `config/`) defining the query to submit |
| `TABLES_FILENAME` | `tables.sql` | SQL file (in `config/`) defining Kafka source and sink tables |
| `PARALLELISM` | `1` | Flink operator parallelism |
| `DISABLE_OPERATOR_CHAINING` | `false` | Set to `true` to disable Flink operator chaining |

## Files

```
control-plane/
├── control-plane.py       # Entry point
├── alloy.py               # Operator graph analysis and plan transformation
├── Dockerfile
├── requirements.txt
└── config/
    ├── tables.sql         # Kafka source/sink table definitions (Flink SQL)
    └── q11.sql            # Example query: Nexmark Q11 (session window)
```

## Running with Docker

Build the image:
```bash
docker build -t alloy-control-plane .
```

Run in `COMPILE` mode (extracts proxy config without submitting to Flink):
```bash
docker run --rm \
  -e EXECUTION_MODE=COMPILE \
  -e QUERY_FILENAME=q11.sql \
  -v $(pwd)/config:/opt/config \
  alloy-control-plane
```

Run in `RUN` mode (compile + submit to Flink):
```bash
docker run --rm \
  -e EXECUTION_MODE=RUN \
  -e JOBMANAGER_RPC_ADDRESS=<jobmanager-host> \
  -e QUERY_FILENAME=q3.sql \
  -e PARALLELISM=2 \
  -v $(pwd)/config:/opt/config \
  alloy-control-plane
```

## Using `alloy.py` standalone

`alloy.py` can also be used as a CLI to analyze and transform a pre-compiled Flink plan:

```bash
python3 alloy.py --input_path config/test.json \
                 --output_dir config/ \
                 --envoy envoy1:9093 \
                 --apply \
                 --projection \
                 --selection
```

| Flag | Description |
|---|---|
| `--input_path` | Path to a compiled Flink plan (JSON) |
| `--output_dir` | Directory to write `compiled_plan.json` |
| `--envoy` | Envoy proxy address to inject into the simplified plan |
| `--apply` | Apply the extracted operations and write the simplified plan |
| `--projection` | Enable projection pushdown |
| `--selection` | Enable selection pushdown |

Without `--apply`, the script prints the extracted Alloy plan without modifying anything.

## Outputs

After a `COMPILE` or `RUN` execution, two files are written to `config/`:

- **`alloy_plan.yaml`** — proxy configuration, one entry per Kafka topic, specifying the `selections`, `projections`, and `partition` fields to be enforced by the Envoy proxies.
- **`alloy_plan.json`** — simplified Flink execution plan, stripped of operators whose work is now handled by the proxies.
