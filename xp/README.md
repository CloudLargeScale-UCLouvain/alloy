# xp — Alloy Experiments

This directory contains the notebooks and configuration files for running and reproducing the Alloy evaluation. Experiments are run on a Grid5000 cluster provisioned via `../infra/grid5000/`.

## Prerequisites

**Build the Docker images** from the repository root before running experiments:

```bash
bash build.sh          # builds with tag "latest"
TAG=mytag bash build.sh  # optional custom tag
```

This builds the Wasm binary, the `alloy-envoy` and `alloy-control-plane` images, the `alloy-benchmarks` image, extracts the benchmarks JAR to `xp/benchmarks/flink-examples/target/`, and saves `alloy-envoy` and `alloy-control-plane` as gzipped tarballs to `images/` — which are then uploaded to the cluster by `load_local_images`.

Install Python dependencies (inside your virtualenv):

```bash
pip install -r requirements.txt
```

Infrastructure provisioning is handled directly in the second cell of `multiple_experiments.ipynb`, which calls `init_docker_swarm`, `init_docker_nodes`, `init_monitoring`, and `load_local_images` from `../infra/grid5000/`. There is no separate provisioning step.

Two parameters control the reservation:

- `cluster` — the Grid5000 cluster to use (e.g. `"gros"`)
- `max_parallelism` — the maximum parallelism level to benchmark; the number of nodes requested is `max_parallelism * 2 + 2` (1 manager/injector, 1 job manager, `max_parallelism` task managers, `max_parallelism` Kafka brokers)

## Notebooks

**`multiple_experiments.ipynb` is the entry point.** Run this notebook to launch a full experiment campaign. `single_experiment.ipynb` is not meant to be run directly — it is the per-run template that `multiple_experiments.ipynb` instantiates and executes via [papermill](https://papermill.readthedocs.io), with parameters injected automatically for each configuration.

### `multiple_experiments.ipynb`

Orchestrates a full experiment campaign by running `single_experiment.ipynb` once per configuration using papermill. It:

1. Generates an experiment matrix over queries × parallelisms × throughputs
2. Writes a `experiment.csv` index to `experiments/<date>/<uuid>/`
3. Iterates over the matrix, skipping already-executed runs (resumable)
4. Each run is saved as an executed notebook under `experiments/<date>/<uuid>/<query>/<run_id>/`
5. Once all runs complete, copies `plot.ipynb` into the experiment folder and executes it via papermill, producing `plot_executed.ipynb` with all plots

Default experiment matrix:

| Dimension | Values |
|---|---|
| Queries | Q1, Q2, Q3, Q5, Q8, Q11 |
| Parallelisms | 1, 2, 4 |
| Throughputs | 5 000, 50 000 events/s |

## Directory structure

```
xp/
├── multiple_experiments.ipynb    # Batch experiment orchestrator (entry point)
├── single_experiment.ipynb       # Per-run template, executed by papermill
├── plot.ipynb                    # Plot generation, executed at end of campaign
├── requirements.txt
├── config/
│   ├── q1.sql … q11.sql          # Nexmark query definitions
│   ├── tables.sql                # Nexmark source table DDL
│   ├── docker-compose.yml        # Base stack (Flink + Kafka + Envoy)
│   ├── docker-compose-alloy-template.yml    # Alloy variant template
│   ├── docker-compose-vanilla-template.yml  # Vanilla variant template
│   ├── docker-compose-generator.yml         # Berserker event generator
│   └── envoy-alloy-test.yml.j2   # Envoy proxy config template (Jinja2)
├── jars/
│   └── flink-sql-connector-kafka-1.16.2.jar
├── benchmarks/
│   └── flink-examples/           # Flink benchmark JAR sources
└── experiments/                  # Output directory (gitignored)
    └── <date>/
        └── <notebook-uuid>/
            ├── experiment.csv    # Experiment index for this campaign
            └── <query>/
                └── <run-uuid>/
                    ├── <query>-<throughput>-<run>.ipynb  # Executed notebook
                    └── config/   # Generated compose and envoy configs
```

## Queries

All queries are from the [Nexmark benchmark](https://github.com/nexmark/nexmark) over a simulated auction stream:

| Query | Description | Alloy operators |
|---|---|---|
| Q1 | Currency conversion | Projection |
| Q2 | Auction item filter | Selection |
| Q3 | Auction/person join | Selection + projection |
| Q5 | Hot items (sliding window) | Projection |
| Q8 | New users joining auctions | Projection |
| Q11 | User sessions | Projection |

## Output

Each experiment run produces an executed notebook with embedded outputs and a DataFrame saved as CSV containing per-second metrics:

- `cpu_*` — container CPU usage per service (task managers, Kafka, Envoy)
- `cpu_rate_*` — instantaneous CPU rate
- `tput` — Flink consumer records/s
- `network_*` / `network_rate_*` — bytes received by task manager containers
- `job_name` — `vanilla` or `alloy`
