# Alloy

Alloy is a proxy-based approach for improving the efficiency of the coupling between stream storage and stream processing. It targets deployments combining **Apache Kafka** (stream storage) and **Apache Flink** (stream processing).

In a standard setup, Kafka serves data at the granularity of entire topics. Flink source operators receive, deserialize, and reify all events — only to immediately drop those irrelevant to the query. Furthermore, Kafka topics typically carry many fields per event, but a given query only reads a small subset of them; the remaining fields are deserialized and transferred needlessly. This wastes network bandwidth between the two clusters and imposes unnecessary CPU load on Flink. Alloy addresses both inefficiencies by filtering data directly on the storage side: the proxy drops entire events that don't match the query predicate (*selection*) and strips fields not referenced by the query (*projection*), sending only what is actually needed.

## How it works

Alloy has two main components:

**Data plane — Envoy proxies** (`envoy/`): Alloy proxies are deployed alongside Kafka brokers, transparently intercepting Flink's fetch requests. They apply *selection* (filtering events by field value) and *projection* (dropping unused fields) directly on Kafka responses before forwarding them to Flink. The proxies are implemented in Rust, compiled to WebAssembly, and deployed as plugins over the [Envoy proxy](https://www.envoyproxy.io) via the `proxy-wasm` ABI — requiring no modifications to Kafka or Flink, and no pre-deployment of Alloy-specific code on the proxy nodes.

**Control plane** (`control-plane/`): A Python service that interposes between the user and the Flink cluster at query submission time. It compiles the SQL query using Flink's TableAPI and Apache Calcite to obtain an operator graph, extracts selection and projection operations from it, configures the proxies accordingly, strips the now-redundant operators from the graph, and submits the simplified plan to Flink — all transparently.

```
User
  │  SQL query
  ▼
Control Plane ──► Flink (simplified plan)
  │
  └──► Envoy Proxies (per Kafka broker)
             │  filtered events only
             ▼
           Flink source operators
```

## Repository structure

```
alloy/
├── build.sh          # Builds all Docker images and extracts artifacts
├── control-plane/    # Alloy control plane (Python, PyFlink)
├── envoy/            # Alloy data plane proxy (Rust, WebAssembly)
├── images/           # Docker image tarballs produced by build.sh
├── infra/            # Cluster deployment scripts (Grid5000)
└── xp/               # Nexmark benchmark experiments
    ├── config/       # Docker Compose templates and SQL queries
    ├── benchmarks/   # Flink benchmark JAR sources
    ├── jars/         # Flink Kafka connector JAR
    ├── multiple_experiments.ipynb  # Experiment campaign entry point
    ├── single_experiment.ipynb     # Per-run template (executed by papermill)
    └── plot.ipynb    # Plot generation
```

## Building

From the repository root:

```bash
bash build.sh
```

This builds the Wasm binary, the `alloy-envoy`, `alloy-control-plane`, and `alloy-benchmarks` Docker images, and saves the first two as gzipped tarballs to `images/` for upload to the cluster.

## Components

### Control plane
See [control-plane/README.md](control-plane/README.md) for usage, configuration, and Docker instructions.

### Envoy proxy (data plane)
See [envoy/README.md](envoy/README.md) for build instructions and proxy configuration.

### Infrastructure
The `infra/grid5000/` directory contains scripts and notebooks for deploying the full Alloy stack on [Grid5000](https://www.grid5000.fr), the testbed used for evaluation. 

### Experiments
See [xp/README.md](xp/README.md) for full details. The entry point is `xp/multiple_experiments.ipynb`, which provisions the cluster, runs the full Nexmark benchmark matrix (queries × parallelisms × throughputs), and generates plots.

## Reference

> Alloy: Transparent Proxy-Based Coupling Between Stream Processing and Storage. Guillaume Rosinosky, Donatien Schmitz, Etienne Rivière. In The 20th ACM International Conference on Distributed and Event-based Systems (DEBS ’26), June 23–26, 2026, Lisbon, Portugal. [https://doi.org/10.1145/3809481.3812609](https://doi.org/10.1145/3809481.3812609)
