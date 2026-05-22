# Alloy Proxy

## 📋 Research Paper

**Title**: Alloy: Transparent Proxy-Based Coupling Between Stream Processing and Storage

**Abstract**: Stream storage and processing workflows typically send full data streams between systems, even when only subsets are needed. Alloy introduces transparent proxy-based filtering that reduces network transfer and computational overhead by sending only query-relevant data from storage (Kafka) to processing (Flink) systems.

## 🔧 Architecture

```
┌─────────────┐    ┌────────────────┐    ┌─────────────┐
│   Flink     │    │   Envoy +      │    │   Kafka     │
│  Operators  │───▶│  Alloy WASM    │───▶│  Brokers    │
└─────────────┘    │  Proxy Filter  │    └─────────────┘
       ↑           └────────────────┘          ↑
       │                ▲   ▲                  │
       │                │   │                  │
       │          ┌─────┴───┴────────┐         │
       └──────────│  Alloy Control   │◄────────┘
                  │    Plane         │
                  │  (Query Analysis)│
                  └──────────────────┘
```

**Data Flow**:
1. Flink submits queries to Alloy Control Plane
2. Control Plane analyzes queries and configures Alloy filters
3. Alloy WASM filters intercept Kafka traffic in Envoy
4. Filters apply selection/projection based on query requirements
5. Only query-relevant data reaches Flink operators

## 📦 Components

### 1. Alloy Proxy (`src/alloy/`)
Main WASM filter implementation that:
- Intercepts Kafka protocol traffic
- Applies selection/projection filters
- Maintains transparent operation

### 2. Kafka Protocol Handler (`src/kafka_alloy/`)
- Full Kafka protocol parsing and manipulation
- Virtual partition support
- Flink-compatible keygroup distribution algorithms

### 3. Routing Engine (`src/routing/`)
- Query-aware data routing
- Configuration management
- Multi-cluster coordination

## 🚀 Key Features

✅ **Transparent Operation** - No changes required to Kafka or Flink

✅ **Protocol-Level Filtering** - Works at Kafka protocol level

✅ **Query-Aware Optimization** - Only sends data relevant to queries

✅ **WASM-Based Deployment** - Lightweight Envoy proxy filters

✅ **Performance Focused** - Reduces network and CPU overhead

## 🔬 Technical Implementation

- **Language**: Rust 2021 Edition
- **Target**: WebAssembly (wasm32-wasip1)
- **Proxy Framework**: [Envoy](https://www.envoyproxy.io/) + [Proxy-WASM](https://github.com/proxy-wasm/spec)
- **Dependencies**:
  - [kafka-protocol-rs](lib/kafka-protocol-rs/) - Kafka protocol implementation
  - [proxy-wasm-rust-sdk](lib/proxy-wasm-rust-sdk/) - WASM proxy SDK

## 📊 Evaluation Results

Based on the research paper evaluation:

- **Network Reduction**: Drastically reduces data transfer between clusters
- **CPU Efficiency**: Maintains or reduces overall CPU usage
- **Latency Impact**: Minimal impact on processing latency
- **Benchmark Validation**: Tested with Nexmark benchmark suite

## Build

The recommended way to build is via `build.sh` from the repository root, which compiles the Wasm binary inside a container and then builds the `alloy-envoy` Docker image:

```bash
bash ../build.sh
```

To build manually:

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup target add wasm32-wasip1

# Build the Wasm filter
make build-wasi-alloy          # with logging
make build-wasi-alloy-nolog    # with reduced logging
```

Output: `xp/alloy.wasm`

## 📂 Project Structure

```
.
├── src/
│   ├── alloy/              # Main WASM filter
│   ├── kafka_alloy/        # Kafka protocol handler
│   └── routing/            # Routing engine
├── lib/
│   ├── kafka-protocol-rs/ # Kafka protocol (submodule)
│   └── proxy-wasm-rust-sdk/ # WASM SDK (submodule)
├── Cargo.toml              # Workspace configuration
├── Makefile                # Build scripts
└── README.md               # This file
```

## 🔧 Configuration

Alloy is configured through:

1. **Flink Job Submission**: Control plane analyzes queries
2. **Envoy Configuration**: WASM filter deployment
3. **YAML Configuration**: Routing rules and parameters

Example routing configuration:

```yaml
workload_type: kafka-alloy
default_cluster: grpc-bridge-edge1
additional_configuration:
  debug_protocol: 0
  remove_empty_records: true
alloy_filters:
  bid: # topic
    num_partitions: 4
    num_sources: 4
    projections:
    - auction
    - bidder
    - dateTime
    selections:
    - attribute: category
      value: 10
```

## 📚 Related Work

- [Apache Kafka](https://kafka.apache.org/) - Distributed streaming platform
- [Apache Flink](https://flink.apache.org/) - Stateful stream processing
- [Envoy Proxy](https://www.envoyproxy.io/) - Service mesh proxy
- [Proxy-WASM](https://github.com/proxy-wasm/spec) - WASM extension mechanism

## 🎯 Research Impact

Alloy demonstrates how service mesh proxies can transparently optimize the storage-processing coupling in stream processing systems, achieving significant efficiency gains without modifying existing systems.
