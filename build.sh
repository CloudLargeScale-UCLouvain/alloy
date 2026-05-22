#!/usr/bin/env bash
set -euo pipefail

TAG="${TAG:-latest}"

echo "==> Building envoy Wasm binary (containerised)"
docker build envoy/ -f envoy/Dockerfile.wasm -t "alloy-wasm-builder:$TAG"
docker run --rm -v "$(pwd)/envoy/xp:/output" "alloy-wasm-builder:$TAG"

echo "==> Building envoy Docker image (alloy-envoy:$TAG)"
docker build envoy/ -t "alloy-envoy:$TAG"

echo "==> Building control-plane Docker image (alloy-control-plane:$TAG)"
docker build control-plane/ -t "alloy-control-plane:$TAG"

echo "==> Building benchmarks Docker image (alloy-benchmarks:$TAG)"
docker build xp/benchmarks/flink-examples/ -t "alloy-benchmarks:$TAG"

echo "==> Extracting benchmarks JAR to xp/benchmarks/flink-examples/target/"
mkdir -p xp/benchmarks/flink-examples/target
docker run --rm -v "$(pwd)/xp/benchmarks/flink-examples/target:/output" "alloy-benchmarks:$TAG"

echo "==> Saving images as tarballs to images/"
mkdir -p images
docker save "alloy-envoy:$TAG"         | gzip > "images/alloy-envoy.tar.gz"
docker save "alloy-control-plane:$TAG" | gzip > "images/alloy-control-plane.tar.gz"

echo "Done."
