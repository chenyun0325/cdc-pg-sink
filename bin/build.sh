#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mvn -U clean package
echo "Built: target/flink-cdc-pipeline-connector-postgres-sink-3.5.0.jar"
