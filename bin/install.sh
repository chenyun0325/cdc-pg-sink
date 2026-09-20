#!/usr/bin/env bash
set -euo pipefail

FLINK_CDC_HOME="${FLINK_CDC_HOME:-/home/hadoop/flink-cdc-3.5.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/target/flink-cdc-pipeline-connector-postgres-sink-3.5.0.jar"

[ -f "$JAR" ] || { echo "Missing $JAR; run bin/build.sh first"; exit 1; }
mkdir -p "$FLINK_CDC_HOME/lib"
cp -f "$JAR" "$FLINK_CDC_HOME/lib/"
echo "Installed custom PostgreSQL sink into $FLINK_CDC_HOME/lib"
ls -lh "$FLINK_CDC_HOME/lib"/*postgres* 2>/dev/null || true
