#!/usr/bin/env bash
set -euo pipefail

FLINK_CDC_HOME="${FLINK_CDC_HOME:-/home/hadoop/flink-cdc-3.5.0}"
LIB="$FLINK_CDC_HOME/lib"
mkdir -p "$LIB"

MYSQL_PIPELINE_JAR="$LIB/flink-cdc-pipeline-connector-mysql-3.5.0.jar"
MYSQL_DRIVER_JAR="$LIB/mysql-connector-j-8.0.27.jar"

fetch() {
  local url="$1" target="$2"
  if [ -f "$target" ]; then
    echo "Exists: $target"
    return
  fi
  echo "Downloading $url"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 -o "$target" "$url"
  else
    wget -O "$target" "$url"
  fi
}

# Required by the OceanBase/MySQL source used in the sample job.
fetch \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/3.5.0/flink-cdc-pipeline-connector-mysql-3.5.0.jar" \
  "$MYSQL_PIPELINE_JAR"
fetch \
  "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar" \
  "$MYSQL_DRIVER_JAR"

echo "Runtime source dependencies:"
ls -lh "$MYSQL_PIPELINE_JAR" "$MYSQL_DRIVER_JAR"
echo "The custom PostgreSQL sink JAR already contains the PostgreSQL JDBC driver."
