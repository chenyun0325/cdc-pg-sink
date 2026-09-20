#!/usr/bin/env bash
set -euo pipefail

export FLINK_HOME="${FLINK_HOME:-/usr/lib/flink}"
export FLINK_CDC_HOME="${FLINK_CDC_HOME:-/home/hadoop/flink-cdc-3.5.0}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-/etc/hadoop/conf}"
export HADOOP_CLASSPATH="$(hadoop classpath)"

JOB_FILE="${1:-$FLINK_CDC_HOME/jobs/oceanbase-to-postgres.yaml}"
CHECKPOINT_BASE="${CHECKPOINT_BASE:-s3://your-bucket/cdc}"

exec "$FLINK_CDC_HOME/bin/flink-cdc.sh" \
  -t yarn-application \
  -Dclassloader.resolve-order=parent-first \
  -Djobmanager.memory.process.size=4096m \
  -Dtaskmanager.memory.process.size=8192m \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dstate.backend.type=rocksdb \
  -Dexecution.checkpointing.interval=60s \
  -Dexecution.checkpointing.mode=EXACTLY_ONCE \
  -Dexecution.checkpointing.timeout=10min \
  -Dexecution.checkpointing.max-concurrent-checkpoints=1 \
  -Dstate.checkpoint-storage=filesystem \
  -Dstate.checkpoints.dir="$CHECKPOINT_BASE/checkpoints/ob-to-postgres" \
  -Dstate.savepoints.dir="$CHECKPOINT_BASE/savepoints/ob-to-postgres" \
  -Dexecution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
  "$JOB_FILE"
