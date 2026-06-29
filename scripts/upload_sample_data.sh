#!/usr/bin/env bash

set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:-materials-research}"

SOURCE_FILE="data/sample/experiment-events.jsonl"

VOLUME_ROOT="dbfs:/Volumes/workspace/materials_research_dev/raw_experiment_files"
INCOMING_DIRECTORY="${VOLUME_ROOT}/incoming"
TARGET_FILE="${INCOMING_DIRECTORY}/experiment-events.jsonl"

if [[ ! -f "${SOURCE_FILE}" ]]; then
  echo "Source file not found: ${SOURCE_FILE}" >&2
  exit 1
fi

databricks fs mkdir \
  "${INCOMING_DIRECTORY}" \
  --profile "${PROFILE}"

databricks fs cp \
  "${SOURCE_FILE}" \
  "${TARGET_FILE}" \
  --overwrite \
  --profile "${PROFILE}"

echo "Uploaded ${SOURCE_FILE}"
echo "Target: ${TARGET_FILE}"
