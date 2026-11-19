#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
for group in ruta-reporteria ruta-auditoria; do
  for view in --state --members --offsets; do
    args=("$view")
    if [[ $view == --members ]]; then args+=(--verbose); fi
    docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
      --describe --group "$group" "${args[@]}"
  done
done
