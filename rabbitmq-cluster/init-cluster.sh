#!/bin/bash
set -e

echo "Esperando que ambos nodos estén disponibles..."
until docker exec rabbit1 rabbitmq-diagnostics -q ping >/dev/null 2>&1; do sleep 3; done
until docker exec rabbit2 rabbitmq-diagnostics -q ping >/dev/null 2>&1; do sleep 3; done

echo "Uniendo rabbit2 al clúster de rabbit1..."
docker exec rabbit2 rabbitmqctl stop_app
docker exec rabbit2 rabbitmqctl reset
docker exec rabbit2 rabbitmqctl join_cluster rabbit1@rabbit1
docker exec rabbit2 rabbitmqctl start_app

echo "Estado final del clúster:"
docker exec rabbit1 rabbitmqctl cluster_status
