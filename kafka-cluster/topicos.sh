#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
broker=kafka-1:9092
cli() { docker compose exec -T kafka-1 "$@"; }
# Esperar el registro de los tres brokers antes de solicitar tres réplicas.
ready=false
for attempt in {1..30}; do
  brokers=$(cli kafka-broker-api-versions --bootstrap-server "$broker" 2>/dev/null || true)
  if [[ $(grep -cE '[(]id: [123] ' <<< "$brokers" || true) == 3 ]]; then
    ready=true
    break
  fi
  sleep 4
done
if [[ $ready != true ]]; then
  echo "Faltan brokers. Revisar docker compose ps y docker compose logs --tail=30." >&2
  exit 1
fi
for topic in envios-eventos envios-auditoria; do
  retention=604800000
  if [[ $topic == envios-auditoria ]]; then retention=2592000000; fi
  cli kafka-topics --bootstrap-server "$broker" --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 3 \
    --config cleanup.policy=delete --config retention.ms="$retention"
  description=$(cli kafka-topics --bootstrap-server "$broker" --describe --topic "$topic")
  echo "$description"
  # Una ejecución repetida debe detectar un tópico preexistente mal configurado.
  awk '/PartitionCount:/ {ok=($0 ~ /PartitionCount: 3([[:space:]]|$)/ && $0 ~ /ReplicationFactor: 3([[:space:]]|$)/)}
       /Partition: / {n++; for(i=1;i<=NF;i++) if($i=="Isr:" && $(i+1) ~ /^[0-9]+,[0-9]+,[0-9]+$/) synced++}
       END {exit !(ok && n==3 && synced==3)}' <<< "$description" || {
    echo "Revisar particiones, réplicas e ISR; si el clúster está iniciando, repetir el script." >&2
    exit 1
  }
  config=$(cli kafka-configs --bootstrap-server "$broker" --describe --entity-type topics --entity-name "$topic")
  echo "$config"
  grep -Eq "retention.ms=$retention([[:space:],]|$)" <<< "$config"
  grep -q 'cleanup.policy=delete' <<< "$config"
done
