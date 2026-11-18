#!/usr/bin/env bash
set -euo pipefail
: "${RABBIT_USER:?Define RABBIT_USER}" "${RABBIT_PASSWORD:?Define RABBIT_PASSWORD}"
api="${RABBIT_API:-http://localhost:15672}/api"
call() { curl -fsS -u "$RABBIT_USER:$RABBIT_PASSWORD" -H 'Content-Type: application/json' "$@"; }
run="EV2-$(date +%s)-$$"
for pair in email:email.send warehouse:warehouse.ticket label:label.gen; do
  domain=${pair%:*}
  key=${pair#*:}
  queue="q.cmd.$domain"
  for error in false true; do
    code="$run-$domain-$error"
    payload=$(jq -nc --arg id "$code" --argjson err "$error" '{codigoSeguimiento:$id,clienteEmail:"prueba@example.com",servicioId:1,simularError:$err}')
    body=$(jq -nc --arg key "$key" --arg msg "$payload" '{properties:{content_type:"application/json",delivery_mode:2},routing_key:$key,payload:$msg,payload_encoding:"string"}')
    call -d "$body" "$api/exchanges/%2F/cmd.direct/publish" | jq -e '.routed == true'
  done
  found=false
  for attempt in {1..12}; do
    # Reencola las muestras para conservar la evidencia del laboratorio.
    if call -d '{"count":100,"ackmode":"ack_requeue_true","encoding":"auto","truncate":10000}' "$api/queues/%2F/$queue.dlq/get" |
        jq -e --arg code "$code" --arg queue "$queue" '.[] | select((.payload|fromjson?).codigoSeguimiento==$code) | .properties.headers["x-death"][] | select(.queue==$queue and .reason=="rejected") | {queue,reason,exchange,"routing-keys"}'; then
      found=true
      break
    fi
    sleep 1
  done
  if [[ $found != true ]]; then echo "Sin evidencia para $code en $queue.dlq" >&2; exit 1; fi
  docker logs "${NOTIFY_CONTAINER:-ms-notify}" 2>&1 | grep -F "ACK cola=$queue codigo=$run-$domain-false "
done
