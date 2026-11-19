#!/usr/bin/env bash
set -eu
api=http://localhost:9095/api/kafka/topics
name="ev3-prueba-$(date +%s)-$$"
req() { curl -fsS -H 'Content-Type: application/json' "$@"; echo; }
req "$api"
req -X POST -d "{\"name\":\"$name\",\"partitions\":3,\"replicationFactor\":3}" "$api"
req "$api/$name"
req "$api/$name/config"
req -X PATCH -d '{"retentionMs":86400000,"cleanupPolicy":"delete"}' "$api/$name/config"
curl -fsS "$api/$name/config" | jq -e '."retention.ms"=="86400000" and ."cleanup.policy"=="delete"'
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
  -X PATCH -d '{"retentionMs":-9}' "$api/$name/config")
[[ "$code" == 400 ]]
req -X DELETE "$api/$name"
