#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
APP_ENV="${APP_ENV:-local}"
REPLICATION="${REPLICATION:-3}"
MIN_ISR="${MIN_ISR:-2}"
ROUTES="${ROUTES:-http-wholesale-1}"
TOPICS_BIN="${TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

DAY=86400000

create() {
  local name="$1" partitions="$2" retention_ms="$3" cleanup="${4:-delete}"
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION" \
    --config "cleanup.policy=$cleanup" \
    --config "retention.ms=$retention_ms" \
    --config "min.insync.replicas=$MIN_ISR" \
    --config "compression.type=producer"
  echo "ok: $name ($partitions partições, $cleanup)"
}

with_retries() {
  local name="$1" partitions="$2"
  create "$name.retry.5m" "$partitions" $((2 * DAY))
  create "$name.retry.1h" "$partitions" $((2 * DAY))
}

create "sms.submit.$APP_ENV" 24 $((7 * DAY))
with_retries "sms.submit.$APP_ENV" 24

IFS=',' read -ra route_list <<< "$ROUTES"
for route in "${route_list[@]}"; do
  create "sms.dispatch.$route" 12 $((3 * DAY))
  with_retries "sms.dispatch.$route" 12
done

create "sms.status.dlr" 12 $((7 * DAY))
with_retries "sms.status.dlr" 12

create "webhook.outbound" 12 $((3 * DAY))
with_retries "webhook.outbound" 12

create "ledger.events" 12 $((30 * DAY))
with_retries "ledger.events" 12

create "campaign.progress" 6 -1 compact
create "link.clicks" 6 $((14 * DAY))
create "dlq.global" 3 $((30 * DAY))

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list
