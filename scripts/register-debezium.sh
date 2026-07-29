#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
echo "Waiting for Kafka Connect at $CONNECT_URL ..."

i=0
until curl -sf "$CONNECT_URL/" >/dev/null; do
  i=$((i + 1))
  if [ "$i" -gt 60 ]; then
    echo "Connect did not become ready"
    exit 1
  fi
  sleep 2
done

# Wait a bit so Order.Api can EnsureCreated tables (connector needs outbox_messages)
echo "Delaying connector register so EF can create schema first..."
sleep 15

echo "Registering outbox connector..."
code=$(curl -s -o /tmp/resp.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data @/connector.json \
  "$CONNECT_URL/connectors" || true)

if [ "$code" = "201" ] || [ "$code" = "200" ]; then
  echo "Connector created ($code)"
elif [ "$code" = "409" ]; then
  echo "Connector exists — updating config..."
  # PUT /config expects only the config object
  sed -n '/"config"/,/^  }/p' /connector.json | sed '1d;$d' > /tmp/cfg.json || true
  # Fallback: use python-free approach with full replace delete+create
  curl -sf -X DELETE "$CONNECT_URL/connectors/orders-outbox-connector" || true
  sleep 2
  curl -sf -X POST -H "Content-Type: application/json" \
    --data @/connector.json \
    "$CONNECT_URL/connectors" >/dev/null
  echo "Connector recreated"
else
  echo "Register response HTTP $code"
  cat /tmp/resp.json 2>/dev/null || true
fi

curl -sf "$CONNECT_URL/connectors/orders-outbox-connector/status" || true
echo
echo "Debezium outbox connector registration finished."
