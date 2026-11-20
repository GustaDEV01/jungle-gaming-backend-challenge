#!/usr/bin/env bash
# Smoke test of the docker compose deployment (3 instances on ports 3001-3003).
set -euo pipefail
for port in 3001 3002 3003; do
  for i in $(seq 1 90); do curl -sf "http://localhost:${port}/health/ready" > /dev/null && break; sleep 2; done
  curl -sf "http://localhost:${port}/health/ready" > /dev/null
  echo "instance :${port} ready"
done
player="smoke-$(date +%s%N)"
wallet=$(curl -sf -X POST localhost:3001/wallets -H 'content-type: application/json' \
  -d "{\"playerId\":\"${player}\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
wallet_id=$(echo "$wallet" | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "wallet ${wallet_id}"
bet() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "localhost:$1/wagering/transactions" -H 'content-type: application/json' \
    -H "Idempotency-Key: provider-a:$2" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$2\",\"playerId\":\"${player}\",\"walletId\":\"${wallet_id}\",\"roundId\":\"r1\",\"gameId\":\"g1\",\"kind\":\"BET\",\"money\":{\"amount\":\"80.00\",\"currency\":\"BRL\"}}"
}
# two concurrent 80.00 bets on two different instances
bet 3002 "${player}-a" > /tmp/a & bet 3003 "${player}-b" > /tmp/b & wait
codes="$(cat /tmp/a) $(cat /tmp/b)"
echo "status codes: ${codes}"
[[ "$codes" == *201* && "$codes" == *422* ]] || { echo "expected one 201 and one 422"; exit 1; }
rec=$(curl -sf -X POST "localhost:3001/wallets/${wallet_id}/reconciliation")
echo "$rec"
echo "$rec" | grep -q '"consistent":true'
echo "$rec" | grep -q '"storedBalance":{"amount":"20.00"'
echo "smoke OK"
