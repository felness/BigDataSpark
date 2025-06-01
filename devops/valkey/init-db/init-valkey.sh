#!/bin/bash
echo "Initializing Valkey database..."

TIMEOUT=60
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  valkey-cli -h localhost -p 6379 PING 2>error.log
  if [ $? -eq 0 ]; then
    echo "Valkey is ready!"
    rm error.log
    break
  fi
  echo "Waiting for Valkey to be ready... ($ELAPSED/$TIMEOUT seconds)"
  cat error.log 2>/dev/null || echo "No connection error yet."
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
  echo "Error: Valkey did not become ready within $TIMEOUT seconds."
  cat error.log 2>/dev/null || echo "No error log available."
  exit 1
fi

echo "Selecting database sales_db (index 1)..."
valkey-cli -h localhost -p 6379 <<EOF
SELECT 1
SET init_key "sales_db_initialized"
EOF

if [ $? -eq 0 ]; then
  echo "Database sales_db (index 1) initialized with test key."
else
  echo "Error: Failed to initialize sales_db."
  exit 1
fi