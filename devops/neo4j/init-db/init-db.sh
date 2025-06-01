#!/bin/bash
echo "Initializing Neo4j database..."

TIMEOUT=300
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  cypher-shell -u neo4j -p password-secret -a bolt://localhost:7687 "CALL db.ping()" 2>error.log
  if [ $? -eq 0 ]; then
    echo "Neo4j is ready!"
    rm error.log
    break
  fi
  echo "Waiting for Neo4j to be ready... ($ELAPSED/$TIMEOUT seconds)"
  cat error.log
  neo4j status
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
  echo "Error: Neo4j did not become ready within $TIMEOUT seconds."
  cat error.log
  neo4j status
  exit 1
fi

echo "Creating database salesdb..."
cypher-shell -u neo4j -p password-secret -a bolt://localhost:7687 -d system "CREATE DATABASE salesdb IF NOT EXISTS;" 2>error.log
if [ $? -ne 0 ]; then
  echo "Error: Failed to create database salesdb."
  cat error.log
  exit 1
fi

echo "Database salesdb created."