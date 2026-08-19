# SQLWorkspace + SQLCatalog E2E — AVRO data producers

Python producers that create a topic (if missing), register the AVRO
`OrderEvent` schema, and publish sample records for SQLWorkspace/SQLCatalog
end-to-end validation on StreamNative Cloud.

## Scripts

| Script | Upstream | Payload format |
|---|---|---|
| `produce_kafka_avro.py` | Kafka (AuthV2) | Confluent wire format (magic byte + schema id + raw Avro) |
| `produce_pulsar_avro.py` | Pulsar (AuthV2) | Pulsar AVRO schema |

Both scripts use the same `OrderEvent` schema used by the SQLCatalog
integration tests: `order_id`, `customer` (record), `items` (array),
`tags`, `metadata`, `status` (enum), `amount`, `created_at`.

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Finding tokens

AuthV2 JWTs can be found in the data-plane cluster-token Secrets in the
workspace namespace:

```bash
# Pulsar
kubectl get secret global-apikey-cluster-token-<pulsar-cluster> -n <org> \
  -o jsonpath='{.data.token}' | base64 -d

# Kafka
kubectl get secret global-apikey-cluster-token-<kafka-cluster>-kafka -n <org> \
  -o jsonpath='{.data.token}' | base64 -d

# Kafka Schema Registry
kubectl get secret global-apikey-cluster-token-<kafka-cluster>-schema-registry -n <org> \
  -o jsonpath='{.data.token}' | base64 -d
```

SASL usernames are the JWT `sub` claim and are extracted automatically.

## Usage (staging example)

### Pulsar

```bash
python3 produce_pulsar_avro.py \
  --token "$(cat /tmp/stg-pulsar.token)" \
  --service-url pulsar+ssl://<pulsar-url>:6651 \
  --topic persistent://public/default/order-events \
  --count 10 --interval 0 --start-id 1
```

### Kafka

```bash
python3 produce_kafka_avro.py \
  --token "$(cat /tmp/stg-kafka.token)" \
  --schema-registry-token "$(cat /tmp/stg-sr.token)" \
  --bootstrap <kafka-url>:9093 \
  --schema-registry-url https://<schema-registry-url> \
  --topic order-events --count 10 --start-id 1
```

Cluster endpoints come from the cluster status
(`kubectl get kafkacluster/pulsarcluster <name> -n <org> -o yaml`).

## End-to-end verification flow

1. Produce initial records (creates the topic + registers the schema).
2. Wait for the SQLCatalog to discover the topic and sync the RisingWave
   source (`kubectl get sqlcatalog <name> -n <org>` → phase `Ready`).
3. Create a materialized view over the source.
4. Produce more records **after** the MV exists — catalog sources are shared
   and disable snapshot backfill, so only post-MV events are ingested.
5. Query the MV through the workspace gateway (psql :4567 or the WebSocket
   endpoint) and verify the rows.

## Gotchas

- Kafka payloads must be the **Confluent wire format** (`\x00` + 4-byte schema
  id + raw Avro). The `--raw` mode exists only as a fallback and will not parse
  in RisingWave; `fastavro.schemaless_writer` is used, not
  `fastavro.writer` (object container files are rejected).
- Kafka's external listener can be flaky from a laptop
  (`NoBrokersAvailable` on connection resets) — retry the script. RisingWave
  inside the cluster connects fine.
- Pulsar schema quirk: `Array(Item())` (instance), not `Array(Item)`; use
  `AvroSchema(OrderEvent)` on the producer.
- The SQLWorkspace/SQLCatalog databases must exist before the topic is
  consumed; create the SQLCatalog first, then produce.

## Reference

- `SQLWorkspace-SQLCatalog-E2E-Producers.md` (Desktop) — original handoff notes
- `StreamNative_SQL_Workspace_Private_Preview_Testing_Guide_Staging.docx`
  (Desktop) — staging validation report that used these scripts
