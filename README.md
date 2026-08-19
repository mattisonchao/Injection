# SQLWorkspace + SQLCatalog E2E — AVRO data producers

Python producers that create a topic (if missing), register the AVRO
`OrderEvent` schema, and publish sample records for SQLWorkspace/SQLCatalog
end-to-end validation on StreamNative Cloud.

## Scripts

| Script | Upstream | Payload format |
|---|---|---|
| `produce_kafka_avro.py` | Kafka | Confluent wire format (magic byte + schema id + raw Avro) |
| `produce_pulsar_avro.py` | Pulsar | Pulsar AVRO schema |

Both scripts use the same `OrderEvent` schema used by the SQLCatalog
integration tests: `order_id`, `customer` (record), `items` (array),
`tags`, `metadata`, `status` (enum), `amount`, `created_at`.

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Pulsar

```bash
python3 produce_pulsar_avro.py \
  --token "$(cat /tmp/stg-pulsar.token)" \
  --count 10
```

### Kafka

```bash
python3 produce_kafka_avro.py \
  --token "$(cat /tmp/stg-kafka.token)" \
  --schema-registry-token "$(cat /tmp/stg-sr.token)" \
  --bootstrap <kafka-url>:9093 \
  --schema-registry-url https://<schema-registry-url> \
  --count 10
```
