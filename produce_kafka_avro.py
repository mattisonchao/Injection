#!/usr/bin/env python3
"""Produce random AVRO OrderEvent records to a Kafka topic (SQLCatalog E2E).

Creates the topic if missing, registers the AVRO schema with the Schema
Registry, and produces Confluent wire-format (magic byte + schema id + avro).

Usage:
  python3 produce_kafka_avro.py --token <KAFKA_JWT> --schema-registry-token <SR_JWT> \
      [--bootstrap host:9093] [--schema-registry-url https://host] \
      [--topic order-events] [--count 10] \
      [--partitions N] [--replication-factor 3] [--no-create-topic]
"""
import argparse
import base64
import io
import json
import random
import ssl
import string
import struct
import time
import urllib.request
import urllib.request as _urllib

import fastavro
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "order_event.avro"
AVRO_SCHEMA = json.loads(SCHEMA_PATH.read_text())


def username_from_token(token):
    """Extract the AuthV2 SASL username (JWT 'sub' claim)."""
    pad = lambda x: x + '=' * (-len(x) % 4)
    import base64 as _b64
    payload = json.loads(_b64.urlsafe_b64decode(pad(token.split('.')[1])))
    return payload.get("sub") or "token"


def rand_email():
    return "".join(random.choices(string.ascii_lowercase, k=8)) + "@example.com"


def rand_event(order_id):
    return {
        "order_id": order_id,
        "customer": {"id": str(random.randint(1000, 9999)), "email": rand_email()},
        "items": [
            {
                "sku": "SKU-%03d" % random.randint(100, 999),
                "qty": random.randint(1, 5),
                "price": round(random.uniform(1, 100), 2),
            }
            for _ in range(random.randint(1, 3))
        ],
        "tags": random.sample(["new", "hot", "sale", "promo", "bulk"], k=random.randint(1, 3)),
        "metadata": {"source": "e2e-producer", "shard": str(random.randint(0, 9))},
        "status": random.choice(["NEW", "PROCESSING", "DONE", "CANCELLED"]),
        "amount": round(random.uniform(0, 500), 2),
        "created_at": int(time.time() * 1000),
    }


def register_schema(base_url, username, token, topic):
    subject = topic + "-value"
    body = json.dumps({"schema": json.dumps(AVRO_SCHEMA)}).encode()
    request = urllib.request.Request(
        base_url.rstrip("/") + "/subjects/" + subject + "/versions",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Authorization": "Basic " + base64.b64encode((username + ":" + token).encode()).decode(),
        },
    )
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with _urllib.urlopen(request, timeout=15, context=context) as response:
        return json.load(response)["id"]


def make_ssl_context():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def main():
    parser = argparse.ArgumentParser(description="Produce random AVRO OrderEvent records to Kafka")
    parser.add_argument("--bootstrap", default="<kafka-url>:9093")
    parser.add_argument("--token", required=True, help="Kafka AuthV2 JWT")
    parser.add_argument("--sasl-username", default=None, help="SASL username (default: from token sub claim)")
    parser.add_argument("--schema-registry-url", default="https://<schema-registry-url>")
    parser.add_argument("--schema-registry-token", required=True, help="Schema Registry AuthV2 JWT")
    parser.add_argument("--schema-registry-username", default=None, help="Schema Registry username (default: from token sub claim)")
    parser.add_argument("--topic", default="order-events")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--partitions", type=int, default=1, help="number of partitions for the topic (1 = non-partitioned)")
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--no-create-topic", action="store_true")
    parser.add_argument("--raw", action="store_true", help="produce raw AVRO bytes (no Confluent wire header)")
    args = parser.parse_args()
    if not args.sasl_username:
        args.sasl_username = username_from_token(args.token)
    if not args.schema_registry_username:
        args.schema_registry_username = username_from_token(args.schema_registry_token)
    print("sasl username:", args.sasl_username)
    print("registry username:", args.schema_registry_username)

    ssl_context = make_ssl_context()
    if not args.no_create_topic:
        admin = KafkaAdminClient(
            bootstrap_servers=[args.bootstrap],
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=args.sasl_username,
            sasl_plain_password=args.token,
            ssl_context=ssl_context,
            client_id="e2e-producer-admin",
        )
        try:
            admin.create_topics([NewTopic(args.topic, args.partitions, args.replication_factor)])
            print("created topic", args.topic)
        except TopicAlreadyExistsError:
            print("topic already exists", args.topic)
        admin.close()

    schema_id = register_schema(args.schema_registry_url, args.schema_registry_username, args.schema_registry_token, args.topic)
    print("registered schema id", schema_id)

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=args.sasl_username,
        sasl_plain_password=args.token,
        ssl_context=ssl_context,
        client_id="e2e-producer",
    )
    for i in range(args.count):
        event = rand_event(i + 1)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, AVRO_SCHEMA, event)
        payload = buffer.getvalue()
        value = payload if args.raw else b"\x00" + struct.pack(">I", schema_id) + payload
        future = producer.send(args.topic, value=value)
        future.get(timeout=15)
        print("sent", event["order_id"], event["status"], event["amount"], flush=True)
    producer.close()
    print("done")


if __name__ == "__main__":
    main()
