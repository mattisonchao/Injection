#!/usr/bin/env python3
"""Produce random AVRO OrderEvent records to a Pulsar topic (SQLCatalog E2E).

Usage:
  python3 produce_pulsar_avro.py --token <JWT> \
      [--service-url pulsar+ssl://<pulsar-url>:6651] \
      [--topic persistent://public/default/order-events] \
      [--count 10] [--partitions N]
"""
import argparse
import json
import random
import ssl
import string
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pulsar
from pulsar.schema import AvroSchema

SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "order_event.avro"
SCHEMA = json.loads(SCHEMA_PATH.read_text())


def admin_base_url(service_url):
    """Derive the admin URL from a pulsar service URL (pulsar+ssl://host:6651 -> https://host:443)."""
    scheme = "https" if service_url.startswith("pulsar+ssl") else "http"
    parts = urllib.parse.urlsplit(service_url.replace("pulsar+ssl", "https").replace("pulsar", "http"))
    return f"{scheme}://{parts.hostname}:443"


def create_partitioned_topic(admin_url, token, topic, partitions):
    """Create a partitioned topic with the given partition count (best effort)."""
    topic_path = topic.replace("persistent://", "", 1)
    url = f"{admin_url}/admin/v2/persistent/{topic_path}/partitions"
    body = json.dumps({"partitions": partitions}).encode()
    request = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
        },
    )
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(request, timeout=15, context=context) as response:
            print("created partitioned topic", topic, "partitions", partitions)
    except urllib.error.HTTPError as error:
        print("partitioned topic create skipped:", error.code, error.reason)


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


def main():
    parser = argparse.ArgumentParser(description="Produce random AVRO OrderEvent records to Pulsar")
    parser.add_argument("--service-url", default="pulsar+ssl://<pulsar-url>:6651")
    parser.add_argument("--token", required=True, help="Pulsar AuthV2 JWT")
    parser.add_argument("--topic", default="persistent://public/default/order-events")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--admin-url", default=None, help="Pulsar admin base URL (default: https://<host>:443 derived from --service-url)")
    parser.add_argument(
        "--partitions",
        type=int,
        default=0,
        help="number of partitions (0 = non-partitioned topic; N >= 1 = partitioned topic with N partitions)",
    )
    args = parser.parse_args()
    admin_url = args.admin_url or admin_base_url(args.service_url)

    client = pulsar.Client(args.service_url, authentication=pulsar.AuthenticationToken(args.token))
    try:
        if args.partitions >= 1:
            create_partitioned_topic(admin_url, args.token, args.topic, args.partitions)
        producer = client.create_producer(args.topic, schema=AvroSchema(None, schema_definition=SCHEMA))
        for i in range(args.count):
            event = rand_event(i + 1)
            producer.send(event)
            print("sent", event["order_id"], event["status"], event["amount"], flush=True)
        producer.close()
    finally:
        client.close()
    print("done")


if __name__ == "__main__":
    main()
