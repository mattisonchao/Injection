#!/usr/bin/env python3
"""Produce random AVRO OrderEvent records to a Pulsar topic (SQLCatalog E2E).

Usage:
  python3 produce_pulsar_avro.py --token <JWT> \
      [--service-url pulsar+ssl://host:6651] [--topic persistent://public/default/order-events] \
      [--count 10] [--interval 0] [--start-id 1]
"""
import argparse
import random
import string
import time

import pulsar
from pulsar.schema import Array, AvroSchema, Double, Integer, Long, Map, Record, String


class Customer(Record):
    id = String()
    email = String()


class Item(Record):
    sku = String()
    qty = Integer()
    price = Double()


class OrderEvent(Record):
    order_id = Long()
    customer = Customer
    items = Array(Item())
    tags = Array(String())
    metadata = Map(String())
    status = String()
    amount = Double()
    created_at = Long()


def rand_email():
    return "".join(random.choices(string.ascii_lowercase, k=8)) + "@example.com"


def rand_event(order_id):
    return OrderEvent(
        order_id=order_id,
        customer=Customer(id=str(random.randint(1000, 9999)), email=rand_email()),
        items=[
            Item(
                sku="SKU-%03d" % random.randint(100, 999),
                qty=random.randint(1, 5),
                price=round(random.uniform(1, 100), 2),
            )
            for _ in range(random.randint(1, 3))
        ],
        tags=random.sample(["new", "hot", "sale", "promo", "bulk"], k=random.randint(1, 3)),
        metadata={"source": "e2e-producer", "shard": str(random.randint(0, 9))},
        status=random.choice(["NEW", "PROCESSING", "DONE", "CANCELLED"]),
        amount=round(random.uniform(0, 500), 2),
        created_at=int(time.time() * 1000),
    )


def main():
    parser = argparse.ArgumentParser(description="Produce random AVRO OrderEvent records to Pulsar")
    parser.add_argument("--service-url", default="pulsar+ssl://pc-5148c1a4.aws-usw1-dev-qeg8u.test.aws.sn2.dev:6651")
    parser.add_argument("--token", required=True, help="Pulsar AuthV2 JWT")
    parser.add_argument("--topic", default="persistent://public/default/order-events")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=0.0, help="seconds between messages")
    parser.add_argument("--start-id", type=int, default=1)
    args = parser.parse_args()

    client = pulsar.Client(args.service_url, authentication=pulsar.AuthenticationToken(args.token))
    try:
        producer = client.create_producer(args.topic, schema=AvroSchema(OrderEvent))
        for i in range(args.count):
            event = rand_event(args.start_id + i)
            producer.send(event)
            print("sent", event.order_id, event.status, event.amount, flush=True)
            if args.interval:
                time.sleep(args.interval)
        producer.close()
    finally:
        client.close()
    print("done")


if __name__ == "__main__":
    main()
