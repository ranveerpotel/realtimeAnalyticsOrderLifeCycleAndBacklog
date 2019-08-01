"""
Order event simulator — writes realistic order lifecycle events directly to
PostgreSQL (simulating Oracle EBS transactions). Debezium CDC then picks them up.

Usage:
    python order_simulator.py --rate 500 --duration 300
"""

import argparse
import asyncio
import logging
import random
import string
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BUSINESS_UNITS = ["North America", "EMEA", "APAC", "Latin America"]
CHANNELS = ["Direct", "Distributor", "OEM", "eCommerce", "Partner"]
PRODUCT_FAMILIES = ["Networking", "Security", "Compute", "Storage", "Collaboration", "Software"]
ORDER_STATUSES = ["CREATED", "ALLOCATED", "IN_PRODUCTION", "SHIPPED", "DELIVERED", "CANCELLED"]
STATUS_TRANSITIONS = {
    "CREATED":       ["ALLOCATED", "CANCELLED"],
    "ALLOCATED":     ["IN_PRODUCTION", "CANCELLED"],
    "IN_PRODUCTION": ["SHIPPED"],
    "SHIPPED":       ["DELIVERED"],
    "DELIVERED":     [],
    "CANCELLED":     [],
}
WAREHOUSES = ["WH-SJC", "WH-AMS", "WH-SIN", "WH-GRU"]
CARRIERS = ["FedEx", "UPS", "DHL", "Amazon Logistics"]


@dataclass
class SimulatedOrder:
    order_id: str
    customer_id: str
    customer_name: str
    channel: str
    business_unit: str
    status: str
    line_items: list = field(default_factory=list)


def random_id(prefix: str = "") -> str:
    return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def random_customer():
    companies = [
        "Acme Corp", "GlobalTech", "NexGen Systems", "PrimeSoft", "Vertex Industries",
        "CoreLogic", "DataBridge", "FutureNet", "Optima Solutions", "Pinnacle Group"
    ]
    name = random.choice(companies) + f" {random.randint(100, 999)}"
    return random_id("CUST-"), name


class OrderSimulator:
    def __init__(self, dsn: str, rate: int):
        self.dsn = dsn
        self.rate = rate
        self.pool: Optional[asyncpg.Pool] = None
        self.active_orders: dict[str, SimulatedOrder] = {}

    async def start(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=5, max_size=20)
        log.info("Connected to PostgreSQL. Starting simulation at %d events/sec.", self.rate)

    async def stop(self):
        if self.pool:
            await self.pool.close()

    async def create_order(self):
        order_id = random_id("ORD-")
        customer_id, customer_name = random_customer()
        channel = random.choice(CHANNELS)
        bu = random.choice(BUSINESS_UNITS)
        priority = random.choices([1, 2, 3, 4, 5], weights=[5, 15, 40, 25, 15])[0]

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO orders (order_id, customer_id, customer_name, channel,
                                       business_unit, order_status, priority_tier,
                                       total_value, currency, expected_ship_date)
                    VALUES ($1,$2,$3,$4,$5,'CREATED',$6,0,'USD',$7)
                    """,
                    order_id, customer_id, customer_name, channel, bu, priority,
                    date.today() + timedelta(days=random.randint(5, 30)),
                )

                total = Decimal("0")
                items = []
                for _ in range(random.randint(1, 5)):
                    line_id = random_id("LINE-")
                    sku = random_id("SKU-")
                    family = random.choice(PRODUCT_FAMILIES)
                    qty = Decimal(str(random.randint(1, 100)))
                    price = Decimal(str(random.uniform(100, 10000))).quantize(Decimal("0.01"))
                    total += qty * price

                    await conn.execute(
                        """
                        INSERT INTO order_line_items
                            (line_id, order_id, sku, product_family, description, quantity, unit_price)
                        VALUES ($1,$2,$3,$4,$5,$6,$7)
                        """,
                        line_id, order_id, sku, family,
                        f"{family} Component Model {random.randint(1000, 9999)}",
                        qty, price,
                    )
                    items.append({"sku": sku, "qty": float(qty)})

                await conn.execute(
                    "UPDATE orders SET total_value=$1 WHERE order_id=$2",
                    total, order_id,
                )

                # Seed inventory for the skus
                for item in items:
                    wh = random.choice(WAREHOUSES)
                    available = random.uniform(0, item["qty"] * 3)
                    await conn.execute(
                        """
                        INSERT INTO inventory (sku, warehouse_id, available_qty, on_order_qty)
                        VALUES ($1,$2,$3,$4)
                        ON CONFLICT (sku, warehouse_id) DO UPDATE
                          SET available_qty = inventory.available_qty + EXCLUDED.available_qty,
                              updated_at = NOW()
                        """,
                        item["sku"], wh, round(available, 3), round(item["qty"] * 1.5, 3),
                    )

        order = SimulatedOrder(order_id, customer_id, customer_name, channel, bu, "CREATED")
        self.active_orders[order_id] = order

    async def advance_order(self, order: SimulatedOrder):
        next_statuses = STATUS_TRANSITIONS.get(order.status, [])
        if not next_statuses:
            self.active_orders.pop(order.order_id, None)
            return

        new_status = random.choice(next_statuses)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE orders SET order_status=$1, updated_at=NOW() WHERE order_id=$2",
                    new_status, order.order_id,
                )

                if new_status == "ALLOCATED":
                    rows = await conn.fetch(
                        "SELECT line_id, sku FROM order_line_items WHERE order_id=$1",
                        order.order_id,
                    )
                    for row in rows:
                        alloc_id = random_id("ALLOC-")
                        wh = random.choice(WAREHOUSES)
                        await conn.execute(
                            """
                            INSERT INTO allocation_records
                                (allocation_id, order_id, sku, warehouse_id, allocated_qty)
                            VALUES ($1,$2,$3,$4,$5)
                            """,
                            alloc_id, order.order_id, row["sku"], wh, round(random.uniform(1, 50), 3),
                        )
                        await conn.execute(
                            """
                            UPDATE inventory
                               SET allocated_qty = allocated_qty + 1, updated_at = NOW()
                             WHERE sku=$1 AND warehouse_id=$2
                            """,
                            row["sku"], wh,
                        )

                elif new_status == "SHIPPED":
                    shipment_id = random_id("SHIP-")
                    await conn.execute(
                        """
                        INSERT INTO shipments
                            (shipment_id, order_id, carrier, tracking_number, status, ship_date)
                        VALUES ($1,$2,$3,$4,'SHIPPED',NOW())
                        """,
                        shipment_id, order.order_id,
                        random.choice(CARRIERS),
                        random_id("TRACK-"),
                    )

                elif new_status == "DELIVERED":
                    await conn.execute(
                        """
                        UPDATE shipments SET status='DELIVERED', delivery_date=NOW(), updated_at=NOW()
                         WHERE order_id=$1
                        """,
                        order.order_id,
                    )

        order.status = new_status
        if new_status in ("DELIVERED", "CANCELLED"):
            self.active_orders.pop(order.order_id, None)

    async def run_tick(self):
        # 70% chance to create a new order, 30% chance to advance an existing one
        if not self.active_orders or random.random() < 0.7:
            await self.create_order()
        else:
            order = random.choice(list(self.active_orders.values()))
            await self.advance_order(order)

    async def run(self, duration: Optional[int] = None):
        await self.start()
        interval = 1.0 / self.rate
        elapsed = 0.0
        tick = 0

        try:
            while duration is None or elapsed < duration:
                start = asyncio.get_event_loop().time()
                tasks = [self.run_tick() for _ in range(min(self.rate, 50))]
                await asyncio.gather(*tasks, return_exceptions=True)
                elapsed_tick = asyncio.get_event_loop().time() - start
                sleep_time = max(0, interval * len(tasks) - elapsed_tick)
                await asyncio.sleep(sleep_time)
                elapsed += elapsed_tick + sleep_time
                tick += len(tasks)

                if tick % 1000 == 0:
                    log.info(
                        "Tick %d | Active orders: %d | Elapsed: %.0fs",
                        tick, len(self.active_orders), elapsed,
                    )
        finally:
            await self.stop()


def main():
    parser = argparse.ArgumentParser(description="Order lifecycle event simulator")
    parser.add_argument("--dsn", default="postgresql://orders_user:orders_pass@localhost:5432/orders_db")
    parser.add_argument("--rate", type=int, default=500, help="Events per second")
    parser.add_argument("--duration", type=int, default=None, help="Run duration in seconds (default: infinite)")
    args = parser.parse_args()

    sim = OrderSimulator(args.dsn, args.rate)
    asyncio.run(sim.run(args.duration))


if __name__ == "__main__":
    main()
