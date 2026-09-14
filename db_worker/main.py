import sys
import os
import asyncio
import json
import aio_pika

# Add root workspace to path to resolve shared_libs and local services
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from shared_libs.config import settings
from services.db_service import db_service


async def process_db_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            payload = json.loads(message.body.decode())

            # Safely handle both payload formats: direct list or dict with "records"
            if isinstance(payload, dict):
                records = payload.get("records", [])
            elif isinstance(payload, list):
                records = payload
            else:
                records = []

            if records:
                # Reconstruct tuples: (mobile, message, is_transactional)
                db_tuples = [
                    (
                        r[0] if isinstance(r, (list, tuple)) else r.get("mobile"),
                        r[1] if isinstance(r, (list, tuple)) else r.get("message"),
                        bool(r[2] if isinstance(r, (list, tuple)) else r.get("is_transactional"))
                    )
                    for r in records
                ]
                inserted = await db_service.bulk_insert_classifications(db_tuples)
                print(f"[DB Worker] Successfully inserted {inserted:,} records into PostgreSQL.")
        except Exception as e:
            print(f"[!] Database insertion failed: {e}")


async def main():
    print("[*] Starting DB Worker...")

    # 1. Initialize PostgreSQL Connection Pool with retry
    pg_connected = False
    for attempt in range(1, 15):
        try:
            await db_service.connect()
            print("[*] Successfully connected to PostgreSQL pool.")
            pg_connected = True
            break
        except Exception as e:
            print(f"[*] Waiting for PostgreSQL... (Attempt {attempt}/15). Retrying in 3s.")
            await asyncio.sleep(3)

    if not pg_connected:
        print("[!] Fatal: Could not connect to PostgreSQL. Exiting.")
        sys.exit(1)

    # 2. Connect to RabbitMQ with robust retry loop
    connection = None
    for attempt in range(1, 15):
        try:
            connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            print("[*] Successfully connected to RabbitMQ.")
            break
        except Exception as e:
            print(f"[*] Waiting for RabbitMQ... (Attempt {attempt}/15). Retrying in 5s.")
            await asyncio.sleep(5)

    if not connection:
        print("[!] Fatal: Could not connect to RabbitMQ. Exiting.")
        sys.exit(1)

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=5)
    queue = await channel.declare_queue(settings.DB_QUEUE_NAME, durable=True)

    print(f"[*] DB Worker listening on queue: '{settings.DB_QUEUE_NAME}'")
    await queue.consume(process_db_message)

    try:
        await asyncio.Future()  # Keeps worker process alive
    finally:
        await connection.close()
        await db_service.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("[*] DB Worker stopped cleanly.")