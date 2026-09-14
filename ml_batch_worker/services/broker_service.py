import sys
import os
import json
import aio_pika
from typing import List, Tuple, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings


class BrokerService:
    def __init__(self):
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.RobustChannel] = None

    async def connect(self):
        if not self._connection or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            self._channel = await self._connection.channel()
            await self._channel.declare_queue(settings.DB_QUEUE_NAME, durable=True)

    async def publish_db_batch(self, records: List[Tuple[Optional[str], str, bool]]):
        """Sends classified batch records to the DB worker queue."""
        if not self._channel or self._channel.is_closed:
            await self.connect()

        payload = {"records": records}
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await self._channel.default_exchange.publish(
            message,
            routing_key=settings.DB_QUEUE_NAME
        )

    async def close(self):
        if self._connection and not self._connection.is_closed:
            await self._connection.close()


batch_broker_service = BrokerService()