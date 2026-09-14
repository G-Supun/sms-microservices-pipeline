import sys
import os
import json
import aio_pika

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings


class GatewayBrokerService:
    def __init__(self):
        self._connection = None
        self._channel = None

    async def connect(self):
        if not self._connection or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            self._channel = await self._connection.channel()
            await self._channel.declare_queue(settings.JOB_QUEUE_NAME, durable=True)

    async def publish_batch_job(self, job_id: str, file_path: str):
        if not self._channel or self._channel.is_closed:
            await self.connect()

        payload = {
            "job_id": job_id,
            "file_path": file_path
        }
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await self._channel.default_exchange.publish(
            message,
            routing_key=settings.JOB_QUEUE_NAME
        )

    async def close(self):
        if self._connection and not self._connection.is_closed:
            await self._connection.close()


gateway_broker = GatewayBrokerService()