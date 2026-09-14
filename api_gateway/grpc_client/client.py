import sys
import os
import grpc

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings
from shared_libs.proto import sms_pb2, sms_pb2_grpc


class GrpcMLClient:
    def __init__(self):
        self._channel = None
        self._stub = None

    def connect(self):
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(settings.GRPC_SERVER_HOST)
            self._stub = sms_pb2_grpc.SMSClassifierStub(self._channel)

    async def classify_single(self, mobile: str | None, message: str):
        if self._stub is None:
            self.connect()

        request = sms_pb2.SingleSMSRequest(
            mobile=mobile or "",
            message=message
        )
        response = await self._stub.ClassifySingle(request)
        return response

    async def close(self):
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None


grpc_client = GrpcMLClient()