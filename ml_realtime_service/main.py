import sys
import os
import asyncio
import grpc
from concurrent import futures

# Add root workspace to path to resolve imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from shared_libs.config import settings
from shared_libs.proto import sms_pb2, sms_pb2_grpc
from services.inference_service import realtime_inference_service


class SMSClassifierServicer(sms_pb2_grpc.SMSClassifierServicer):
    def ClassifySingle(self, request, context):
        try:
            is_trans, conf = realtime_inference_service.predict_single(request.message)
            category = "transactional" if is_trans else "promotional"

            return sms_pb2.SinglePredictResponse(
                status="success",
                result=sms_pb2.ClassificationResult(
                    mobile=request.mobile or "",
                    message=request.message,
                    category=category,
                    is_transactional=is_trans,
                    confidence=float(conf)
                )
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Inference error: {str(e)}")
            return sms_pb2.SinglePredictResponse(status="error")


def serve():
    print("[*] Pre-warming ONNX model and tokenizer for real-time service...")
    realtime_inference_service.load_model()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    sms_pb2_grpc.add_SMSClassifierServicer_to_server(SMSClassifierServicer(), server)

    # Bind host address configured in shared settings
    host_port = settings.GRPC_SERVER_HOST
    server.add_insecure_port(host_port)
    print(f"[*] Real-time ML gRPC service running on {host_port}")
    
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()