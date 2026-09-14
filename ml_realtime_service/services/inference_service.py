import os
import sys
import math
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer
from typing import Tuple

# Resolve shared configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings


class RealtimeInferenceService:
    def __init__(self):
        self._session: ort.InferenceSession | None = None
        self._tokenizer: Tokenizer | None = None
        self.logit_threshold: float = 0.0

    def load_model(self) -> None:
        """Initializes the ONNX session and fast Rust tokenizer."""
        # Calculate logit threshold from probability cutoff
        p = settings.DECISION_THRESHOLD
        self.logit_threshold = math.log(p / (1.0 - p))

        sess_options = ort.SessionOptions()
        sess_options.intra_op_num_threads = 1
        sess_options.inter_op_num_threads = 1
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        sess_options.enable_cpu_mem_arena = False
        sess_options.enable_mem_pattern = False

        # Support container-local or custom-configured paths
        model_path = settings.MODEL_PATH
        if not os.path.isabs(model_path) and not os.path.exists(model_path):
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            model_path = os.path.join(base_dir, "ml", "student_cnn.onnx")

        tokenizer_dir = settings.TOKENIZER_DIR
        if not os.path.isabs(tokenizer_dir) and not os.path.exists(tokenizer_dir):
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            tokenizer_dir = os.path.join(base_dir, "ml")

        self._session = ort.InferenceSession(
            model_path,
            sess_options=sess_options,
            providers=["CPUExecutionProvider"]
        )

        tokenizer_path = os.path.join(tokenizer_dir, "tokenizer.json")
        self._tokenizer = Tokenizer.from_file(tokenizer_path)
        self._tokenizer.enable_truncation(max_length=settings.MAX_SEQUENCE_LENGTH)
        self._tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")

    def predict_single(self, text: str) -> Tuple[bool, float]:
        """Runs inference for a single text input and returns (is_transactional, confidence)."""
        if self._session is None or self._tokenizer is None:
            raise RuntimeError("Model session is not loaded.")

        encoding = self._tokenizer.encode(text)
        raw_seq_len = len(encoding.ids)
        seq_len = max(raw_seq_len, 4)  # Enforce minimum sequence length for CNN kernels

        input_ids = np.zeros((1, seq_len), dtype=np.int64)
        input_ids[0, :raw_seq_len] = encoding.ids

        logits = self._session.run(["logits"], {"input_ids": input_ids})[0]
        logit_val = float(logits.flatten()[0])

        # Sigmoid probability calculation
        probability = 1.0 / (1.0 + math.exp(-logit_val))
        is_transactional = logit_val >= self.logit_threshold
        confidence = probability if is_transactional else (1.0 - probability)

        return is_transactional, round(confidence, 4)


realtime_inference_service = RealtimeInferenceService()