import os
import sys
import math
import time
from typing import List, Tuple, Dict, Optional
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings


class BatchInferenceService:
    def __init__(self):
        self._executor: Optional[ThreadPoolExecutor] = None
        self._session: Optional[ort.InferenceSession] = None
        self._tokenizer: Optional[Tokenizer] = None

    def start_pool(self):
        """Initializes the shared ONNX session and tokenizers for thread execution."""
        sess_options = ort.SessionOptions()
        sess_options.intra_op_num_threads = 1
        sess_options.inter_op_num_threads = 1
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        sess_options.enable_cpu_mem_arena = False
        sess_options.enable_mem_pattern = False

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

        tokenizer_file = os.path.join(tokenizer_dir, "tokenizer.json")
        self._tokenizer = Tokenizer.from_file(tokenizer_file)
        self._tokenizer.enable_truncation(max_length=settings.MAX_SEQUENCE_LENGTH)
        self._tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")

        self._executor = ThreadPoolExecutor(max_workers=settings.NUM_WORKERS)

    def shutdown_pool(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

    def _process_chunk(self, worker_id: int, texts_chunk: List[str], threshold: float) -> Tuple[int, int, float, float, List[bool]]:
        if not texts_chunk:
            return worker_id, 0, 0.0, 0.0, []

        chunk_start_wall = time.perf_counter()
        chunk_start_cpu = time.thread_time()

        batch_size = settings.BATCH_SIZE
        total = len(texts_chunk)
        predictions = []
        logit_threshold = math.log(threshold / (1.0 - threshold))

        for i in range(0, total, batch_size):
            sub_batch = texts_chunk[i : i + batch_size]
            b_size = len(sub_batch)

            encodings = self._tokenizer.encode_batch_fast(sub_batch)
            raw_seq_len = len(encodings[0].ids) if encodings else 0
            seq_len = max(raw_seq_len, 4)

            input_ids = np.zeros((b_size, seq_len), dtype=np.int64)
            for row_idx, enc in enumerate(encodings):
                input_ids[row_idx, :raw_seq_len] = enc.ids

            logits = self._session.run(["logits"], {"input_ids": input_ids})[0]
            is_trans = (logits.flatten() >= logit_threshold).tolist()
            predictions.extend(is_trans)

        chunk_elapsed_wall = time.perf_counter() - chunk_start_wall
        chunk_elapsed_cpu = time.thread_time() - chunk_start_cpu

        return worker_id, total, chunk_elapsed_wall, chunk_elapsed_cpu, predictions

    def classify_parallel(self, texts: List[str]) -> Tuple[List[bool], float, List[Dict]]:
        total_records = len(texts)
        if total_records == 0:
            return [], 0.0, []

        infer_start = time.perf_counter()

        lengths = np.fromiter((len(t) for t in texts), dtype=np.int32, count=total_records)
        sorted_indices = np.argsort(lengths)
        sorted_texts = [texts[i] for i in sorted_indices]

        num_workers = min(settings.NUM_WORKERS, total_records)
        chunks = [sorted_texts[i::num_workers] for i in range(num_workers)]

        futures = [
            self._executor.submit(self._process_chunk, idx + 1, chunk, settings.DECISION_THRESHOLD)
            for idx, chunk in enumerate(chunks)
        ]

        worker_predictions = []
        chunk_metrics = []
        for f in futures:
            w_id, count, elapsed_wall, elapsed_cpu, preds = f.result()
            worker_predictions.append(preds)
            chunk_metrics.append({
                "worker_id": w_id,
                "records_count": count,
                "wall_time": elapsed_wall,
                "cpu_time": elapsed_cpu
            })

        sorted_preds = [None] * total_records
        for worker_idx, preds in enumerate(worker_predictions):
            sorted_preds[worker_idx::num_workers] = preds

        final_predictions = np.empty(total_records, dtype=bool)
        final_predictions[sorted_indices] = sorted_preds

        total_inference_wall_time = time.perf_counter() - infer_start
        return final_predictions.tolist(), total_inference_wall_time, chunk_metrics


batch_inference_service = BatchInferenceService()