import os
import gc
import sys
import json
import time
import asyncio
import aio_pika
import psutil
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from shared_libs.config import settings
from services.inference_service import batch_inference_service
from services.broker_service import batch_broker_service

STREAM_CHUNK_SIZE = 25000

def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    col_map = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand in col_map:
            return col_map[cand]
    return None

async def process_batch_job(job_id: str, file_path: str):
    # 1. Hardware & Process Baseline Initialization
    process = psutil.Process()
    logical_cores = psutil.cpu_count(logical=True) or 1
    physical_cores = psutil.cpu_count(logical=False) or max(1, logical_cores // 2)

    baseline_system_cpu_pct = psutil.cpu_percent(interval=None)
    ram_baseline_mb = process.memory_info().rss / (1024 * 1024)
    peak_ram_mb = ram_baseline_mb
    peak_raw_process_cpu_pct = 0.0

    process.cpu_percent(interval=None)
    full_start = time.perf_counter()

    filename = os.path.basename(file_path)
    print(f"\n[JOB START] File: '{filename}' | Hardware: {physical_cores} Cores / {logical_cores} Threads | Baseline RAM: {ram_baseline_mb:.2f} MB")
    print("-" * 85)

    total_processed = 0
    total_trans_count = 0
    total_promo_count = 0
    total_parse_time = 0.0
    total_infer_wall_time = 0.0
    total_pure_cpu_time = 0.0
    total_publish_time = 0.0
    chunk_index = 0
    aggregated_worker_timings: dict[int, dict] = {}

    try:
        chunk_iterator = pd.read_csv(
            file_path,
            dtype=str,
            keep_default_na=False,
            engine="c",
            chunksize=STREAM_CHUNK_SIZE
        )

        for chunk_df in chunk_iterator:
            chunk_index += 1
            p_start = time.perf_counter()
            chunk_len = len(chunk_df)
            if chunk_len == 0:
                continue

            msg_col = _find_column(chunk_df, ["message", "text", "sms", "body", "content"])
            mobile_col = _find_column(chunk_df, ["mobile", "phone", "number", "contact", "msisdn"])

            if not msg_col:
                raise ValueError(f"Required message column not found. Available: {list(chunk_df.columns)}")

            messages = chunk_df[msg_col].tolist()
            mobiles = chunk_df[mobile_col].tolist() if mobile_col else [None] * chunk_len
            total_parse_time += (time.perf_counter() - p_start)

            # Parallel Inference Cycle
            preds, chunk_wall, chunk_metrics = batch_inference_service.classify_parallel(messages)
            total_infer_wall_time += chunk_wall

            chunk_cpu_cycle = 0.0
            worker_details = []
            for metric in chunk_metrics:
                w_id = metric["worker_id"]
                w_cpu = metric["cpu_time"]
                w_wall = metric["wall_time"]
                w_recs = metric["records_count"]
                chunk_cpu_cycle += w_cpu

                if w_id not in aggregated_worker_timings:
                    aggregated_worker_timings[w_id] = {
                        "worker_id": w_id,
                        "records_count": 0,
                        "inference_wall_time_seconds": 0.0,
                        "cpu_compute_time_seconds": 0.0
                    }
                aggregated_worker_timings[w_id]["records_count"] += w_recs
                aggregated_worker_timings[w_id]["inference_wall_time_seconds"] += w_wall
                aggregated_worker_timings[w_id]["cpu_compute_time_seconds"] += w_cpu
                worker_details.append(f"W{w_id}: {w_cpu:.2f}s ({w_recs:,} msgs)")

            total_pure_cpu_time += chunk_cpu_cycle

            # Forward batch payload to DB worker queue via RabbitMQ
            pub_start = time.perf_counter()
            db_records = [(mobiles[i], messages[i], bool(preds[i])) for i in range(chunk_len)]
            await batch_broker_service.publish_db_batch(db_records)
            total_publish_time += (time.perf_counter() - pub_start)

            chunk_trans = sum(preds)
            total_trans_count += chunk_trans
            total_promo_count += (chunk_len - chunk_trans)
            total_processed += chunk_len

            # Real-time resource metrics sampling
            current_ram_mb = process.memory_info().rss / (1024 * 1024)
            if current_ram_mb > peak_ram_mb:
                peak_ram_mb = current_ram_mb

            raw_process_cpu = process.cpu_percent(interval=None)
            if raw_process_cpu > peak_raw_process_cpu_pct:
                peak_raw_process_cpu_pct = raw_process_cpu

            normalized_host_cpu = raw_process_cpu / logical_cores
            chunk_speed = chunk_len / max(chunk_wall, 0.0001)

            print(
                f"[CHUNK {chunk_index:02d}] Records: {chunk_len:,} | "
                f"Infer Wall: {chunk_wall:.2f}s | "
                f"CPU Compute: {chunk_cpu_cycle:.2f}s | "
                f"Host CPU: {normalized_host_cpu:4.1f}% | "
                f"RAM: {current_ram_mb:5.1f} MB | "
                f"Speed: {chunk_speed:,.0f} msg/s | "
                f"[{', '.join(worker_details)}]"
            )

            del chunk_df, messages, mobiles, preds

        gc.collect()
        total_wall_time = max(time.perf_counter() - full_start, 0.0001)
        ram_overhead_mb = max(peak_ram_mb - ram_baseline_mb, 0.0)
        throughput = round(total_processed / total_wall_time, 2)
        avg_latency_ms = round((total_wall_time / total_processed) * 1000, 4)

        # Normalized Resource Usage Calculations
        host_normalized_peak_cpu = round(peak_raw_process_cpu_pct / logical_cores, 1)
        dedicated_core_utilization = round(peak_raw_process_cpu_pct / physical_cores, 1)
        vcpus_consumed = round(peak_raw_process_cpu_pct / 100.0, 2)
        worker_pool_saturation = round((total_pure_cpu_time / max(total_infer_wall_time, 0.0001)) * 100, 1)

        print("-" * 85)
        print(f" HARDWARE RESOURCE CONSUMPTION & SERVER SIZING REPORT ({total_processed:,} Records)")
        print("-" * 85)
        print("RAM UTILIZATION:")
        print(f"├── Baseline Startup RAM:       {ram_baseline_mb:.2f} MB")
        print(f"├── Peak Application RAM:       {peak_ram_mb:.2f} MB")
        print(f"└── RAM Overhead Added (ΔRAM):  {ram_overhead_mb:.2f} MB (Required dynamic memory buffer)\n")
        
        print("CPU UTILIZATION:")
        print(f"├── Baseline Idle Load:          {baseline_system_cpu_pct:.1f}% (Host OS background load before job)")
        print(f"├── Peak Process Host CPU:       {host_normalized_peak_cpu:.1f}% (0–100% Host scale)")
        print(f"├── Dedicated Core Utilization:  {dedicated_core_utilization:.1f}% (Across {physical_cores} physical cores)")
        print(f"├── Multi-Core Capacity Used:    {vcpus_consumed:.2f} vCPUs / Cores")
        print(f"├── Worker Pool Saturation:     {worker_pool_saturation}% (Capacity across {settings.NUM_WORKERS} threads)")
        
        for w_id, w_data in sorted(aggregated_worker_timings.items()):
            share = (w_data['cpu_compute_time_seconds'] / max(total_pure_cpu_time, 0.0001)) * 100
            print(f"│   ├── Worker {w_id}: {w_data['records_count']:,} msgs | Wall: {w_data['inference_wall_time_seconds']:.2f}s | Pure CPU: {w_data['cpu_compute_time_seconds']:.2f}s | Load Share: {share:.1f}%")
        
        print("\nEXECUTION PIPELINE TIMINGS:")
        print(f"├── CSV Parse Time:             {total_parse_time:.4f}s")
        print(f"├── ML Inference Wall Time:     {total_infer_wall_time:.4f}s")
        print(f"├── DB Queue Publish Time:      {total_publish_time:.4f}s (Offloaded to db_worker)")
        print(f"├── Total Full Process:         {total_wall_time:.4f}s")
        print(f"├── System Overall Throughput:  {throughput:,.2f} msgs/sec")
        print(f"└── Single Record Latency:      {avg_latency_ms:.4f} ms/msg")
        print("=" * 85 + "\n")

        report_data = {
            "job_id": job_id,
            "status": "completed",
            "total_records": total_processed,
            "throughput_msgs_sec": throughput,
            "timings": {
                "total_wall_time_sec": round(total_wall_time, 4),
                "ml_inference_sec": round(total_infer_wall_time, 4),
                "db_publish_sec": round(total_publish_time, 4)
            },
            "hardware": {
                "peak_ram_mb": round(peak_ram_mb, 2),
                "peak_cpu_pct": host_normalized_peak_cpu,
                "worker_saturation_pct": worker_pool_saturation
            }
        }
        
        report_path = os.path.join(os.path.dirname(file_path), f"{job_id}_report.json")
        with open(report_path, "w") as f:
            json.dump(report_data, f, indent=4)

    except Exception as e:
        print(f"[!] Error processing batch job {job_id}: {e}")
    finally:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass


async def main():
    print("[*] Starting Batch ML Worker...")
    batch_inference_service.start_pool()
    
    # 1. Clean Connection Retry Loop
    channel = None
    for attempt in range(1, 15):
        try:
            # Let the broker service manage the connection internally
            await batch_broker_service.connect()
            # Request a channel from the safely established connection
            channel = await batch_broker_service._connection.channel()
            print("[*] Successfully connected to RabbitMQ.")
            break
        except Exception as e:
            print(f"[*] Waiting for RabbitMQ... (Attempt {attempt}/15). Retrying in 5 seconds.")
            await asyncio.sleep(5)
            
    if not channel:
        print("[!] Fatal: Could not connect to RabbitMQ. Exiting.")
        sys.exit(1)

    # 2. Queue Initialization
    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue(settings.JOB_QUEUE_NAME, durable=True)

    print(f"[*] Batch ML Worker listening on queue: '{settings.JOB_QUEUE_NAME}'")

    # 3. Message Consumption Loop
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                payload = json.loads(message.body.decode())
                await process_batch_job(
                    job_id=payload["job_id"],
                    file_path=payload["file_path"]
                )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        batch_inference_service.shutdown_pool()