import asyncio
import json
import time
import os
import atexit
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import Dict, Any, Set
from datetime import datetime

from .mindray_hl7_parser import (
    HL7ProcessingError, SignalDataExtractor, FormatError, MissingFieldError,
    InvalidTimestampError, SignalDataError
)

from .rabbitmq import SyncRMQConnection
from .utils.metrics import MetricsManager
from .utils.status_updates import safe_print, Fore
from .utils.utils import load_registry

# --- Custom Exceptions with Context ---

"""Raised when an unknown signal is encountered in the HL7 message."""
class UnknownSignal(HL7ProcessingError): pass

"""Raised when the ward/bed combination is unknown."""
class UnknownBedspace(HL7ProcessingError): pass

# Worker globals
parser_worker: SignalDataExtractor = None
rmq_parser: SyncRMQConnection = None
metrics_manager: MetricsManager = None

# Registry globals
bedspace_mapping: Dict = {}
ward_mapping: Dict = {}
signal_name_keeps: Set = set()
known_signals: Set = set()

def initialise_parser_worker(service_type: str):
    """Initialise the parser worker, including its own copy of the registry."""
    global parser_worker, rmq_parser, metrics_manager, service_name
    global bedspace_mapping, ward_mapping, signal_name_keeps, known_signals

    service_name = f"hl7.parser.{service_type}"

    # Each worker process now loads the registry and creates a simple parser
    parser_worker = SignalDataExtractor()
    reg = load_registry()
    bedspace_mapping = reg["bedspace_mapping"]
    ward_mapping = reg["ward_mapping"]
    signal_name_keeps = set(reg["signal_name_keeps"])
    known_signals = signal_name_keeps | set(reg["signal_name_drops"])

    rmq_parser = SyncRMQConnection(blocking=True, required_queues=['cmf_atriumdb'])
    rmq_parser.start_connection()
    metrics_manager = MetricsManager(service_name=service_name)
    atexit.register(rmq_parser.close)
    atexit.register(metrics_manager.shutdown)

def make_cmf(devid: int, systime: int, mname: str, uom: str, freq: int, val: str, resolution: float) -> str:
    """Create CMF JSON."""
    return json.dumps({
        "srcid": 0, "devid": devid, "systime": systime, "type": "wav",
        "mname": mname, "mtime": systime, "uom": uom, "freq": freq,
        "val": val, "srcmeta": {"scale_m": resolution, "scale_b": 0}
    })

def parse_and_publish_in_process(msg: Dict[str, Any], enable_backpressure: bool) -> None:
    """Parse, validate, and publish messages."""
    global parser_worker, rmq_parser, metrics_manager, service_name
    global bedspace_mapping, ward_mapping, signal_name_keeps, known_signals

    hl7_cmfs_published = 0
    backpressure_applied = 0
    data = None
    message_error = None
    unknown_signals_found: list[str] = []

    # Check cmf_atriumdb queue size, if >100 apply scaling backpressure
    if enable_backpressure:
        queue_size = rmq_parser.get_queue_size('cmf_atriumdb')
        if queue_size > 100:
            time.sleep(queue_size / 100)  # Sleep 1s per 100 messages in queue
            backpressure_applied = 1

    body = msg.get("body")
    headers = msg.get("headers", {})
    headers['parser_service'] = service_name
    headers['parser_entry_timestamp'] = datetime.now().isoformat()

    try:
        # 1. Parse HL7
        data = parser_worker.extract_data(body)

        # 2. Validate Bedspace
        ward_code = ward_mapping.get(data.ward)
        bed_code = bedspace_mapping.get(data.ward, {}).get(data.bed) if ward_code else None

        if not ward_code or not bed_code:
            raise UnknownBedspace()

        # 3. Validate Signals
        devid = int(ward_code + bed_code)
        cmf_list: list[str] = []

        for sig in data.signals:
            if sig.name not in known_signals:
                unknown_signals_found.append(sig.name)
                continue  # Skip processing this signal but check others

            if sig.name in signal_name_keeps:
                cmf_body = make_cmf(
                    devid, data.timestamp_ns, sig.name, sig.units,
                    sig.frequency, sig.values, sig.resolution
                )
                cmf_list.append(cmf_body)

        # 4. Publish valid CMFs
        cmf_total = len(cmf_list)
        for i, cmf in enumerate(cmf_list):
            if i == cmf_total - 1:
                headers['parser_exit_timestamp'] = datetime.now().isoformat()
            rmq_parser.publish(exchange='cmf_exchange', body=cmf, headers=headers, persistent=True)
            hl7_cmfs_published += 1

        # 5. Raise error for unknown signals after publishing valid ones
        if unknown_signals_found:
            raise UnknownSignal()

    # 6. Handle Errors
    except (FormatError, MissingFieldError, InvalidTimestampError, SignalDataError, UnknownSignal, UnknownBedspace) as e:
        routing_key = e.error_queue
        rmq_parser.publish(routing_key=routing_key, body=body, headers=headers, persistent=True)
        message_error = True

    except Exception:
        routing_key = 'hl7_unhandled_error'
        rmq_parser.publish(routing_key=routing_key, body=body, headers=headers, persistent=True)
        message_error = True
        safe_print(f"[DEBUG] Unhandled error message body: {body}; headers: {headers}", Fore.MAGENTA)

    # Metrics recording
    counters = {
        "hl7_messages_processed_total": (1, {}),
        "hl7_cmf_messages_published_total": (hl7_cmfs_published, {}),
        "hl7_parser_backpressure_applied_total": (backpressure_applied, {}),
        "hl7_parser_backpressure_enabled": (1 if enable_backpressure else 0, {}),
    }

    if message_error:
        bedspace = f"{data.ward}/{data.bed}" if data else "Unknown"
        counters["hl7_message_errors_total"] = [
            (1, {"error_type": routing_key, "bedspace": bedspace, "signal": signal})
            for signal in (unknown_signals_found or ["N/A"])
        ]

    metrics_manager.record({"counters": counters})

class ParsingOrchestrator:
    """
    Orchestrator that manages the process pool and input pipeline.
    Subclasses implement _input_task().
    If task is finite (backlog/errors), input_task returns when done.
    If task is infinite (realtime), input_task runs indefinitely until stop() is called.
    """

    def __init__(
        self,
        service_type: str,
        max_parsers: int = 1,
        enable_backpressure: bool = False
    ):
        self.service_type = service_type
        self.max_parsers = max_parsers
        self.enable_backpressure = enable_backpressure
        self.shutdown_event = asyncio.Event()
        self.parse_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=max_parsers * 100)
        self.worker_tasks: list[asyncio.Task] = []

    async def _input_task(self):
        """
        Abstract method for getting input.
        Must put items into self.parse_queue.
        """
        raise NotImplementedError

    async def _run_worker(self, pool: ProcessPoolExecutor):
        """Run a worker to process messages."""
        loop = asyncio.get_running_loop()
        while not self.shutdown_event.is_set():
            try:
                msg = await self.parse_queue.get()
            except asyncio.CancelledError:
                return

            try:
                await loop.run_in_executor(
                    pool, parse_and_publish_in_process, msg, self.enable_backpressure
                )
            finally:
                self.parse_queue.task_done()

    async def start(self):
        initialiser = partial(
            initialise_parser_worker,
            service_type=self.service_type
        )

        try:
            with ProcessPoolExecutor(
                max_workers=self.max_parsers,
                initializer=initialiser
            ) as pool:
                self.worker_tasks = [
                    asyncio.create_task(self._run_worker(pool))
                    for _ in range(self.max_parsers)
                ]

                # Run producer until it finishes (backlog/errors) or until shutdown (realtime)
                await self._input_task()

                # Drain whatever was already queued
                await self.parse_queue.join()
                safe_print(f"[INFO] Completed parser run. Service Type: {self.service_type}", Fore.CYAN)

        finally:
            await self.stop()

    async def stop(self):
        self.shutdown_event.set()
        for task in self.worker_tasks:
            task.cancel()
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)