import asyncio
import aiofiles
import argparse
import time
import functools
from pathlib import Path
from typing import Tuple
from datetime import datetime
from colorama import Fore, Style

from core.utils.status_updates import startup, safe_print
from core.utils.metrics import MetricsManager
from core.rabbitmq import AsyncRMQConnection, RMQConnectionError, RMQPublishError

_hl7_files_received = 0
_publish_success = None

def get_status():
    global _hl7_files_received, _publish_success
    count = _hl7_files_received
    _hl7_files_received = 0

    publishing_dict = {None: "init", True: "active", False: "down"}
    return {
        "hl7 files received": f"{count}",
        "RMQ publishing": publishing_dict.get(_publish_success, "unknown"),
    }

# MLLP Framing bytes
SB, FS, CR = b'\x0b', b'\x1c', b'\x0d'
EB = FS + CR

# Instantiate the MetricsManager for the service
metrics_manager = MetricsManager(
    service_name="hl7.receiver"
)

def get_message_details(raw_hl7_payload: bytes) -> Tuple[bytes, dict]:
    """
    Parse required fields from an HL7 message and return the control id as bytes and a dictionary of the required details: 
    - message_id: safe string conversion of control id bytes
    - segment_timestamp: ISO 8601 string
    - relative_filepath: YYYYMMDD/HH/{timestamp}_{control_id}.hl7
    This function is strict, we raise a ValueError if mandatory data is missing or malformed.
    """

    # Iterate through the payload without creating a full list of lines.
    msh_line = None
    obr_line = None
    start = 0
    while start < len(raw_hl7_payload):
        # Find the next carriage return, or the end of the payload
        end = raw_hl7_payload.find(CR, start)
        if end == -1:
            end = len(raw_hl7_payload)
        line = raw_hl7_payload[start:end]
        if line.startswith(b"MSH"):
            msh_line = line
        elif line.startswith(b"OBR"):
            obr_line = line
        
        # Exit when we find both required lines
        if msh_line and obr_line:
            break
        start = end + 1

    if not msh_line:
        raise ValueError("Missing MSH segment")
    msh_fields = msh_line.split(b'|')
    if len(msh_fields) <= 9:
        raise ValueError("MSH missing control ID field 10")
    control_id = msh_fields[9].strip()
    if not control_id:
        raise ValueError("Empty control ID")

    if not obr_line:
        raise ValueError("Missing OBR segment")
    obr_fields = obr_line.split(b'|')
    if len(obr_fields) <= 7 or not obr_fields[7]:
        raise ValueError("OBR missing timestamp field 8")
    
    try:
        raw_timestamp = obr_fields[7].split(b'.')[0].decode('ascii')
    except (UnicodeDecodeError, ValueError):
        raise ValueError("Invalid or non-ASCII timestamp")

    if len(raw_timestamp) < 10:
        raise ValueError("Timestamp too short")
    date_part = raw_timestamp[:8]
    hour_part = raw_timestamp[8:10]
    if not (date_part.isdigit() and hour_part.isdigit()):
        raise ValueError("Timestamp date/hour not numeric")

    safe_id = control_id.decode('utf-8', errors='strict').replace('/', '_')
    filename = f"{raw_timestamp}_{safe_id}.hl7"
    relative_filepath = f"{date_part}/{hour_part}/{filename}"
    message_details = {
        "message_id": safe_id,
        "relative_filepath": relative_filepath
    }
    return control_id, message_details   

def create_mllp_ack(control_id: bytes, current_time: str) -> bytes:
    """Creates a basic MLLP-framed ACK."""
    msh = b"MSH|^~\\&|RECV_APP|RECV_FAC|SEND_APP_ACK|SEND_FAC_ACK|" + \
          current_time.encode('utf-8') + \
          b"||ACK^A01^ACK|ACK" + control_id + b"|P|2.3" + CR
    msa = b"MSA|AA|" + control_id + CR
    return SB + msh + msa + EB

async def write_hl7_to_file(
    raw_hl7_payload: bytes,
    relative_filepath: str,
    success: bool,
    empty_write_out: bool
) -> bool:
    """Write HL7 payload to disk under archive/ or failed/ using the provided relative filepath."""
    try:
        status_dir = "archive" if success else "failed"
        full_path = Path('/app/hl7') / status_dir / relative_filepath
        full_path.parent.mkdir(parents=True, exist_ok=True)
        content_to_write = b'' if empty_write_out else raw_hl7_payload
        async with aiofiles.open(full_path, mode='wb') as f:
            await f.write(content_to_write)
        return True
    except Exception as e:
        safe_print(f"[Error] File write failed: {e}", Fore.RED)
        return False

async def process_message(
    message: bytes,
    rmq_receiver: AsyncRMQConnection,
    empty_write_out: bool
) -> bytes:
    """
    Processes a single HL7 message: validates, publishes, writes, and returns an ACK.
    Metrics are recorded for each step.
    """
    global _hl7_files_received, _publish_success
    _hl7_files_received += 1

    # store timestamp for when message is received to add to the headers
    received_timestamp = datetime.now().isoformat()    
    counters = {"hl7_receiver_messages_received_total": 1}
    histograms = {}
    message_start_ns = time.time_ns()

    try:
        control_id, message_details = get_message_details(message)
        message_details["received_timestamp"] = received_timestamp
    except ValueError as e:
        safe_print(f"[Reject] Invalid HL7 message: {e}", Fore.YELLOW)
        counters["hl7_receiver_messages_rejected_total"] = 1
        metrics_manager.record({"counters": counters})
        # A valid ACK cannot be created without a control_id, so we return nothing.
        return b''

    # 1. Publish to RabbitMQ
    publish_success = False
    rmq_start_ns = time.time_ns()
    try:
        await rmq_receiver.publish(
            routing_key="hl7",
            message_id=message_details["message_id"],
            headers=message_details,
            body=message,
            persistent=True,
        )
        publish_success = True
        counters["hl7_receiver_messages_published_total"] = 1
        histograms["hl7_receiver_rmq_publish_duration_ms"] = (time.time_ns() - rmq_start_ns) / 1e6
    except (RMQConnectionError, RMQPublishError) as e:
        publish_success = False
        counters["hl7_receiver_messages_publish_failed_total"] = (1, {"reason": e.__class__.__name__})
    _publish_success = publish_success

    # 2. Write to file
    write_start_ns = time.time_ns()
    write_success = await write_hl7_to_file(
        message, message_details["relative_filepath"], publish_success, empty_write_out
    )
    if write_success:
        histograms["hl7_receiver_file_write_duration_ms"] = (time.time_ns() - write_start_ns) / 1e6
        histograms["hl7_receiver_message_size"] = len(message)
        status_attr = {"status": "archived" if publish_success else "failed"}
        counters["hl7_receiver_messages_written_total"] = (1, status_attr)

    # 3. Record metrics for this message
    message_duration_ms = (time.time_ns() - message_start_ns) / 1_000_000
    histograms["hl7_receiver_message_processing_duration_ms"] = message_duration_ms
    current_time_str = time.strftime("%Y%m%d%H%M%S")
    metrics_manager.record({"counters": counters, "histograms": histograms})

    # 4. Create ACK
    return create_mllp_ack(control_id, current_time_str)

async def handle_mllp_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    rmq_receiver: AsyncRMQConnection,
    max_buffer_size: int,
    empty_write_out: bool
):
    """Handles a single MLLP client connection."""
    peername = str(writer.get_extra_info('peername'))
    safe_print(f"[Connected] {peername}", Fore.GREEN)
    metrics_manager.record({"counters": {"hl7_receiver_connections_total": 1}})

    buffer = bytearray()
    try:
        while True:
            chunk = await asyncio.wait_for(reader.read(16384), timeout=100.0)
            if not chunk:
                break # Client has closed the connection
            buffer.extend(chunk)

            # Monitor the current buffer size immediately after a read.
            metrics_manager.record({
                "gauges": {"hl7_receiver_socket_buffer_bytes": (len(buffer), {"peer": peername})}
            })

            # Process all complete MLLP frames in the buffer
            while True:
                try:
                    start_idx = buffer.index(SB)
                    end_idx = buffer.index(EB, start_idx + 1)
                except ValueError:
                    break # No complete frame in buffer

                message = buffer[start_idx + 1:end_idx]
                del buffer[:end_idx + len(EB)]
                ack = await process_message(message, rmq_receiver, empty_write_out)
                if ack:
                    writer.write(ack)
                    await writer.drain()

            if len(buffer) > max_buffer_size:
                safe_print(f"[Error] Buffer overflow for {peername}", Fore.RED)
                metrics_manager.record({"counters": {"hl7_receiver_connections_failed_total": (1, {"reason": "buffer_overflow"})}})
                break

    except asyncio.TimeoutError:
        safe_print(f"[Timeout] {peername} - no data for 100s", Fore.YELLOW)
        metrics_manager.record({"counters": {"hl7_receiver_connections_failed_total": (1, {"reason": "timeout"})}})
    except (ConnectionResetError, BrokenPipeError):
        safe_print(f"[Disconnected] {peername} - connection lost", Fore.YELLOW)
        metrics_manager.record({"counters": {"hl7_receiver_connections_failed_total": (1, {"reason": "reset"})}})
    except Exception as e:
        safe_print(f"[Error] {peername}: {e}", Fore.RED)
        metrics_manager.record({"counters": {"hl7_receiver_connections_failed_total": (1, {"reason": "exception"})}})
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        safe_print(f"[Closed] {peername}", Fore.GREEN)

async def main_hl7_server(port: int, max_buffer_size: int, empty_write_out: bool):
    async with AsyncRMQConnection(required_queues=['hl7']) as rmq_receiver:
        client_handler = functools.partial(
            handle_mllp_client,
            rmq_receiver=rmq_receiver,
            max_buffer_size=max_buffer_size,
            empty_write_out=empty_write_out
        )
        server = await asyncio.start_server(client_handler, '0.0.0.0', port)
        addr = server.sockets[0].getsockname()
        write_mode = "(empty files)" if empty_write_out else "(full content)"
        safe_print(f"[Init] MLLP Receiver listening on {addr[0]}:{addr[1]} {write_mode}", Fore.GREEN)
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HL7 MLLP Receiver")
    parser.add_argument("--port", type=int, help="Port to listen on")
    parser.add_argument("--max-buffer-size", type=int, default=10 * 1024 * 1024, help="Max buffer size for reading messages")
    parser.add_argument("--empty-write-out", action="store_true", help="Write empty files instead of full content (debug)")
    
    args = parser.parse_args()

    startup("HL7 Receiver", vars(args), get_status=get_status)

    try:
        asyncio.run(main_hl7_server(args.port, args.max_buffer_size, args.empty_write_out))
    except KeyboardInterrupt:
        safe_print("[Shutdown] User interrupt", Fore.YELLOW)
    except Exception as e:
        safe_print(f"[Critical] Server failed: {e}", Fore.RED)
    finally:
        safe_print("[Shutdown] Server stopped", Fore.CYAN)
        metrics_manager.shutdown()