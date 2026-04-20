import argparse
import asyncio
from aiormq.exceptions import AMQPConnectionError

from core.utils.status_updates import startup, safe_print, Fore
from core.rabbitmq import AsyncRMQConnection, RMQConnectionError, RMQPublishError
from core.parser_pipeline import ParsingOrchestrator


class ErrorQueueOrchestrator(ParsingOrchestrator):
    """Orchestrates the pipeline for reprocessing messages from an HL7 error queue."""

    def __init__(
        self,
        error_queue: str,
        max_parsers: int = 4,
        enable_backpressure: bool = True,
    ):
        super().__init__(
            service_type=f"errors.{error_queue}",
            max_parsers=max_parsers,
            enable_backpressure=enable_backpressure,
        )
        self.error_queue = error_queue
        self.rmq_error_queue: AsyncRMQConnection | None = None

    async def _input_task(self):
        try:
            async with AsyncRMQConnection(required_queues=[self.error_queue], blocking=False) as rmq:
                self.rmq_error_queue = rmq
                await self.rmq_error_queue.channel.set_qos(prefetch_count=self.max_parsers * 15)

                target_count = await self.rmq_error_queue.get_queue_size(self.error_queue)

                safe_print(
                    f"Starting finite error reprocess for '{self.error_queue}': "
                    f"target_count={target_count}{Fore.RESET}",
                    Fore.CYAN
                )

                if target_count <= 0:
                    safe_print("No messages to process. Exiting.", Fore.CYAN)
                    return

                processed = 0

                async def on_message(message):
                    nonlocal processed
                    if processed >= target_count or self.shutdown_event.is_set():
                        return
                    async with message.process(requeue=True):
                        await self.parse_queue.put(
                            {
                                "body": message.body,
                                "routing_key": message.routing_key,
                                "headers": dict(message.headers or {}),
                            }
                        )
                    processed += 1
                    if processed >= target_count:
                        self.shutdown_event.set()

                await self.rmq_error_queue.consume(self.error_queue, on_message, prefetch=self.max_parsers * 15)
                await self.shutdown_event.wait()

                safe_print(
                    f"{Fore.CYAN}Finished finite error reprocess for '{self.error_queue}': "
                    f"processed={processed}/{target_count}{Fore.RESET}"
                )

                initial_remaining = await self.rmq_error_queue.get_queue_size(self.error_queue)
                await asyncio.sleep(5)
                delayed_remaining = await self.rmq_error_queue.get_queue_size(self.error_queue)

                if delayed_remaining > 0:
                    safe_print(
                        f"{Fore.YELLOW}WARNING: {delayed_remaining} messages remain in '{self.error_queue}'.{Fore.RESET}"
                    )
                    if delayed_remaining > initial_remaining:
                        safe_print(
                            f"{Fore.YELLOW}WARNING: Queue size increased; other components may be producing new errors.{Fore.RESET}"
                        )

        except (AMQPConnectionError, ConnectionResetError, RMQConnectionError, RMQPublishError) as e:
            safe_print(f"{Fore.RED}[Error] RabbitMQ became unavailable: {e}{Fore.RESET}")
            self.shutdown_event.set()
            raise



async def main():
    ap = argparse.ArgumentParser(description="Reprocess HL7 error queue messages (finite snapshot run).")
    ap.add_argument("--error-queue", type=str, required=True)
    ap.add_argument("--max-parsers", type=int, default=4)
    ap.add_argument("--disable-backpressure", action="store_true")
    args = ap.parse_args()

    orch = ErrorQueueOrchestrator(
        error_queue=args.error_queue,
        max_parsers=args.max_parsers,
        enable_backpressure=not args.disable_backpressure,
    )

    def get_status():
        active = len([t for t in orch.worker_tasks if not t.done()])
        return {
            "Workers": f"{active}/{orch.max_parsers}",
            "Queue": f"{orch.parse_queue.qsize()}"
        }

    startup("HL7 Error Queue Parser", vars(args), get_status=get_status)
    await orch.start()


if __name__ == "__main__":
    asyncio.run(main())
