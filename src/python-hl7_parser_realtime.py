import asyncio
import argparse

from core.utils.status_updates import startup
from core.rabbitmq import AsyncRMQConnection
from core.parser_pipeline import ParsingOrchestrator

class RealtimeOrchestrator(ParsingOrchestrator):
    """Orchestrates the pipeline for real-time processing from RabbitMQ HL7."""
    
    def __init__(self, max_parsers: int = 1):
        super().__init__(
            service_type="realtime",
            max_parsers=max_parsers
        )
        self.rmq_hl7: AsyncRMQConnection | None = None

    async def _input_task(self):
        async with AsyncRMQConnection(required_queues=["hl7"], blocking=True) as rmq:
            self.rmq_hl7 = rmq

            async def on_message(message):
                body = message.body
                headers = message.headers or {}
                async with message.process():
                    await self.parse_queue.put({"body": body, "headers": headers})

            await self.rmq_hl7.consume("hl7", on_message, prefetch=self.max_parsers * 15)
            await self.shutdown_event.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-parsers", type=int, default=1)
    args = ap.parse_args()
    
    orch = RealtimeOrchestrator(max_parsers=args.max_parsers)

    def get_status():
        active = len([t for t in orch.worker_tasks if not t.done()])

        rmq_state = "init"
        if orch.rmq_hl7 is not None:
            if getattr(orch.rmq_hl7, "_waiting_for_rmq", False):
                rmq_state = "waiting"
            else:
                rmq_state = "active" if orch.rmq_hl7.is_connected else "down"

        return {
            "Workers": f"{active}/{orch.max_parsers}",
            "Queue": f"{orch.parse_queue.qsize()}",
            "RMQ": rmq_state,
        }

    startup("HL7 Realtime Parser", vars(args), get_status=get_status)
    
    try:
        asyncio.run(orch.start())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()