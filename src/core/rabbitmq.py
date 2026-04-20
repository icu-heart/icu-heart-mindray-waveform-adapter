"""
RabbitMQ utilities with robust connection management for async and sync
"""
import time
import os
import logging
import logging.config
from typing import Optional, Dict, Any
from datetime import datetime
import pika
import asyncio
import aio_pika

# Stop aio-pika attempting asyncio cleanup in __del__ (can be called in Otel thread -> no event loop -> spam)
aio_pika.connection.Connection.__del__ = lambda self: None  # type: ignore[attr-defined]

from .utils.status_updates import safe_print, Fore

# Suppress aio_pika/aiormq output (keep the rest of the logging)
logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "loggers": {
        "aio_pika": {"level": "CRITICAL", "handlers": [], "propagate": False},
        "aiormq": {"level": "CRITICAL", "handlers": [], "propagate": False},
    },
})

# RabbitMQ connection params
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD")
RABBITMQ_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@rabbitmq:5672/"


class RMQConnectionError(RuntimeError): pass
class RMQPublishError(RuntimeError): pass


class BaseRMQConnection():
    """RabbitMQ connection base class. For managing connection states."""
    def __init__(
        self,
        required_queues: Optional[list[str]] = None,
        blocking: bool = False,
        retry_wait_period: float = 1.0,
    ):
        """
        Initialize RabbitMQ connection parameters.

        Args:
            required_queues: Optional list of queue names that must exist (connection fails if not found)
            blocking: If True, wait indefinitely for RabbitMQ to become available (1 attempt per retry_wait_period)
            retry_wait_period: Minimum seconds between reconnection attempts
        """
        self.required_queues = required_queues or []
        self.blocking = blocking
        self.retry_wait_period = retry_wait_period
        self._connection = None
        self._channel = None
        self._last_connection_attempt: Optional[datetime] = None
        self._waiting_for_rmq = False
        self._warned_down = False
        self._ever_connected = False

    def _can_attempt_reconnect(self) -> bool:
        """Check if enough time has passed since last connection attempt. Prevents too many attempts."""
        if self._last_connection_attempt is None:
            return True
        elapsed = datetime.now() - self._last_connection_attempt
        return elapsed.total_seconds() >= self.retry_wait_period

    def _update_attempt_time(self):
        """Update the last connection attempt timestamp."""
        self._last_connection_attempt = datetime.now()

    @property
    def connection(self):
        if self._connection is None:
            raise RMQConnectionError("RabbitMQ connection is not available (not connected)")
        return self._connection

    @property
    def channel(self):
        if self._channel is None:
            raise RMQConnectionError("RabbitMQ channel is not available (not connected)")
        return self._channel

    @property
    def is_connected(self) -> bool:
        c, ch = self._connection, self._channel
        # aio-pika and pika have different connection/channel state attributes, requiring this nonsense...
        return bool(c and ch and not getattr(c, "is_closed", True) and not getattr(ch, "is_closed", False) and getattr(ch, "is_open", True))


class AsyncRMQConnection(BaseRMQConnection):
    """Async RabbitMQ connection using aio-pika."""

    async def _connect(self) -> bool:
        try:
            self._update_attempt_time()
            self._connection = await aio_pika.connect(RABBITMQ_URL)

            self._channel = await self._connection.channel()
            for queue_name in self.required_queues:
                await self._channel.declare_queue(queue_name, passive=True)
            self._ever_connected = True
            return True
        except Exception:
            await self._drop_connection()
            return False

    async def _drop_connection(self) -> None:
        c, ch = self._connection, self._channel; self._connection = self._channel = None
        try: ch and await ch.close()
        except Exception: pass
        try: c and await c.close()
        except Exception: pass

    async def _ensure_connection(self, blocking: Optional[bool] = None) -> None:
        if self.is_connected:
            return

        blocking = self.blocking if blocking is None else blocking
        label = f"[{','.join(self.required_queues)}]" if self.required_queues else ""

        if blocking:
            if self._ever_connected and not self._waiting_for_rmq:
                safe_print(f"[RMQ]{label} Connection lost. Waiting for RabbitMQ to return...", Fore.YELLOW)
                self._waiting_for_rmq = True
        else:
            if self._ever_connected and not self._warned_down:
                safe_print(f"[RMQ]{label} Connection lost.", Fore.YELLOW)
                self._warned_down = True

        while True:
            if blocking or self._can_attempt_reconnect():
                if await self._connect():
                    if self._ever_connected and (self._waiting_for_rmq or self._warned_down):
                        safe_print(f"[RMQ]{label} RabbitMQ connection re-established.", Fore.GREEN)
                        self._waiting_for_rmq, self._warned_down = False, False
                    return

            if not blocking:
                raise RMQConnectionError("Unable to connect to RabbitMQ")

            await asyncio.sleep(self.retry_wait_period)

    async def start_connection(self) -> bool:
        await self._ensure_connection(blocking=False)
        return True

    async def consume(self, queue_name: str, on_message, *, prefetch: int = 0) -> None:
        """Consume messages with automatic reconnect/resubscribe respecting self.blocking."""
        while True:
            await self._ensure_connection()
            try:
                if prefetch:
                    await self.channel.set_qos(prefetch_count=prefetch)
                queue = await self.channel.get_queue(queue_name)
                await queue.consume(on_message)

                while True:
                    await self.channel.declare_queue(queue_name, passive=True)
                    await asyncio.sleep(2)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._drop_connection()
                if not self.blocking:
                    raise RMQConnectionError(f"Consume failed for '{queue_name}': {e}") from e

    async def publish(
        self,
        body: Optional[bytes | str] = None,
        exchange: str = '',
        routing_key: str = '',
        message_id: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        persistent: bool = True,
    ) -> bool:
        while True:
            await self._ensure_connection()
            message_body = body.encode() if isinstance(body, str) else (body or b'')
            try:
                msg = aio_pika.Message(
                    body=message_body,
                    message_id=message_id,
                    headers=headers or {},
                    delivery_mode=(aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT),
                )
                if not exchange:
                    await self.channel.default_exchange.publish(msg, routing_key=routing_key)
                else:
                    exch = await self.channel.declare_exchange(exchange, durable=True)
                    await exch.publish(msg, routing_key=routing_key)
                return True
            except Exception as e:
                await self._drop_connection()
                if not self.blocking:
                    raise RMQPublishError(f"Failed to publish message: {e}") from e

    async def get_queue_size(self, queue_name: str) -> int:
        while True:
            await self._ensure_connection()
            try:
                queue = await self.channel.declare_queue(queue_name, passive=True)
                return int(queue.declaration_result.message_count)
            except Exception as e:
                await self._drop_connection()
                if not self.blocking:
                    raise RMQConnectionError(f"Failed to get queue size for '{queue_name}': {e}") from e

    async def close(self) -> None:
        await self._drop_connection()

    async def __aenter__(self) -> "AsyncRMQConnection":
        await self.start_connection()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class SyncRMQConnection(BaseRMQConnection):
    """Sync RabbitMQ connection for worker processes."""

    def _connect(self) -> bool:
        try:
            self._update_attempt_time()
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            for queue_name in self.required_queues:
                self._channel.queue_declare(queue=queue_name, passive=True)
            self._ever_connected = True
            return True
        except Exception:
            self._drop_connection()
            return False

    def _drop_connection(self) -> None:
        c = self._connection
        self._connection = None
        self._channel = None
        try:
            c and c.close()
        except Exception:
            pass

    def _ensure_connection(self, blocking: Optional[bool] = None) -> None:
        if self.is_connected:
            return

        blocking = self.blocking if blocking is None else blocking

        label = f"[{','.join(self.required_queues)}]" if self.required_queues else ""

        if blocking:
            if self._ever_connected and not self._waiting_for_rmq:
                safe_print(f"[RMQ]{label} Connection lost. Waiting for RabbitMQ to return...", Fore.YELLOW)
                self._waiting_for_rmq = True
        else:
            if self._ever_connected and not self._warned_down:
                safe_print(f"[RMQ]{label} Connection lost.", Fore.YELLOW)
                self._warned_down = True

        while True:
            if blocking or self._can_attempt_reconnect():
                if self._connect():
                    if self._ever_connected and (self._waiting_for_rmq or self._warned_down):
                        safe_print(f"[RMQ]{label} RabbitMQ connection re-established.", Fore.GREEN)
                        self._waiting_for_rmq, self._warned_down = False, False
                    return

            if not blocking:
                raise RMQConnectionError("Unable to connect to RabbitMQ")

            time.sleep(self.retry_wait_period)

    def start_connection(self) -> bool:
        self._ensure_connection(blocking=False)
        return True

    def publish(
        self,
        body: Optional[bytes | str] = None,
        exchange: str = '',
        routing_key: str = '',
        message_id: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        persistent: bool = True,
    ) -> bool:
        while True:
            self._ensure_connection()
            message_body = body.encode() if isinstance(body, str) else (body or b'')
            try:
                props = pika.BasicProperties(
                    delivery_mode=2 if persistent else 1,
                    message_id=message_id,
                    headers=headers or {},
                )
                self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message_body, properties=props)
                return True
            except Exception as e:
                self._drop_connection()
                if not self.blocking:
                    raise RMQPublishError(f"Failed to publish message: {e}") from e

    def get_queue_size(self, queue_name: str) -> int:
        while True:
            self._ensure_connection()
            try:
                result = self._channel.queue_declare(queue=queue_name, passive=True)
                return int(result.method.message_count)
            except Exception as e:
                self._drop_connection()
                if not self.blocking:
                    raise RMQConnectionError(f"Failed to get queue size for '{queue_name}': {e}") from e

    def consume(self, queue_name: str, on_message, *, prefetch: int = 0, auto_ack: bool = False) -> None:
        """Consume with automatic reconnect/resubscribe respecting self.blocking."""
        while True:
            self._ensure_connection()
            try:
                if prefetch:
                    self.channel.basic_qos(prefetch_count=prefetch)
                self.channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=auto_ack)
                self.channel.start_consuming()
                if not self.blocking:
                    return
            except Exception as e:
                self._drop_connection()
                if not self.blocking:
                    raise RMQConnectionError(f"Consume failed for '{queue_name}': {e}") from e

    def close(self):
        self._drop_connection()