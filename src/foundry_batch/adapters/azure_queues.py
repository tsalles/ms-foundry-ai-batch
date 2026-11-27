"""Azure Storage Queue adapters. Messages are plain text (host.json ``messageEncoding: none``)."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from azure.core.exceptions import AzureError
from azure.storage.queue import QueueClient, QueueServiceClient

from foundry_batch.errors import QueueUnavailableError
from foundry_batch.ports import ReceivedMessage

#: Retention promised by the runbooks for queued work (14 days). Without an explicit
#: ``time_to_live`` the service keeps a message for 7 days.
MESSAGE_TTL_SECONDS = 14 * 24 * 3600
#: The receiver must answer the provider within seconds, so its client gets one quick retry and
#: short socket timeouts instead of the SDK default (3 retries, about 57 s of back-off). These
#: keywords reach the SDK's ``ExponentialRetry`` and transport.
RECEIVER_QUEUE_CLIENT_OPTIONS: Mapping[str, int] = {
    "retry_total": 1,
    "initial_backoff": 1,
    "increment_base": 1,
    "random_jitter_range": 1,
    "connection_timeout": 2,
    "read_timeout": 3,
}


def receiver_queue_client(
    account_url: str,
    queue_name: str,
    credential: object,
    *,
    factory: Callable[..., QueueClient] = QueueClient,
) -> QueueClient:
    """Queue client for the webhook receiver's enqueue-then-acknowledge path."""
    return factory(account_url, queue_name, credential=credential, **RECEIVER_QUEUE_CLIENT_OPTIONS)


class AzureMessageQueue:
    def __init__(self, client: QueueClient) -> None:
        self._client = client

    def send(self, message: str, *, visibility_timeout: int | None = None) -> None:
        try:
            self._client.send_message(
                message, visibility_timeout=visibility_timeout, time_to_live=MESSAGE_TTL_SECONDS
            )
        except AzureError as exc:
            # Includes timeouts after the request was sent: the message may exist.
            raise QueueUnavailableError(type(exc).__name__) from exc


class AzureQueueAdmin:
    def __init__(self, service: QueueServiceClient) -> None:
        self._service = service

    def approximate_depth(self, queue: str) -> int:
        properties = self._service.get_queue_client(queue).get_queue_properties()
        return int(properties.approximate_message_count or 0)

    def receive(
        self, queue: str, *, max_messages: int, visibility_timeout: int
    ) -> list[ReceivedMessage]:
        client = self._service.get_queue_client(queue)
        received: list[ReceivedMessage] = []
        for message in client.receive_messages(
            messages_per_page=min(max_messages, 32), visibility_timeout=visibility_timeout
        ):
            received.append(
                ReceivedMessage(
                    message_id=message.id,
                    pop_receipt=message.pop_receipt or "",
                    content=str(message.content),
                    dequeue_count=int(message.dequeue_count or 0),
                )
            )
            if len(received) >= max_messages:
                break
        return received

    def peek(self, queue: str, *, max_messages: int) -> list[ReceivedMessage]:
        client = self._service.get_queue_client(queue)
        return [
            ReceivedMessage(
                message_id=message.id,
                pop_receipt="",
                content=str(message.content),
                dequeue_count=int(message.dequeue_count or 0),
            )
            for message in client.peek_messages(max_messages=min(max_messages, 32))
        ]

    def delete(self, queue: str, message: ReceivedMessage) -> None:
        self._service.get_queue_client(queue).delete_message(
            message.message_id, message.pop_receipt
        )

    def send_to(self, queue: str, content: str) -> None:
        AzureMessageQueue(self._service.get_queue_client(queue)).send(content)
