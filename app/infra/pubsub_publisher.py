# ed_upload_manager/infra/pubsub_publisher.py
import json
from typing import Dict, Any, Optional
from google.cloud.pubsub_v1 import PublisherClient

PROJECT_ID = "your-gcp-project"  # set via env in real code

_publisher: Optional[PublisherClient] = None


def _get_publisher() -> PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = PublisherClient()
    return _publisher


def enqueue_job(topic: str, payload: Dict[str, Any]) -> None:
    publisher = _get_publisher()
    topic_path = publisher.topic_path(PROJECT_ID, topic)
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data)
    # Optionally block on publish in dev/test; in prod you can skip result() for throughput
    future.result(timeout=10)
