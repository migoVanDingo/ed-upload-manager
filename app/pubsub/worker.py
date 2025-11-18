# e.g. ed_upload_manager/worker.py
import asyncio

from platform_common.pubsub.factory import get_subscriber
from platform_common.pubsub.event import PubSubEvent


async def handle_gcs_file_finalized(event: PubSubEvent):
    # event.payload has whatever you set above
    bucket = event.payload["bucket"]
    name = event.payload["name"]
    # e.g. create UploadSession, kick off processing, etc.
    print(f"[worker] Got GCS file: gs://{bucket}/{name}")


async def main():
    subscriber = get_subscriber()

    topic_handlers = {
        "files.raw": {
            "gcs_file_finalized": handle_gcs_file_finalized,
            # "*" would be a catch-all
        }
    }

    await subscriber.subscribe(topic_handlers)


if __name__ == "__main__":
    asyncio.run(main())
