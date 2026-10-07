import asyncio
from datetime import datetime

from aiohttp import web

from src.config.settings import get_settings
from src.db.database import AsyncSessionLocal, Base, engine
from src.integrations.messaging.service_bus_consumer import ServiceBusConsumer
from src.orchestrator.orchestrator import Orchestrator
from src.orchestrator.state import IDocFailureEvent
from src.poller.rfc_poller import RfcPoller
from src.utils.logger import configure_logging, get_logger

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)

orchestrator = Orchestrator()
consumer = ServiceBusConsumer()


async def _process_work_queue() -> None:
    async for body, ack in consumer.consume_work_queue():
        try:
            event = IDocFailureEvent(
                idoc_number=body["idoc_number"],
                idoc_type=body["idoc_type"],
                message_type=body["message_type"],
                sap_system_id=body["sap_system_id"],
                direction=body["direction"],
                status_code=body["status_code"],
                status_text=body["status_text"],
                idoc_status_timestamp=datetime.fromisoformat(body["idoc_status_timestamp"]),
                partner_number=body.get("partner_number"),
                segment_errors=body.get("segment_errors", []),
                raw_status_record=body.get("raw_status_record", {}),
            )
            async with AsyncSessionLocal() as session:
                await orchestrator.handle_new_failure(event, session)
            await ack.complete()
        except Exception as exc:  # noqa: BLE001
            logger.error("work_queue_message_failed", error=str(exc), body_keys=list(body.keys()))
            await ack.abandon()


async def _process_control_topic() -> None:
    async for body, ack in consumer.consume_control_topic():
        try:
            case_id = body["case_id"]
            approved = body["action"] == "approved"
            async with AsyncSessionLocal() as session:
                await orchestrator.resume_after_approval(case_id, approved, session)
            await ack.complete()
        except Exception as exc:  # noqa: BLE001
            logger.error("control_topic_message_failed", error=str(exc))
            await ack.abandon()


async def _healthz(_request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def _run_health_server() -> None:
    app = web.Application()
    app.router.add_get("/healthz", _healthz)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8090)
    await site.start()
    logger.info("health_server_started", port=8090)


async def main() -> None:
    logger.info("agent_worker_starting", environment=settings.environment)

    if settings.environment != "production":
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    poller = RfcPoller()

    await asyncio.gather(
        _run_health_server(),
        poller.run_forever(),
        _process_work_queue(),
        _process_control_topic(),
    )


if __name__ == "__main__":
    asyncio.run(main())