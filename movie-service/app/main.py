import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastavro.validation import ValidationError

from app.config import load_settings
from app.events import MovieEventIn
from app.generator import EventGenerator
from app.publisher import EventPublisher, PublishError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("movie.api")


class State:
    settings = None
    publisher: EventPublisher | None = None
    generator: EventGenerator | None = None


state = State()


@asynccontextmanager
async def lifespan(_: FastAPI):
    state.settings = load_settings()
    state.publisher = EventPublisher(state.settings)
    if state.settings.generator_enabled:
        state.generator = EventGenerator(state.publisher, state.settings)
        state.generator.start()
    yield
    if state.generator:
        state.generator.stop()
    state.publisher.close()


app = FastAPI(title="Movie Service (event producer)", version="1.0.0", lifespan=lifespan)


@app.post("/events", status_code=201)
async def publish_event(event: MovieEventIn):
    """Принимает событие в JSON, валидирует (Pydantic + Avro-схема) и публикует в Kafka.

    Ответ отправляется только после подтверждения записи брокерами (acks=all).
    """
    record = event.to_record()
    future = state.publisher.publish(record)
    try:
        result = await asyncio.wait_for(asyncio.wrap_future(future), timeout=state.settings.publish_timeout)
    except ValidationError as exc:
        return JSONResponse(status_code=422, content={"error": "SCHEMA_VALIDATION_FAILED", "message": str(exc)})
    except (PublishError, asyncio.TimeoutError) as exc:
        log.error("publish failed for event_id=%s: %s", record["event_id"], exc)
        return JSONResponse(status_code=503, content={"error": "KAFKA_UNAVAILABLE", "message": str(exc)})
    return {
        "event_id": result.event_id,
        "partition": result.partition,
        "offset": result.offset,
        "schema_version": state.publisher.schema_version,
    }


@app.get("/generator")
def generator_status():
    if state.generator is None:
        return {"enabled": False}
    return {"enabled": True, **state.generator.stats}


@app.get("/health")
def health():
    return {"status": "ok"}
