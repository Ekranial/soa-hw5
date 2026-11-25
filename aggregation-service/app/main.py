import datetime as dt
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from app.aggregator import Aggregator
from app.config import load_settings
from app.exporter import NothingToExport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logging.getLogger("botocore").setLevel(logging.WARNING)
log = logging.getLogger("agg.api")


class State:
    aggregator: Aggregator | None = None


state = State()


@asynccontextmanager
async def lifespan(_: FastAPI):
    state.aggregator = Aggregator(load_settings())
    state.aggregator.start()
    yield
    state.aggregator.stop()


app = FastAPI(title="Aggregation Service", version="1.0.0", lifespan=lifespan)


@app.post("/aggregate")
def aggregate(date: dt.date = Query(..., description="Дата пересчёта, YYYY-MM-DD")):
    """Ручной пересчёт метрик за дату (и когорт за 7 предыдущих дней), не дожидаясь расписания."""
    return state.aggregator.aggregate_date(date)


@app.post("/aggregate/run")
def aggregate_run():
    """Внеплановый запуск цикла: пересчитать все даты с новыми событиями."""
    return state.aggregator.run_cycle()


@app.post("/export")
def export(date: dt.date = Query(..., description="Дата выгрузки, YYYY-MM-DD")):
    """Ручная выгрузка агрегатов за дату в S3. Повторный вызов перезаписывает файл."""
    try:
        return state.aggregator.export_date(date)
    except NothingToExport as exc:
        return JSONResponse(status_code=404, content={"error": "NO_AGGREGATES", "message": str(exc)})
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"error": "EXPORT_FAILED", "message": str(exc)})


@app.get("/metrics")
def metrics(date: dt.date = Query(...)):
    """Готовые метрики за дату из PostgreSQL."""
    return state.aggregator.pg.read_metrics(date)


@app.get("/health")
def health():
    return {"status": "ok"}
