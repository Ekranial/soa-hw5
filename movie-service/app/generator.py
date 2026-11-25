"""Генератор синтетических событий: реалистичные сессии просмотра.

Сессия: [SEARCHED] → VIEW_STARTED(0) → (VIEW_PAUSED → VIEW_RESUMED)* → VIEW_FINISHED(длительность) → [LIKED]
либо обрывается паузой (пользователь не досмотрел). progress_seconds только растёт,
VIEW_FINISHED всегда идёт после VIEW_STARTED той же сессии.

Два режима:
* backfill — история за GENERATOR_BACKFILL_DAYS дней (нужна для retention и когорт).
  Детерминирован (seed): при рестарте в тот же день события получают те же event_id,
  и ClickHouse схлопывает повторы.
* live — непрерывный поток с текущим временем, GENERATOR_EVENTS_PER_SECOND событий/с.
"""

import datetime as dt
import logging
import math
import random
import threading
import time
import uuid

from app.events import make_record

log = logging.getLogger("movie.generator")

_NS = uuid.UUID("6f1c2a52-8d3e-4b7a-9c11-5b2f0e8a7d40")
DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
DEVICE_WEIGHTS = [0.45, 0.25, 0.2, 0.1]


def build_catalog(rng: random.Random, size: int = 60):
    """Фильмы m-001..m-060 с длительностью 80–180 мин и популярностью по закону Ципфа."""
    movies = [(f"m-{i:03d}", rng.randint(80, 180) * 60) for i in range(1, size + 1)]
    weights = [1 / (rank ** 1.1) for rank in range(1, size + 1)]
    return movies, weights


def plan_session(rng: random.Random, duration: int) -> list[tuple[str, int, int]]:
    """План сессии: список (event_type, progress_seconds, секунд от предыдущего события)."""
    steps: list[tuple[str, int | None, int]] = []
    if rng.random() < 0.4:
        steps.append(("SEARCHED", None, 0))
    steps.append(("VIEW_STARTED", 0, rng.randint(5, 90) if steps else 0))
    progress = 0
    finished = rng.random() < 0.65
    pauses = rng.choice([0, 1, 1, 2, 3])
    for _ in range(pauses):
        watched = rng.randint(300, 1800)
        if progress + watched >= duration:
            break
        progress += watched
        steps.append(("VIEW_PAUSED", progress, watched))
        steps.append(("VIEW_RESUMED", progress, rng.randint(30, 900)))
    if finished:
        steps.append(("VIEW_FINISHED", duration, duration - progress))
        if rng.random() < 0.35:
            steps.append(("LIKED", duration, rng.randint(2, 30)))
    else:
        # не досмотрел: смотрел ещё немного и закрыл на паузе
        watched = min(rng.randint(120, 1500), max(duration - progress - 60, 1))
        progress += watched
        steps.append(("VIEW_PAUSED", progress, watched))
        if rng.random() < 0.05:
            steps.append(("LIKED", progress, rng.randint(2, 30)))
    return steps


class EventGenerator:
    def __init__(self, publisher, settings):
        self._pub = publisher
        self._s = settings
        self._rng = random.Random(settings.generator_seed)
        self.movies, self.weights = build_catalog(random.Random(settings.generator_seed))
        self.users = [f"u-{i:05d}" for i in range(1, settings.generator_users + 1)]
        self.user_device = {
            u: random.Random(f"{settings.generator_seed}:{u}").choices(DEVICES, DEVICE_WEIGHTS)[0] for u in self.users
        }
        self.stats = {"backfill_events": 0, "backfill_done": False, "live_events": 0, "failed": 0}
        self._stop = threading.Event()

    # ------------------------------------------------------------------ helpers

    def _device(self, rng: random.Random, user: str) -> str:
        return self.user_device[user] if rng.random() < 0.8 else rng.choices(DEVICES, DEVICE_WEIGHTS)[0]

    def _movie(self, rng: random.Random):
        return rng.choices(self.movies, self.weights)[0]

    def _on_done(self, fut) -> None:
        if fut.exception() is not None:
            self.stats["failed"] += 1

    def _emit(self, record: dict, counter: str) -> None:
        self._pub.publish(record).add_done_callback(self._on_done)
        self.stats[counter] += 1

    # ------------------------------------------------------------------ backfill

    def backfill(self) -> None:
        days = self._s.generator_backfill_days
        if days <= 0:
            self.stats["backfill_done"] = True
            return
        seed = self._s.generator_seed
        rng = random.Random(seed)
        now = dt.datetime.now(dt.timezone.utc)
        today = now.date()
        events: list[dict] = []
        for user in self.users:
            cohort_age = rng.randint(0, days)              # сколько дней назад пришёл пользователь
            for age_today in range(cohort_age, -1, -1):    # от первого дня до сегодня
                day = today - dt.timedelta(days=age_today)
                life_day = cohort_age - age_today         # день жизни пользователя
                if life_day > 0:
                    p_return = 0.45 * math.exp(-(life_day - 1) / 4) + 0.06
                    if rng.random() > p_return:
                        continue
                day_start = dt.datetime.combine(day, dt.time(6, 0), tzinfo=dt.timezone.utc)
                day_end = dt.datetime.combine(day, dt.time(21, 0), tzinfo=dt.timezone.utc)
                for s_idx in range(rng.choice([1, 1, 1, 2, 2, 3])):
                    movie_id, duration = self._movie(rng)
                    plan = plan_session(rng, duration)
                    total = sum(step[2] for step in plan)
                    latest_start = min(day_end, now - dt.timedelta(seconds=total + 60))
                    if latest_start <= day_start:
                        continue  # сегодня сессия не успела бы закончиться к текущему моменту
                    start = day_start + dt.timedelta(
                        seconds=rng.randint(0, int((latest_start - day_start).total_seconds())))
                    session_id = str(uuid.uuid5(_NS, f"{seed}:{user}:{day}:{s_idx}"))
                    device = self._device(rng, user)
                    ts = start
                    for n, (etype, progress, delta) in enumerate(plan):
                        ts += dt.timedelta(seconds=delta)
                        events.append(make_record(
                            event_id=str(uuid.uuid5(_NS, f"{session_id}:{n}")),
                            user_id=user, movie_id=movie_id, event_type=etype, timestamp=ts,
                            device_type=device, session_id=session_id, progress_seconds=progress,
                        ))
        # по времени: порядок событий каждого пользователя в партиции совпадает с реальным
        events.sort(key=lambda e: e["timestamp"])
        log.info("backfill: generated %d events for %d users over %d days, publishing...",
                 len(events), len(self.users), days + 1)
        for record in events:
            if self._stop.is_set():
                return
            self._emit(record, "backfill_events")
        self._pub.flush(120)
        self.stats["backfill_done"] = True
        log.info("backfill finished: %d events published (failed: %d)", self.stats["backfill_events"],
                 self.stats["failed"])

    # ------------------------------------------------------------------ live

    def live(self) -> None:
        eps = self._s.generator_events_per_second
        if eps <= 0:
            return
        rng = self._rng
        active: list[dict] = []
        log.info("live generator started: %.1f events/s", eps)
        while not self._stop.is_set():
            if len(active) < 25 and (not active or rng.random() < 0.25):
                user = rng.choice(self.users)
                movie_id, duration = self._movie(rng)
                active.append({
                    "user": user, "movie": movie_id, "device": self._device(rng, user),
                    "session": str(uuid.uuid4()), "plan": plan_session(rng, duration), "pos": 0,
                })
            session = rng.choice(active)
            etype, progress, _ = session["plan"][session["pos"]]
            session["pos"] += 1
            self._emit(make_record(
                event_id=str(uuid.uuid4()), user_id=session["user"], movie_id=session["movie"],
                event_type=etype, timestamp=dt.datetime.now(dt.timezone.utc), device_type=session["device"],
                session_id=session["session"], progress_seconds=progress,
            ), "live_events")
            if session["pos"] >= len(session["plan"]):
                active.remove(session)
            time.sleep(1 / eps)

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        def run():
            try:
                self.backfill()
                self.live()
            except Exception:  # noqa: BLE001
                log.exception("generator crashed")

        threading.Thread(target=run, name="generator", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
