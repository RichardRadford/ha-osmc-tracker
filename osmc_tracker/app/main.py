"""OSMC Watch Tracker – a Home Assistant app.

Connects to Kodi (OSMC) over its JSON-RPC websocket, records every playback
session (what, when, how long, how far through) into SQLite, publishes live
sensors to Home Assistant and serves a small history/stats UI over ingress.
"""

import asyncio
import csv
import io
import json
import logging
import os
import sqlite3
import time
from pathlib import Path

import aiohttp
from aiohttp import web

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
OPTIONS_PATH = Path(os.environ.get("OPTIONS_PATH", DATA_DIR / "options.json"))
STATIC_DIR = Path(__file__).parent / "static"
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN")
HA_API = os.environ.get("HA_API", "http://supervisor/core/api")
WEB_PORT = int(os.environ.get("WEB_PORT", "8099"))

DEFAULTS = {
    "kodi_host": "osmc.local",
    "kodi_port": 9090,
    "poll_interval": 10,
    "min_session_seconds": 60,
    "completed_percent": 90,
    "publish_sensors": True,
    "entity_prefix": "osmc",
    "log_level": "info",
}

log = logging.getLogger("osmc_tracker")


def load_options():
    opts = dict(DEFAULTS)
    try:
        opts.update(json.loads(OPTIONS_PATH.read_text()))
    except FileNotFoundError:
        log.warning("No options file at %s, using defaults", OPTIONS_PATH)
    return opts


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    ended_at        REAL,
    media_type      TEXT,
    title           TEXT,
    show_title      TEXT,
    season          INTEGER,
    episode         INTEGER,
    year            INTEGER,
    artist          TEXT,
    album           TEXT,
    channel         TEXT,
    file            TEXT,
    display         TEXT,
    duration        REAL,
    start_position  REAL,
    end_position    REAL,
    max_percent     REAL,
    watched_seconds REAL NOT NULL DEFAULT 0,
    pauses          INTEGER NOT NULL DEFAULT 0,
    completed       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at);
"""


class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def execute(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    def query(self, sql, params=()):
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def close_orphans(self, min_seconds):
        """Sessions left open by a crash/restart: close them at their last update."""
        self.execute(
            "DELETE FROM sessions WHERE ended_at IS NULL AND watched_seconds < ?",
            (min_seconds,),
        )
        self.execute("UPDATE sessions SET ended_at = updated_at WHERE ended_at IS NULL")


# --------------------------------------------------------------------------
# Kodi JSON-RPC websocket client
# --------------------------------------------------------------------------


class KodiError(Exception):
    pass


class Kodi:
    def __init__(self, host, port, on_notification):
        self.url = f"ws://{host}:{port}/jsonrpc"
        self.on_notification = on_notification
        self.ws = None
        self._id = 0
        self._pending = {}

    @property
    def connected(self):
        return self.ws is not None and not self.ws.closed

    async def call(self, method, params=None, timeout=10):
        if not self.connected:
            raise KodiError("not connected")
        self._id += 1
        rid = self._id
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        msg = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        try:
            await self.ws.send_str(json.dumps(msg))
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(rid, None)

    async def run(self, session, on_connect, on_disconnect):
        backoff = 5
        while True:
            try:
                log.info("Connecting to Kodi at %s", self.url)
                async with session.ws_connect(self.url, heartbeat=30, max_msg_size=0) as ws:
                    self.ws = ws
                    backoff = 5
                    log.info("Connected to Kodi")
                    asyncio.create_task(on_connect())
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            data = json.loads(msg.data)
                        except ValueError:
                            continue
                        if "id" in data and data["id"] in self._pending:
                            fut = self._pending[data["id"]]
                            if not fut.done():
                                if "error" in data:
                                    fut.set_exception(KodiError(data["error"]))
                                else:
                                    fut.set_result(data.get("result"))
                        elif "method" in data:
                            self.on_notification(data["method"], data.get("params", {}))
            except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as e:
                log.debug("Kodi connection error: %s", e)
            finally:
                if self.ws is not None:
                    log.info("Disconnected from Kodi")
                self.ws = None
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(KodiError("disconnected"))
                self._pending.clear()
                await on_disconnect()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def kodi_time(t):
    if not t:
        return 0.0
    return (t.get("hours", 0) * 3600 + t.get("minutes", 0) * 60
            + t.get("seconds", 0) + t.get("milliseconds", 0) / 1000)


def display_name(item):
    t = item.get("type")
    title = item.get("title") or item.get("label") or ""
    if t == "episode" and item.get("showtitle"):
        s, e = item.get("season", -1), item.get("episode", -1)
        se = f" S{s:02d}E{e:02d}" if s is not None and s >= 0 and e is not None and e >= 0 else ""
        return f"{item['showtitle']}{se} · {title}".strip(" ·")
    if t == "movie" and item.get("year"):
        return f"{title} ({item['year']})"
    if t == "song" and item.get("artist"):
        return f"{', '.join(item['artist'])} – {title}"
    if t == "channel":
        return item.get("channel") or title
    if title:
        return title
    f = item.get("file") or ""
    return f.rstrip("/").split("/")[-1] or "Unknown"


def _nz(v):
    """Kodi uses -1 / 0 / '' for 'unknown'; normalise to None."""
    if v in (None, "", -1) or v == []:
        return None
    return v


# --------------------------------------------------------------------------
# Tracker
# --------------------------------------------------------------------------


class Tracker:
    ITEM_PROPS = ["title", "showtitle", "season", "episode", "year", "artist",
                  "album", "file", "channel", "runtime", "duration"]
    PLAYER_PROPS = ["speed", "time", "totaltime", "percentage"]

    def __init__(self, opts, db):
        self.opts = opts
        self.db = db
        self.kodi = Kodi(opts["kodi_host"], opts["kodi_port"], self._notify)
        self.wake = asyncio.Event()
        self.lock = asyncio.Lock()
        self.current = None       # dict describing the active session
        self.state = "disconnected"  # disconnected | idle | playing | paused
        self.last_tick = None
        self.was_playing = False
        self.ha = None
        self.http = None

    # -- Kodi events ----------------------------------------------------
    def _notify(self, method, params):
        if method.startswith("Player.") or method in ("System.OnQuit", "System.OnSleep"):
            log.debug("Kodi event %s", method)
            self.wake.set()

    async def _on_connect(self):
        self.wake.set()

    async def _on_disconnect(self):
        async with self.lock:
            self._advance_clock()
            await self._end_session("kodi disconnected")
            self._set_state("disconnected")
        await self.ha_publish(force=True)

    # -- main loop --------------------------------------------------------
    async def run(self):
        self.http = aiohttp.ClientSession()
        if self.opts["publish_sensors"] and SUPERVISOR_TOKEN:
            self.ha = HAPublisher(self.http, self.opts["entity_prefix"])
        asyncio.create_task(self.kodi.run(self.http, self._on_connect, self._on_disconnect))
        await self.ha_publish(force=True)
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), self.opts["poll_interval"])
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            if self.kodi.connected:
                try:
                    async with self.lock:
                        await self.tick()
                except KodiError as e:
                    log.debug("Poll failed: %s", e)
                except Exception:  # noqa: BLE001
                    log.exception("Unexpected error while polling Kodi")
            await self.ha_publish()

    def _advance_clock(self):
        """Credit wall-clock time since the last tick if we were playing."""
        now = time.time()
        if self.current and self.was_playing and self.last_tick:
            # cap a single gap so a suspended host doesn't inflate totals
            gap = min(now - self.last_tick, max(self.opts["poll_interval"] * 3, 60))
            self.current["watched_seconds"] += max(gap, 0)
        self.last_tick = now

    def _set_state(self, s):
        if s != self.state:
            log.debug("State %s -> %s", self.state, s)
        self.state = s

    async def tick(self):
        players = await self.kodi.call("Player.GetActivePlayers") or []
        # prefer video, then audio, then anything
        player = None
        for kind in ("video", "audio", "picture"):
            player = next((p for p in players if p.get("type") == kind), None)
            if player:
                break
        if player is None or player.get("type") == "picture":
            self._advance_clock()
            await self._end_session("stopped")
            self._set_state("idle")
            self.was_playing = False
            return

        pid = player["playerid"]
        item = (await self.kodi.call("Player.GetItem",
                                     {"playerid": pid, "properties": self.ITEM_PROPS}) or {}).get("item", {})
        props = await self.kodi.call("Player.GetProperties",
                                     {"playerid": pid, "properties": self.PLAYER_PROPS}) or {}

        self._advance_clock()
        speed = props.get("speed", 0)
        position = kodi_time(props.get("time"))
        total = kodi_time(props.get("totaltime")) or item.get("runtime") or item.get("duration") or None
        percent = props.get("percentage") or (position / total * 100 if total else 0)

        key = (item.get("type"), item.get("id"), item.get("file"), display_name(item))
        if not self.current or self.current["key"] != key:
            await self._end_session("next item")
            self._start_session(key, item, position, total)

        c = self.current
        if self.was_playing and speed == 0:
            c["pauses"] += 1
        c["end_position"] = position
        c["duration"] = total or c["duration"]
        c["percent"] = percent
        c["max_percent"] = max(c["max_percent"], percent)
        self.was_playing = speed != 0
        self._set_state("playing" if self.was_playing else "paused")
        self._save_current()

    def _start_session(self, key, item, position, total):
        now = time.time()
        self.current = {
            "key": key,
            "id": None,
            "started_at": now,
            "media_type": item.get("type") or "unknown",
            "title": _nz(item.get("title") or item.get("label")),
            "show_title": _nz(item.get("showtitle")),
            "season": _nz(item.get("season")),
            "episode": _nz(item.get("episode")),
            "year": _nz(item.get("year")),
            "artist": ", ".join(item.get("artist") or []) or None,
            "album": _nz(item.get("album")),
            "channel": _nz(item.get("channel")),
            "file": _nz(item.get("file")),
            "display": display_name(item),
            "duration": total,
            "start_position": position,
            "end_position": position,
            "percent": 0,
            "max_percent": 0,
            "watched_seconds": 0.0,
            "pauses": 0,
        }
        self.was_playing = False
        log.info("Started: %s", self.current["display"])

    def _save_current(self):
        c = self.current
        now = time.time()
        if c["id"] is None:
            # only write once it's long enough to count, keeps junk out of the DB
            if c["watched_seconds"] < self.opts["min_session_seconds"]:
                return
            cur = self.db.execute(
                """INSERT INTO sessions (started_at, updated_at, media_type, title, show_title,
                   season, episode, year, artist, album, channel, file, display, duration,
                   start_position, end_position, max_percent, watched_seconds, pauses)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (c["started_at"], now, c["media_type"], c["title"], c["show_title"],
                 c["season"], c["episode"], c["year"], c["artist"], c["album"], c["channel"],
                 c["file"], c["display"], c["duration"], c["start_position"],
                 c["end_position"], c["max_percent"], c["watched_seconds"], c["pauses"]))
            c["id"] = cur.lastrowid
        else:
            self.db.execute(
                """UPDATE sessions SET updated_at=?, duration=?, end_position=?, max_percent=?,
                   watched_seconds=?, pauses=? WHERE id=?""",
                (now, c["duration"], c["end_position"], c["max_percent"],
                 c["watched_seconds"], c["pauses"], c["id"]))

    async def _end_session(self, reason):
        c = self.current
        if not c:
            return
        self.current = None
        self.was_playing = False
        completed = c["max_percent"] >= self.opts["completed_percent"]
        if c["id"] is None:
            log.info("Discarded short session (%.0fs): %s", c["watched_seconds"], c["display"])
            return
        now = time.time()
        self.db.execute(
            """UPDATE sessions SET updated_at=?, ended_at=?, end_position=?, max_percent=?,
               watched_seconds=?, pauses=?, completed=? WHERE id=?""",
            (now, now, c["end_position"], c["max_percent"], c["watched_seconds"],
             c["pauses"], int(completed), c["id"]))
        log.info("Ended (%s): %s – watched %.0f min, %.0f%%%s", reason, c["display"],
                 c["watched_seconds"] / 60, c["max_percent"], " ✓" if completed else "")
        if self.ha:
            await self.ha.fire_event("osmc_tracker_session_ended", {
                "title": c["display"], "media_type": c["media_type"],
                "show": c["show_title"], "season": c["season"], "episode": c["episode"],
                "watched_minutes": round(c["watched_seconds"] / 60, 1),
                "percent": round(c["max_percent"], 1), "completed": completed,
            })

    # -- views ------------------------------------------------------------
    def now_playing(self):
        c = self.current
        out = {"state": self.state, "kodi": self.kodi.url}
        if c:
            out.update({k: c[k] for k in (
                "display", "media_type", "title", "show_title", "season", "episode", "year",
                "artist", "album", "channel", "started_at", "duration", "end_position",
                "percent", "pauses")})
            out["watched_seconds"] = round(c["watched_seconds"])
            out["recorded"] = c["id"] is not None
        return out

    async def ha_publish(self, force=False):
        if not self.ha:
            return
        try:
            await self.ha.publish(self, force)
        except Exception as e:  # noqa: BLE001
            log.warning("Failed to update Home Assistant: %s", e)


# --------------------------------------------------------------------------
# Home Assistant sensors (via Supervisor proxy to the Core REST API)
# --------------------------------------------------------------------------


class HAPublisher:
    def __init__(self, http, prefix):
        self.http = http
        self.prefix = prefix
        self.headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
        self.last = {}
        self.last_sent = {}

    async def _set(self, entity, state, attrs, force):
        """POST a state only when it changed, or every 10 min (HA forgets
        API-set states on restart, so the refresh brings them back)."""
        payload = {"state": str(state), "attributes": attrs}
        sig = json.dumps(payload, sort_keys=True, default=str)
        now = time.time()
        stale = now - self.last_sent.get(entity, 0) > 600
        if not force and not stale and self.last.get(entity) == sig:
            return
        async with self.http.post(f"{HA_API}/states/{entity}", json=payload,
                                  headers=self.headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status >= 400:
                raise RuntimeError(f"{entity}: HTTP {r.status} {await r.text()}")
        self.last[entity] = sig
        self.last_sent[entity] = now

    async def fire_event(self, event, data):
        try:
            async with self.http.post(f"{HA_API}/events/{event}", json=data,
                                      headers=self.headers, timeout=aiohttp.ClientTimeout(total=10)):
                pass
        except Exception as e:  # noqa: BLE001
            log.warning("Failed to fire event: %s", e)

    async def publish(self, tracker, force=False):
        p = self.prefix
        np = tracker.now_playing()
        # Progress is bucketed (5%, whole minutes) so the recorder isn't flooded.
        attrs = {
            "friendly_name": "OSMC now playing",
            "icon": "mdi:television-play",
            "playback_state": np["state"],
        }
        if np.get("display"):
            attrs.update({
                "media_type": np["media_type"], "title": np["title"],
                "show": np["show_title"], "season": np["season"], "episode": np["episode"],
                "year": np["year"], "artist": np["artist"], "channel": np["channel"],
                "session_minutes": int(np["watched_seconds"] // 60),
                "progress_percent": int((np["percent"] or 0) // 5 * 5),
                "duration_minutes": round((np["duration"] or 0) / 60),
            })
        state = np.get("display") or np["state"]
        await self._set(f"sensor.{p}_now_playing", state[:255], attrs, force)

        # today's totals
        rows = tracker.db.query(
            """SELECT media_type, SUM(watched_seconds) s, COUNT(*) n FROM sessions
               WHERE date(started_at,'unixepoch','localtime') = date('now','localtime')
               GROUP BY media_type""")
        by_type = {r["media_type"]: r["s"] for r in rows}
        total = sum(by_type.values())
        c = tracker.current
        if c and c["id"] is None:  # not yet in DB
            total += c["watched_seconds"]
        await self._set(f"sensor.{p}_watch_time_today", int(total // 60), {
            "friendly_name": "OSMC watch time today",
            "unit_of_measurement": "min", "icon": "mdi:timer-play-outline",
            "state_class": "measurement",
            **{f"{k}_minutes": int(v // 60) for k, v in by_type.items()},
            "sessions": sum(r["n"] for r in rows),
        }, force)

        last = tracker.db.query(
            "SELECT * FROM sessions WHERE ended_at IS NOT NULL ORDER BY ended_at DESC LIMIT 1")
        if last:
            l = last[0]
            await self._set(f"sensor.{p}_last_watched", (l["display"] or "")[:255], {
                "friendly_name": "OSMC last watched", "icon": "mdi:history",
                "media_type": l["media_type"],
                "watched_minutes": round(l["watched_seconds"] / 60, 1),
                "percent": round(l["max_percent"] or 0), "completed": bool(l["completed"]),
                "ended": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(l["ended_at"])),
            }, force)


# --------------------------------------------------------------------------
# Web UI / API (served through HA ingress)
# --------------------------------------------------------------------------


def build_app(tracker):
    db = tracker.db
    routes = web.RouteTableDef()

    @routes.get("/")
    async def index(_):
        return web.FileResponse(STATIC_DIR / "index.html")

    @routes.get("/api/now")
    async def api_now(_):
        return web.json_response(tracker.now_playing())

    @routes.get("/api/stats")
    async def api_stats(request):
        days = max(1, min(int(request.query.get("days", 30)), 3650))
        since = time.time() - days * 86400
        w = "started_at >= ?"
        totals = db.query(f"""SELECT COUNT(*) sessions, COALESCE(SUM(watched_seconds),0) seconds,
                              COALESCE(SUM(completed),0) completed FROM sessions WHERE {w}""", (since,))[0]
        by_type = db.query(f"""SELECT media_type, COUNT(*) sessions, SUM(watched_seconds) seconds
                               FROM sessions WHERE {w} GROUP BY media_type ORDER BY seconds DESC""", (since,))
        daily = db.query(f"""SELECT date(started_at,'unixepoch','localtime') day,
                             SUM(watched_seconds) seconds, COUNT(*) sessions
                             FROM sessions WHERE {w} GROUP BY day ORDER BY day""", (since,))
        hourly = db.query(f"""SELECT CAST(strftime('%H',started_at,'unixepoch','localtime') AS INTEGER) hour,
                              SUM(watched_seconds) seconds FROM sessions WHERE {w}
                              GROUP BY hour ORDER BY hour""", (since,))
        shows = db.query(f"""SELECT show_title name, SUM(watched_seconds) seconds, COUNT(*) sessions,
                             COUNT(DISTINCT season||'x'||episode) episodes, MAX(started_at) last
                             FROM sessions WHERE {w} AND media_type='episode' AND show_title IS NOT NULL
                             GROUP BY show_title ORDER BY seconds DESC LIMIT 15""", (since,))
        movies = db.query(f"""SELECT display name, SUM(watched_seconds) seconds, COUNT(*) sessions,
                              MAX(max_percent) percent, MAX(completed) completed, MAX(started_at) last
                              FROM sessions WHERE {w} AND media_type='movie'
                              GROUP BY display ORDER BY last DESC LIMIT 15""", (since,))
        music = db.query(f"""SELECT artist name, SUM(watched_seconds) seconds, COUNT(*) sessions
                             FROM sessions WHERE {w} AND media_type='song' AND artist IS NOT NULL
                             GROUP BY artist ORDER BY seconds DESC LIMIT 10""", (since,))
        return web.json_response({"days": days, "totals": totals, "by_type": by_type,
                                  "daily": daily, "hourly": hourly, "shows": shows,
                                  "movies": movies, "music": music})

    @routes.get("/api/history")
    async def api_history(request):
        limit = max(1, min(int(request.query.get("limit", 50)), 500))
        offset = max(0, int(request.query.get("offset", 0)))
        q = request.query.get("q", "").strip()
        mtype = request.query.get("type", "").strip()
        where, params = ["1=1"], []
        if q:
            where.append("(display LIKE ? OR file LIKE ?)")
            params += [f"%{q}%", f"%{q}%"]
        if mtype:
            where.append("media_type = ?")
            params.append(mtype)
        sql = " AND ".join(where)
        total = db.query(f"SELECT COUNT(*) n FROM sessions WHERE {sql}", params)[0]["n"]
        rows = db.query(f"SELECT * FROM sessions WHERE {sql} ORDER BY started_at DESC LIMIT ? OFFSET ?",
                        params + [limit, offset])
        return web.json_response({"total": total, "rows": rows})

    @routes.delete("/api/history/{sid}")
    async def api_delete(request):
        sid = int(request.match_info["sid"])
        if tracker.current and tracker.current["id"] == sid:
            return web.json_response({"error": "session in progress"}, status=409)
        db.execute("DELETE FROM sessions WHERE id=?", (sid,))
        return web.json_response({"ok": True})

    @routes.get("/api/export.csv")
    async def api_export(_):
        rows = db.query("SELECT * FROM sessions ORDER BY started_at")
        buf = io.StringIO()
        cols = [d[1] for d in db.conn.execute("PRAGMA table_info(sessions)")]
        wr = csv.DictWriter(buf, fieldnames=cols + ["started_local", "ended_local"])
        wr.writeheader()
        fmt = lambda t: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) if t else ""
        for r in rows:
            r["started_local"], r["ended_local"] = fmt(r["started_at"]), fmt(r["ended_at"])
            wr.writerow(r)
        return web.Response(text=buf.getvalue(), content_type="text/csv",
                            headers={"Content-Disposition": 'attachment; filename="osmc_history.csv"'})

    app = web.Application()
    app.add_routes(routes)
    app.router.add_static("/static/", STATIC_DIR)
    return app


async def main():
    opts = load_options()
    logging.basicConfig(level=getattr(logging, str(opts["log_level"]).upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = DB(DATA_DIR / "osmc_tracker.db")
    db.close_orphans(opts["min_session_seconds"])
    tracker = Tracker(opts, db)

    runner = web.AppRunner(build_app(tracker))
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", WEB_PORT).start()
    log.info("Web UI listening on port %d", WEB_PORT)
    if not SUPERVISOR_TOKEN:
        log.info("No SUPERVISOR_TOKEN – Home Assistant sensors disabled")
    await tracker.run()


if __name__ == "__main__":
    asyncio.run(main())
