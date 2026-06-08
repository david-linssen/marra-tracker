#!/usr/bin/env python3
"""Persistent aisstream listener for MARRA + a tiny HTTP server for the map.

Holds the aisstream WebSocket open 24/7, filtered to MARRA's MMSI, and writes
every fix into track.json (on a Fly volume): stationary fixes refresh the last
point in place, and while moving the trail is capped to one point per
MIN_APPEND_SECONDS. Serves the Leaflet map at / and the data at /track.json.
"""
import asyncio
import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import websockets
from aiohttp import web

MMSI = os.environ.get("AIS_MMSI", "244038459")
API_KEY = os.environ.get("AISSTREAM_API_KEY", "")
STREAM_URL = "wss://stream.aisstream.io/v0/stream"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
TRACK_FILE = DATA_DIR / "track.json"
INDEX_FILE = Path(__file__).parent / "index.html"
MIN_MOVE_METERS = float(os.environ.get("AIS_MIN_MOVE_METERS", "50"))
MIN_APPEND_SECONDS = float(os.environ.get("AIS_MIN_APPEND_SECONDS", "120"))
# If no message arrives for this long, force a reconnect (guards against a stale/zombie socket).
WATCHDOG_SECONDS = float(os.environ.get("AIS_WATCHDOG_SECONDS", "900"))
# AIS position-report message types we accept. MARRA is Class B -> StandardClassBPositionReport.
POSITION_TYPES = ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport")
# A stationary period longer than this counts as a labelled "stop" (gets a place name).
STOP_SECONDS = float(os.environ.get("AIS_STOP_HOURS", "6")) * 3600
# One-time historical backfill baked into the image; applied to the volume on first boot.
SEED_FILE = Path(__file__).parent / "seed_track.json"
SEED_MARKER = DATA_DIR / ".history_imported"

_last_commit = None  # in-memory time of the last appended trail point


def _read_track():
    if TRACK_FILE.exists():
        try:
            return json.loads(TRACK_FILE.read_text() or "[]")
        except json.JSONDecodeError:
            return []
    return []


def _write_track(track):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(DATA_DIR), suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(track, f, indent=2)
    os.replace(tmp, TRACK_FILE)  # atomic: HTTP readers never see a torn file


def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def save_record(rec):
    global _last_commit
    track = _read_track()
    now = datetime.fromisoformat(rec["time"])
    rec.setdefault("arrived", rec["time"])  # first moment seen at this position
    if not track:
        track.append(rec)
        _last_commit = now
        action = "appended"
    else:
        last = track[-1]
        moved = _haversine_m(last["lat"], last["lon"], rec["lat"], rec["lon"])
        too_soon = _last_commit is not None and (now - _last_commit).total_seconds() < MIN_APPEND_SECONDS
        if moved < MIN_MOVE_METERS or too_soon:
            # keep the committed position + arrival time; refresh live status + last-seen time
            last.setdefault("arrived", last["time"])  # backfill legacy points
            for k in ("time", "ais_time", "sog", "cog", "heading", "nav_status"):
                last[k] = rec[k]
            if rec.get("destination"):
                last["destination"] = rec["destination"]
            action = "refreshed"
        else:
            track.append(rec)
            _last_commit = now
            action = "appended"
    _write_track(track)
    return action, len(track)


async def _reverse_geocode(lat, lon):
    """Nearest place name for a coordinate, via OpenStreetMap Nominatim (Dutch)."""
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": str(lat), "lon": str(lon), "format": "json", "zoom": "12", "accept-language": "nl"}
    headers = {"User-Agent": "marra-tracker/1.0 (personal sailing tracker)"}
    try:
        async with aiohttp.ClientSession(headers=headers,
                                         timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.get(url, params=params) as r:
                if r.status != 200:
                    return None
                d = await r.json()
        a = d.get("address", {})
        for k in ("harbour", "port", "town", "village", "city", "municipality", "suburb", "hamlet", "county"):
            if a.get(k):
                return a[k]
        dn = d.get("display_name", "")
        return dn.split(",")[0].strip() if dn else None
    except Exception as e:
        print(f"[listener] geocode failed: {e!r}", flush=True)
        return None


async def _geocode_stop_if_needed():
    """If the last point has been stationary > STOP_SECONDS and has no place yet,
    reverse-geocode it once and store the name (so each stop is geocoded a single time)."""
    track = _read_track()
    if not track:
        return
    last = track[-1]
    arrived = last.get("arrived")
    if not arrived or last.get("place"):
        return
    try:
        dwell = (datetime.fromisoformat(last["time"]) - datetime.fromisoformat(arrived)).total_seconds()
    except Exception:
        return
    if dwell < STOP_SECONDS:
        return
    place = await _reverse_geocode(last["lat"], last["lon"])
    if place:
        track = _read_track()  # re-read; the listener is the only writer, tail is unchanged
        if track and not track[-1].get("place"):
            track[-1]["place"] = place
            _write_track(track)
            print(f"[listener] stop geocoded: {place}", flush=True)


def _seed_history_if_needed():
    """Apply the baked-in historical backfill to the volume. Re-applies when a larger
    seed ships (version = point count), preserving any live points captured beyond the
    seed's window. Idempotent via a marker that records the applied seed size."""
    if not SEED_FILE.exists():
        return
    data = SEED_FILE.read_bytes()
    version = hashlib.md5(data).hexdigest()[:12]  # re-apply whenever the seed content changes
    applied = SEED_MARKER.read_text().strip() if SEED_MARKER.exists() else ""
    if applied == version:
        return
    try:
        seed = json.loads(data)
    except Exception as e:
        print(f"[seed] could not parse seed: {e!r}", flush=True)
        return
    if not seed:
        return
    current = _read_track()
    seed_last = seed[-1].get("time", "")
    tail = [p for p in current if p.get("time", "") > seed_last]  # live points beyond the seed
    merged = seed + tail
    _write_track(merged)
    try:
        SEED_MARKER.write_text(version + "\n")
    except Exception as e:
        print(f"[seed] could not write marker: {e!r}", flush=True)
    print(f"[seed] applied seed {version} ({len(seed)} pts) + {len(tail)} newer live = {len(merged)}", flush=True)


async def listener():
    global _last_commit
    _seed_history_if_needed()
    track = _read_track()
    if track:
        try:
            _last_commit = datetime.fromisoformat(track[-1]["time"])
        except Exception:
            _last_commit = None
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": [MMSI],
        # No FilterMessageTypes: MARRA is Class B, whose reports are NOT "PositionReport".
        # Filtered to one MMSI the volume is tiny, so just accept every type she sends.
    }
    static = {}
    backoff = 1
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=20,
                                          ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps(sub))
                backoff = 1
                print(f"[listener] connected, listening for MMSI {MMSI}", flush=True)
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WATCHDOG_SECONDS)
                    except asyncio.TimeoutError:
                        print(f"[listener] silent for {WATCHDOG_SECONDS:.0f}s — reconnecting", flush=True)
                        break  # re-establish the subscription
                    msg = json.loads(raw)
                    mtype = msg.get("MessageType")
                    meta = msg.get("MetaData", {})
                    message = msg.get("Message", {})
                    if mtype == "ShipStaticData":  # Class A static carries a destination
                        dest = (message.get("ShipStaticData", {}).get("Destination") or "").strip()
                        if dest:
                            static["destination"] = dest
                        continue
                    if mtype not in POSITION_TYPES:
                        continue
                    pr = message.get(mtype, {})
                    lat, lon = pr.get("Latitude"), pr.get("Longitude")
                    if lat is None or lon is None:
                        continue
                    sog, cog, hd = pr.get("Sog"), pr.get("Cog"), pr.get("TrueHeading")
                    rec = {
                        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "ais_time": meta.get("time_utc"),
                        "lat": lat,
                        "lon": lon,
                        "sog": None if sog is None or sog >= 102.3 else sog,
                        "cog": None if cog is None or cog >= 360 else cog,
                        "heading": None if hd is None or hd >= 511 else hd,
                        "nav_status": pr.get("NavigationalStatus"),  # absent for Class B -> None
                        "name": (meta.get("ShipName") or "MARRA").strip(),
                        "destination": static.get("destination", ""),
                    }
                    action, n = save_record(rec)
                    print(f"[listener] {action} ({n} pts): {rec['lat']:.5f},{rec['lon']:.5f} "
                          f"sog={rec['sog']} type={mtype}", flush=True)
                    await _geocode_stop_if_needed()
        except Exception as e:
            print(f"[listener] disconnected: {e!r} — reconnecting in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def h_index(request):
    return web.FileResponse(INDEX_FILE)


async def h_track(request):
    return web.json_response(
        _read_track(),
        headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"},
    )


async def h_health(request):
    t = _read_track()
    return web.json_response({"ok": True, "points": len(t),
                              "last": t[-1]["time"] if t else None})


async def _start_listener(app):
    app["listener_task"] = asyncio.create_task(listener())


async def _stop_listener(app):
    app["listener_task"].cancel()


def make_app():
    app = web.Application()
    app.router.add_get("/", h_index)
    app.router.add_get("/track.json", h_track)
    app.router.add_get("/health", h_health)
    app.on_startup.append(_start_listener)
    app.on_cleanup.append(_stop_listener)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), port=int(os.environ.get("PORT", "8080")))
