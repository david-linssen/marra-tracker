#!/usr/bin/env python3
"""Fetch MARRA's latest AIS position from aisstream.io and append it to docs/track.json.

Designed to run once per invocation (e.g. hourly via cron / GitHub Actions):
connect, listen for one PositionReport for our MMSI, write it, exit.
"""
import asyncio
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets

MMSI = os.environ.get("AIS_MMSI", "244038459")  # default: MARRA
STREAM_URL = "wss://stream.aisstream.io/v0/stream"
TRACK_FILE = Path(__file__).parent / "docs" / "track.json"
WAIT_SECONDS = int(os.environ.get("AIS_WAIT_SECONDS", "180"))
# Fixes closer than this to the last logged point refresh it in place instead of
# appending, so a moored/anchored boat stays one dot rather than hundreds.
MIN_MOVE_METERS = float(os.environ.get("AIS_MIN_MOVE_METERS", "50"))


def load_api_key() -> str:
    key = os.environ.get("AISSTREAM_API_KEY")
    if not key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("AISSTREAM_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not key:
        sys.exit("AISSTREAM_API_KEY is not set (env var or .env file).")
    return key


async def fetch_position(api_key: str):
    """Listen up to WAIT_SECONDS for one PositionReport. Returns a record dict or None."""
    subscribe = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],  # whole world; filtered by MMSI below
        "FiltersShipMMSI": [MMSI],
        # No FilterMessageTypes: MARRA is Class B (StandardClassBPositionReport), which
        # "PositionReport" (Class A only) would exclude. Filtered to one MMSI volume is tiny.
    }
    # Class A + Class B position-report message types.
    position_types = ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport")
    static = {}
    loop = asyncio.get_event_loop()
    deadline = loop.time() + WAIT_SECONDS
    async with websockets.connect(STREAM_URL, ping_interval=20) as ws:
        await ws.send(json.dumps(subscribe))
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            msg = json.loads(raw)
            mtype = msg.get("MessageType")
            meta = msg.get("MetaData", {})
            message = msg.get("Message", {})
            if mtype == "ShipStaticData":
                sd = message.get("ShipStaticData", {})
                static["destination"] = (sd.get("Destination") or "").strip()
                continue
            if mtype in position_types:
                pr = message.get(mtype, {})
                lat, lon = pr.get("Latitude"), pr.get("Longitude")
                if lat is None or lon is None:
                    continue
                sog = pr.get("Sog")
                cog = pr.get("Cog")
                heading = pr.get("TrueHeading")
                return {
                    "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "ais_time": meta.get("time_utc"),
                    "lat": lat,
                    "lon": lon,
                    "sog": None if sog is None or sog >= 102.3 else sog,
                    "cog": None if cog is None or cog >= 360 else cog,
                    "heading": None if heading is None or heading >= 511 else heading,
                    "nav_status": pr.get("NavigationalStatus"),  # absent for Class B -> None
                    "name": (meta.get("ShipName") or "MARRA").strip(),
                    "destination": static.get("destination", ""),
                }


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0  # earth radius, metres
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def save_record(record: dict):
    """Append the record, or—if within MIN_MOVE_METERS of the last point—refresh that
    point in place (keep its lat/lon, update time + dynamic fields). Returns (action, count)."""
    TRACK_FILE.parent.mkdir(parents=True, exist_ok=True)
    track = []
    if TRACK_FILE.exists():
        try:
            track = json.loads(TRACK_FILE.read_text() or "[]")
        except json.JSONDecodeError:
            track = []

    if track and _haversine_m(track[-1]["lat"], track[-1]["lon"],
                              record["lat"], record["lon"]) < MIN_MOVE_METERS:
        last = track[-1]  # stationary: keep anchored position, refresh the rest
        for field in ("time", "ais_time", "sog", "cog", "heading", "nav_status"):
            last[field] = record[field]
        if record.get("destination"):
            last["destination"] = record["destination"]
        action = "updated"
    else:
        track.append(record)
        action = "appended"

    TRACK_FILE.write_text(json.dumps(track, indent=2) + "\n")
    return action, len(track)


def main() -> int:
    api_key = load_api_key()
    record = asyncio.run(fetch_position(api_key))
    if record is None:
        print(f"No AIS position for MMSI {MMSI} within {WAIT_SECONDS}s — nothing written "
              "(no coverage this hour).")
        return 0
    action, n = save_record(record)
    sog = f"{record['sog']} kn" if record["sog"] is not None else "n/a"
    verb = "Logged new point" if action == "appended" else "Refreshed stationary point"
    print(f"{verb} ({n} total): {record['lat']:.5f}, {record['lon']:.5f} "
          f"(SOG {sog}) at {record['time']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
