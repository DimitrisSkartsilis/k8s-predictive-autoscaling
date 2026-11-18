import argparse
import asyncio
import math
import time

import httpx

def target_rps(elapsed: float, base: float, amplitude: float, period: float) -> float:
    """Requests per second at this moment: base plus a smooth wave."""
    wave = (1 - math.cos(2 * math.pi * elapsed / period)) / 2   # goes 0 → 1 → 0 over one period
    return base + amplitude * wave

def spike_rps(elapsed: float, every: float, length: float, height: float) -> float:
    """Extra requests per second during a spike; 0 outside spikes."""
    if height == 0 or elapsed < every:          # no spikes configured, or before the first one
        return 0
    in_spike = (elapsed % every) < length       # the first `length` seconds of each interval
    return height if in_spike else 0

async def send(client: httpx.AsyncClient, url: str, stats: dict) -> None:
    """Send one request and record whether it succeeded."""
    try:
        response = await client.get(url)
        stats["ok" if response.status_code == 200 else "failed"] += 1
    except httpx.HTTPError:
        stats["failed"] += 1


async def run(url: str, base: float, amplitude: float, period: float,
              spike_every: float, spike_length: float, spike_height: float,
              duration: float) -> None:
    # No keep-alive: every request opens a new connection, so the Service spreads them across pods.
    limits = httpx.Limits(max_keepalive_connections=0, max_connections=None)
    stats = {"sent": 0, "ok": 0, "failed": 0}
    in_flight: set[asyncio.Task] = set()

    async with httpx.AsyncClient(timeout=30, limits=limits) as client:
        start = time.monotonic()
        next_send = start
        last_report = start

        while time.monotonic() - start < duration:
            elapsed = time.monotonic() - start
            rate = (target_rps(elapsed, base, amplitude, period)
                    + spike_rps(elapsed, spike_every, spike_length, spike_height))

            # Fire the request without waiting for it: that's what makes it open-loop.
            task = asyncio.create_task(send(client, url, stats))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            stats["sent"] += 1

            # Print progress every 5 seconds.
            now = time.monotonic()
            if now - last_report >= 5:
                print(f"{now - start:6.0f}s  rate={rate:.2f}  sent={stats['sent']}  ok={stats['ok']}  "
                      f"failed={stats['failed']}  in_flight={len(in_flight)}")
                last_report = now

            # Schedule the next request 1/rate seconds after the previous one.
            next_send += 1 / rate
            await asyncio.sleep(max(0, next_send - time.monotonic()))
    print(f"done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Open-loop traffic generator")
    parser.add_argument("--url", default="http://localhost:8080/work")
    parser.add_argument("--base", type=float, default=2, help="minimum requests per second")
    parser.add_argument("--amplitude", type=float, default=0, help="how much the wave adds at its peak")
    parser.add_argument("--period", type=float, default=300, help="seconds for one full wave")
    parser.add_argument("--spike-every", type=float, default=240, help="seconds between spike starts")
    parser.add_argument("--spike-length", type=float, default=30, help="seconds each spike lasts")
    parser.add_argument("--spike-height", type=float, default=0, help="extra requests per second during a spike")
    parser.add_argument("--duration", type=float, default=60, help="seconds to run")
    args = parser.parse_args()
    asyncio.run(run(args.url, args.base, args.amplitude, args.period,
                args.spike_every, args.spike_length, args.spike_height, args.duration))