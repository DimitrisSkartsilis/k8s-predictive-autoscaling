import argparse
import asyncio
import time

import httpx


async def send(client: httpx.AsyncClient, url: str, stats: dict) -> None:
    """Send one request and record whether it succeeded."""
    try:
        response = await client.get(url)
        stats["ok" if response.status_code == 200 else "failed"] += 1
    except httpx.HTTPError:
        stats["failed"] += 1


async def run(url: str, rps: float, duration: float) -> None:
    # No keep-alive: every request opens a new connection, so the Service spreads them across pods.
    limits = httpx.Limits(max_keepalive_connections=0, max_connections=None)
    stats = {"sent": 0, "ok": 0, "failed": 0}
    in_flight: set[asyncio.Task] = set()

    async with httpx.AsyncClient(timeout=30, limits=limits) as client:
        start = time.monotonic()
        next_send = start
        last_report = start

        while time.monotonic() - start < duration:
            # Fire the request without waiting for it: that's what makes it open-loop.
            task = asyncio.create_task(send(client, url, stats))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            stats["sent"] += 1

            # Print progress every 5 seconds.
            now = time.monotonic()
            if now - last_report >= 5:
                print(f"{now - start:6.0f}s  sent={stats['sent']}  ok={stats['ok']}  "
                      f"failed={stats['failed']}  in_flight={len(in_flight)}")
                last_report = now

            # Schedule the next request exactly 1/rps seconds after the previous one.
            next_send += 1 / rps
            await asyncio.sleep(max(0, next_send - time.monotonic()))

        await asyncio.gather(*in_flight)   # let the last requests finish
    print(f"done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Open-loop traffic generator")
    parser.add_argument("--url", default="http://localhost:8080/work")
    parser.add_argument("--rps", type=float, default=2, help="requests per second")
    parser.add_argument("--duration", type=float, default=60, help="seconds to run")
    args = parser.parse_args()
    asyncio.run(run(args.url, args.rps, args.duration))