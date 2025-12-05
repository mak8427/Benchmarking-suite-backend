"""Asynchronous load generator for exercising HTTP endpoints."""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, Tuple

import httpx


@dataclass(frozen=True)
class Settings:
    """Configuration for a load generation run."""

    url: str
    requests: int
    concurrency: int
    processes: int
    timeout: float
    use_http2: bool


def parse_args(argv: Iterable[str]) -> Settings:
    """Parse CLI arguments into a Settings instance."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Service base URL.")
    parser.add_argument(
        "requests",
        type=int,
        nargs="?",
        default=1_000_000,
        help="Total number of requests to issue.",
    )
    parser.add_argument(
        "concurrency",
        type=int,
        nargs="?",
        default=1_000,
        help="Total concurrency level across all processes.",
    )
    parser.add_argument(
        "processes",
        type=int,
        nargs="?",
        default=os.cpu_count() or 1,
        help="Number of worker processes to spawn.",
    )
    parser.add_argument(
        "timeout",
        type=float,
        nargs="?",
        default=2.0,
        help="Request timeout in seconds.",
    )
    parser.add_argument(
        "--http2",
        action="store_true",
        default=bool(int(os.environ.get("HTTP2", "0"))),
        help="Enable HTTP/2 for requests.",
    )
    args = parser.parse_args(list(argv))
    return Settings(
        url=args.url,
        requests=args.requests,
        concurrency=args.concurrency,
        processes=args.processes,
        timeout=args.timeout,
        use_http2=args.http2,
    )


def split_load(total_requests: int, parts: int) -> list[int]:
    """Return an even distribution of work units."""
    base, remainder = divmod(total_requests, parts)
    return [base + (1 if idx < remainder else 0) for idx in range(parts)]


async def bench_async(
    url: str,
    total_requests: int,
    concurrency: int,
    timeout_seconds: float,
    use_http2: bool,
) -> Tuple[int, int]:
    """Issue HTTP GET requests and capture success/failure counts."""
    succeeded = 0
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )
    timeout = httpx.Timeout(timeout_seconds)
    async with httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        http2=use_http2,
        trust_env=False,
    ) as client:
        lock = asyncio.Lock()
        iterator = iter(range(total_requests))

        async def worker() -> None:
            nonlocal succeeded
            while True:
                async with lock:
                    try:
                        next(iterator)
                    except StopIteration:
                        return
                try:
                    response = await client.get(url)
                    if response.status_code == 200:
                        succeeded += 1
                except httpx.HTTPError:
                    continue

        await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(concurrency)))
    return succeeded, total_requests - succeeded


def proc_worker(
    config: Settings,
    requests_per_proc: int,
    concurrency_per_proc: int,
    queue: mp.Queue,
) -> None:
    """Run the asynchronous benchmark inside a separate process."""
    succeeded, failed = asyncio.run(
        bench_async(
            config.url,
            requests_per_proc,
            concurrency_per_proc,
            config.timeout,
            config.use_http2,
        )
    )
    queue.put((succeeded, failed))


def main(argv: Iterable[str] | None = None) -> None:
    """Entry point for the CLI."""
    args = list(argv if argv is not None else sys.argv[1:])
    settings = parse_args(args)

    processes = max(1, settings.processes)
    queue: mp.Queue = mp.Queue()

    requests_per_proc = split_load(settings.requests, processes)
    concurrency_per_proc = max(1, settings.concurrency // processes)

    start = time.time()
    workers = []
    for idx in range(processes):
        process = mp.Process(
            target=proc_worker,
            args=(settings, requests_per_proc[idx], concurrency_per_proc, queue),
        )
        process.start()
        workers.append(process)

    succeeded = failed = 0
    for _ in workers:
        ok, err = queue.get()
        succeeded += ok
        failed += err

    for process in workers:
        process.join()

    duration = time.time() - start
    rate = settings.requests / duration if duration else 0.0
    print(
        f"done: {settings.requests} in {duration:.2f}s, "
        f"rps={rate:.1f}, ok={succeeded}, fail={failed}, "
        f"procs={processes}, conc/proc={concurrency_per_proc}"
    )


if __name__ == "__main__":
    main()
