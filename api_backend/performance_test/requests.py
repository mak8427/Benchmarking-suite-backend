"""Asynchronous load generator for exercising HTTP endpoints."""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Tuple

import httpx
import yaml


@dataclass(frozen=True)
class Settings:
    """Configuration for a load generation run."""

    url: str
    requests: int
    concurrency: int
    processes: int
    timeout: float
    use_http2: bool


def _load_config(path: Path) -> dict[str, Any]:
    """Load YAML configuration from disk.

    Args:
        path (Path): YAML config path.

    Returns:
        dict[str, Any]: Parsed mapping.

    Examples:
        >>> from pathlib import Path
        >>> p = Path('/tmp/nonexistent-config.yml')
        >>> isinstance(str(p), str)
        True
    """
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError("Config root must be a mapping.")
    return loaded


def parse_args(argv: Iterable[str]) -> Settings:
    """Parse CLI arguments into a Settings instance.

    Args:
        argv (Iterable[str]): Command-line arguments excluding executable name.

    Returns:
        Settings: Parsed runtime settings.

    Examples:
        >>> cfg = parse_args(["http://localhost:8000", "10", "2", "1", "1.5"])
        >>> (cfg.url, cfg.requests, cfg.concurrency)
        ('http://localhost:8000', 10, 2)
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Optional YAML config file. Keys: url, requests, concurrency, "
            "processes, timeout, http2."
        ),
    )
    parser.add_argument("url", nargs="?", help="Service base URL.")
    parser.add_argument("requests", type=int, nargs="?", help="Total number of requests.")
    parser.add_argument("concurrency", type=int, nargs="?", help="Total concurrency across workers.")
    parser.add_argument("processes", type=int, nargs="?", help="Worker process count.")
    parser.add_argument("timeout", type=float, nargs="?", help="Request timeout in seconds.")
    parser.add_argument(
        "--http2",
        action="store_true",
        default=None,
        help="Enable HTTP/2 for requests.",
    )
    args = parser.parse_args(list(argv))

    config: dict[str, Any] = {}
    if args.config:
        config = _load_config(args.config)

    url = args.url or config.get("url")
    if not url:
        raise ValueError("A target URL is required via positional arg or config file.")

    requests_total = args.requests or int(config.get("requests", 1_000_000))
    concurrency = args.concurrency or int(config.get("concurrency", 1_000))
    processes = args.processes or int(config.get("processes", os.cpu_count() or 1))
    timeout = args.timeout or float(config.get("timeout", 2.0))

    if args.http2 is None:
        use_http2 = bool(config.get("http2", bool(int(os.environ.get("HTTP2", "0")))))
    else:
        use_http2 = args.http2

    return Settings(
        url=url,
        requests=requests_total,
        concurrency=concurrency,
        processes=processes,
        timeout=timeout,
        use_http2=use_http2,
    )


def split_load(total_requests: int, parts: int) -> list[int]:
    """Return an even distribution of work units.

    Args:
        total_requests (int): Total request budget.
        parts (int): Number of buckets.

    Returns:
        list[int]: Request counts per bucket.

    Examples:
        >>> split_load(10, 3)
        [4, 3, 3]
    """
    base, remainder = divmod(total_requests, parts)
    return [base + (1 if idx < remainder else 0) for idx in range(parts)]


async def bench_async(
    url: str,
    total_requests: int,
    concurrency: int,
    timeout_seconds: float,
    use_http2: bool,
) -> Tuple[int, int]:
    """Issue HTTP GET requests and capture success/failure counts.

    Args:
        url (str): Target URL.
        total_requests (int): Number of requests to issue.
        concurrency (int): Concurrent worker coroutines.
        timeout_seconds (float): Request timeout.
        use_http2 (bool): Whether to use HTTP/2.

    Returns:
        Tuple[int, int]: Succeeded and failed request counts.

    Examples:
        >>> isinstance(bench_async.__name__, str)
        True
    """
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
            """Consume request budget and submit GET requests.

            Returns:
                None: Work is reported through outer-scope counters.

            Examples:
                >>> "worker" in worker.__name__
                True
            """
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
    """Run the asynchronous benchmark inside a separate process.

    Args:
        config (Settings): Global load settings.
        requests_per_proc (int): Worker request budget.
        concurrency_per_proc (int): Worker concurrency budget.
        queue (mp.Queue): Result queue.

    Returns:
        None: Writes process results into queue.

    Examples:
        >>> isinstance(Settings("u", 1, 1, 1, 1.0, False), Settings)
        True
    """
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
    """Entry point for the load-testing CLI.

    Args:
        argv (Iterable[str] | None): Optional argument list.

    Returns:
        None: Prints final throughput summary.

    Examples:
        >>> isinstance(main.__name__, str)
        True
    """
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
