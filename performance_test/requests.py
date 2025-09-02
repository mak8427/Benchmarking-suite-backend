import sys, os, time, asyncio, httpx, multiprocessing as mp

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7800/"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000
TOTAL_CONCURRENCY = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
PROCESSES = int(sys.argv[4]) if len(sys.argv) > 4 else (os.cpu_count() or 1)
TIMEOUT_S = float(sys.argv[5]) if len(sys.argv) > 5 else 2.0
HTTP2 = bool(int(os.environ.get("HTTP2", "0")))

def split_load(n: int, parts: int):
    base, rem = divmod(n, parts)
    return [base + (1 if i < rem else 0) for i in range(parts)]

async def bench_async(url: str, n: int, concurrency: int, timeout_s: float, http2: bool):
    ok = 0
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(timeout_s)
    async with httpx.AsyncClient(limits=limits, timeout=timeout, http2=http2, trust_env=False) as client:
        lock = asyncio.Lock()
        it = iter(range(n))

        async def worker():
            nonlocal ok
            while True:
                async with lock:
                    try:
                        next(it)
                    except StopIteration:
                        return
                try:
                    r = await client.get(url)
                    if r.status_code == 200:
                        ok += 1
                except Exception:
                    pass

        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await asyncio.gather(*tasks)
    return ok, n - ok

def proc_worker(url: str, n: int, concurrency: int, timeout_s: float, http2: bool, q: mp.Queue):
    ok, fail = asyncio.run(bench_async(url, n, concurrency, timeout_s, http2))
    q.put((ok, fail))

def main():
    procs = PROCESSES if PROCESSES > 0 else 1
    conc_per_proc = max(1, TOTAL_CONCURRENCY // procs)
    n_per_proc = split_load(N, procs)

    q = mp.Queue()
    ps = []
    start = time.time()
    for i in range(procs):
        p = mp.Process(target=proc_worker, args=(URL, n_per_proc[i], conc_per_proc, TIMEOUT_S, HTTP2, q))
        p.start()
        ps.append(p)

    ok = fail = 0
    for _ in ps:
        o, f = q.get()
        ok += o
        fail += f
    for p in ps:
        p.join()

    dur = time.time() - start
    print(f"done: {N} in {dur:.2f}s, rps={N/dur:.1f}, ok={ok}, fail={fail}, procs={procs}, conc/proc={conc_per_proc}")

if __name__ == "__main__":
    main()