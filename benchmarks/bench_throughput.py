import asyncio
import time
import pyaio_faq

async def bench_throughput(n=50000):
    start = time.perf_counter()
    for _ in range(n):
        await pyaio_faq.ready_inline(1)
    dur = time.perf_counter() - start
    print(f"Inline ops/s: {n / dur:,.0f} ({dur * 1000:.2f} ms total)")

async def bench_sleep(n=1000):
    start = time.perf_counter()
    tasks = [pyaio_faq.sleep_ret(i, 1) for i in range(n)]
    await asyncio.gather(*tasks)
    dur = time.perf_counter() - start
    print(f"Concurrent sleeps ({n}): {dur * 1000:.2f} ms")

if __name__ == "__main__":
    asyncio.run(bench_throughput())
    asyncio.run(bench_sleep())
