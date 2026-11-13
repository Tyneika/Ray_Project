"""Part 8: by-value vs. shared reference, plus put/get bandwidth."""
import argparse
import time
import numpy as np
import ray

@ray.remote
def consume(array):
    return float(array[0]) + float(array[-1])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mb", type=int, default=20)
    parser.add_argument("--sizes", nargs="+", type=int, default=[1, 10, 50, 100, 200, 400])
    args = parser.parse_args()
    ray.init(address="auto")
    ray.get(consume.remote(np.ones(128)))
    arr = np.ones(args.mb * 1024 * 1024 // 8, dtype=np.float64)
    t = time.perf_counter()
    ray.get([consume.remote(arr) for _ in range(20)])
    by_value = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    ref = ray.put(arr)
    ray.get([consume.remote(ref) for _ in range(20)])
    by_ref = (time.perf_counter() - t) * 1000
    print(f"By value: {by_value:.2f} ms")
    print(f"By reference (including ray.put): {by_ref:.2f} ms")
    print(f"Reference speed ratio: {by_value / by_ref:.2f}x")
    print("size_MiB,put_ms,get_ms,put_MiB_per_s")
    for mb in args.sizes:
        data = np.ones(mb * 1024 * 1024 // 8, dtype=np.float64)
        t = time.perf_counter()
        obj = ray.put(data)
        put = time.perf_counter() - t
        t = time.perf_counter()
        result = ray.get(obj)
        get = time.perf_counter() - t
        print(f"{mb},{put*1000:.2f},{get*1000:.2f},{mb/put:.2f}", flush=True)
        del data, result, obj
    ray.shutdown()
