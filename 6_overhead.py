"""Part 6a: compare Python function calls to Ray tasks."""
import argparse
import time
import ray

def plain():
    return 1

@ray.remote
def remote_empty():
    return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=1000)
    args = parser.parse_args()
    ray.init(address="auto")
    ray.get(remote_empty.remote())  # warm up
    t = time.perf_counter()
    for _ in range(args.n):
        plain()
    python_us = (time.perf_counter() - t) * 1e6 / args.n
    t = time.perf_counter()
    ray.get([remote_empty.remote() for _ in range(args.n)])
    ray_us = (time.perf_counter() - t) * 1e6 / args.n
    print(f"Plain Python call: {python_us:.3f} us")
    print(f"Ray task (batched parallel throughput): {ray_us:.3f} us")
    print(f"Derived overhead: {ray_us - python_us:.3f} us/task")
    ray.shutdown()
