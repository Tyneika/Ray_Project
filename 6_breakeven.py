"""Part 6b: sweep simulated CPU work from 10 us to 100 ms."""
import time
import ray

DURATIONS_US = [10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000]
N = 64

def busy_work(us):
    end = time.perf_counter() + us / 1e6
    x = 0
    while time.perf_counter() < end:
        x += 1
    return x

@ray.remote
def remote_work(us):
    return busy_work(us)

if __name__ == "__main__":
    ray.init(address="auto")
    ray.get(remote_work.remote(1000))
    print("work_us,serial_s,ray_s,speedup")
    crossover = None
    for us in DURATIONS_US:
        t = time.perf_counter()
        [busy_work(us) for _ in range(N)]
        serial = time.perf_counter() - t
        t = time.perf_counter()
        ray.get([remote_work.remote(us) for _ in range(N)])
        parallel = time.perf_counter() - t
        speedup = serial / parallel
        print(f"{us},{serial:.6f},{parallel:.6f},{speedup:.3f}", flush=True)
        if crossover is None and speedup > 1:
            crossover = us
    print("First sampled break-even work size (us):", crossover)
    ray.shutdown()
