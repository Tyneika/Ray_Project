"""Part 3: persistent stateful actors."""
import time
import ray

@ray.remote(num_cpus=1)
class Accumulator:
    def __init__(self):
        import os
        self.total = 0
        self.pid = os.getpid()

    def add(self, amount):
        self.total += amount
        return self.total

    def info(self):
        return {"pid": self.pid, "total": self.total}

if __name__ == "__main__":
    ray.init(address="auto")
    count = max(1, min(4, int(ray.cluster_resources().get("CPU", 1))))
    actors = [Accumulator.remote() for _ in range(count)]
    for i in range(12):
        ray.get(actors[i % count].add.remote(i + 1))
    print("Actor partial totals:", ray.get([a.info.remote() for a in actors]), flush=True)
    print("Actors remain alive for 30 seconds: screenshot Actors tab", flush=True)
    time.sleep(30)
    ray.shutdown()
