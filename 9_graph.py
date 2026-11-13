"""Part 9: two independent branches, three-stage critical path."""
import time
import ray

@ray.remote
def load(value):
    time.sleep(1)
    return value

@ray.remote
def square(value):
    time.sleep(1)
    return value * value

@ray.remote
def add(left, right):
    time.sleep(1)
    return left + right

if __name__ == "__main__":
    ray.init(address="auto")
    t = time.perf_counter()
    left = square.remote(load.remote(3))
    right = square.remote(load.remote(4))
    final = add.remote(left, right)
    result = ray.get(final)
    print("Result:", result)
    print(f"Elapsed time: {time.perf_counter()-t:.3f} seconds")
    print("With >=2 CPUs expect roughly 3 seconds, plus Ray overhead")
    ray.shutdown()
