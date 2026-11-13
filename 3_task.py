"""Part 3: stateless Ray tasks."""
import ray

@ray.remote
def add(value):
    return value + 1

if __name__ == "__main__":
    ray.init(address="auto")
    values = [ray.get(add.remote(0)) for _ in range(8)]
    print("Stateless task results (each starts at zero):", values)
    ray.shutdown()
