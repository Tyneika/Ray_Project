"""Part 5: actor restarts lose in-memory state."""
import os
import time
import ray

@ray.remote(max_restarts=1, max_task_retries=2)
class Counter:
    def __init__(self):
        self.value = 0
        self.pid = os.getpid()
        print("Actor initialized with counter=0, pid=", self.pid, flush=True)

    def increment(self):
        self.value += 1
        return self.value, self.pid

    def crash(self):
        print("Deliberately crashing actor pid=", self.pid, flush=True)
        os._exit(1)

if __name__ == "__main__":
    ray.init(address="auto")
    actor = Counter.remote()
    print("Before crash:", ray.get(actor.increment.remote()), flush=True)
    print("Before crash:", ray.get(actor.increment.remote()), flush=True)
    try:
        ray.get(actor.crash.remote())
    except Exception as exc:
        print("Expected crash:", type(exc).__name__, str(exc)[:300], flush=True)
    time.sleep(2)
    print("After restart (counter reset):", ray.get(actor.increment.remote()), flush=True)
    ray.shutdown()
