"""Parts 1 & 2: queued tasks and concurrent jobs."""
import argparse
import time
import ray

@ray.remote(num_cpus=1)
def work(index, seconds):
    import os
    start = time.time()
    time.sleep(seconds)
    return {"task": index, "pid": os.getpid(), "started": round(start, 2)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=24)
    parser.add_argument("--seconds", type=float, default=15)
    args = parser.parse_args()
    ray.init(address="auto")
    print("JOB ID:", ray.get_runtime_context().get_job_id(), flush=True)
    print("CLUSTER CPUs:", ray.cluster_resources().get("CPU", 0), flush=True)
    refs = [work.remote(i, args.seconds) for i in range(args.tasks)]
    print("Tasks submitted; inspect Dashboard > Tasks now", flush=True)
    print("Results:", ray.get(refs), flush=True)
    ray.shutdown()
