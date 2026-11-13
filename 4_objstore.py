"""Part 4: fill object store; observe spilling and ray memory."""
import argparse
import time
import numpy as np
import ray

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--objects", type=int, default=30)
    parser.add_argument("--mb", type=int, default=100)
    parser.add_argument("--hold", type=int, default=60)
    args = parser.parse_args()
    ray.init(address="auto")
    refs = []
    for i in range(args.objects):
        array = np.full(args.mb * 1024 * 1024 // 8, i, dtype=np.float64)
        refs.append(ray.put(array))
        print(f"Stored {i + 1}/{args.objects}: approximately {(i + 1) * args.mb} MiB", flush=True)
    print("Objects pinned; inspect ray memory and Ray logs during hold", flush=True)
    time.sleep(args.hold)
    print("References retained:", len(refs))
    ray.shutdown()
