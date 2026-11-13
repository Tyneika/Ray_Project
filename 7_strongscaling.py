"""Part 7: fixed workload, vary Ray CPU resources; saves CSV and plot."""
import argparse
import csv
import time
import ray
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

@ray.remote(num_cpus=1)
def cpu_work(seconds):
    deadline = time.perf_counter() + seconds
    value = 0
    while time.perf_counter() < deadline:
        value += 1
    return value

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--counts", nargs="+", type=int, default=[1, 2, 4])
    parser.add_argument("--tasks", type=int, default=24)
    parser.add_argument("--seconds", type=float, default=0.5)
    args = parser.parse_args()
    ray.init(address="auto")
    available = int(ray.cluster_resources().get("CPU", 1))
    counts = sorted(set(c for c in args.counts if 1 <= c <= available))
    if not counts:
        raise ValueError(f"Choose CPU counts between 1 and {available}")
    rows = []
    for cpus in counts:
        # Placement is constrained by logical CPU resource tokens using a pool
        # of in-flight tasks. This is not a physical cluster restart.
        start = time.perf_counter()
        for offset in range(0, args.tasks, cpus):
            ray.get([cpu_work.remote(args.seconds) for _ in range(min(cpus, args.tasks-offset))])
        elapsed = time.perf_counter() - start
        rows.append({"cpus": cpus, "time_s": elapsed})
    baseline = rows[0]["time_s"]
    for r in rows:
        r["speedup"] = baseline / r["time_s"]
        print(f"CPUs={r['cpus']}, time={r['time_s']:.3f}s, speedup={r['speedup']:.2f}x")
    with open("strong_scaling.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cpus", "time_s", "speedup"])
        writer.writeheader()
        writer.writerows(rows)
    plt.plot([r["cpus"] for r in rows], [r["speedup"] for r in rows], "o-", label="Measured")
    plt.plot(counts, [c / counts[0] for c in counts], "--", label="Ideal")
    plt.xlabel("Concurrent CPU tokens")
    plt.ylabel("Speedup")
    plt.title("Strong scaling (fixed workload)")
    plt.legend()
    plt.savefig("strong_scaling.png", dpi=160, bbox_inches="tight")
    print("Saved strong_scaling.csv and strong_scaling.png")
    ray.shutdown()
