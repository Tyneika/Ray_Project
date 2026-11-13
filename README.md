# COSC 4333 — Building and Measuring a Ray Cluster

These are **original implementations** based on the assignment's descriptions, not the instructor's original classroom scripts. Run on Ubuntu with Ray, NumPy, and Matplotlib installed.

## Start Ray

```bash
source ~/ray-env/bin/activate
ray stop
ray start --head --dashboard-host=0.0.0.0
```

Open `http://localhost:8265` in Ubuntu. Run from this project directory.

## Commands

```bash
python 1_task.py
# In two separate terminals at the same time:
python 1_task.py --seconds 30
python 1_task.py --seconds 30
python 3_task.py
python 3_actor.py
python 4_objstore.py
python 5_fault.py
python 6_overhead.py
python 6_breakeven.py
python 7_strongscaling.py --counts 1 2 4
python 8_objstore_move.py
python 9_graph.py
```

### Object spilling experiment

The assignment proposes `--object-store-memory=2000000000` (2 GB), which may be **too large for a 4 GB VM**. Stop Ray and restart with an object store sized appropriately to your available memory, for example:

```bash
ray stop
ray start --head --object-store-memory=500000000 --dashboard-host=0.0.0.0
python 4_objstore.py --objects 20 --mb 100 --hold 90
```

In another terminal, run `ray memory` and inspect dashboard/logs for spill metrics. Spill metrics are not guaranteed to appear in `ray memory` on all Ray versions; check the Ray dashboard metrics/logs too. Ray spilling also depends on available disk and memory configuration. Afterward restart Ray normally.

### Scaling caveat

`7_strongscaling.py` uses batches of in-flight tasks to **limit effective concurrency** while retaining one running cluster. The assignment explicitly asks you to **restart the cluster with each CPU count**. For strict compliance, restart Ray separately for each CPU count with `ray start --head --num-cpus=N --dashboard-host=0.0.0.0`, and run the script with `--counts N` for each run; compare the resulting elapsed times manually. The built-in graph represents concurrency-limited scaling, not literal per-count cluster restarts.

### Measurements and screenshots

Capture real output from your Ubuntu VM; do not fabricate data. Take screenshots of the Dashboard Tasks/Jobs/Actors views, object store, fault crash/restart, timing outputs, CSV, and scaling graph. Ray scheduling, hardware, and memory limits can change the numbers substantially.

### Important

Use a cluster with at least 2 CPU tokens to see parallel branches in `9_graph.py`. For a VM with fewer than 4 CPU tokens, modify `--counts` accordingly. The 400 MiB object test can exhaust RAM; use `--sizes 1 10 50 100` on small VMs. The `ray.get` timing of a local NumPy array may be small because Ray can share memory without copying.
