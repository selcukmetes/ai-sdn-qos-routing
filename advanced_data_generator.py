#!/usr/bin/env python3
"""
advanced_data_generator.py
===========================
High-volume dataset generator for an ML-based (XGBoost) SDN QoS anomaly
detector, built for Mininet + Open vSwitch + Ryu.

WHAT IT DOES
------------
Runs on Mininet host h1. It:
  1. Spins up two background threads that continuously push dynamic, bursty
     UDP traffic (via classic `iperf`) into the MEDIUM (TOS 32) and LOW
     (TOS 48) queues *simultaneously*, wandering across 1-100 Mbps with
     random step sizes, random phase durations, and short-term jitter.
  2. In the main loop, continuously fires `ping` probes tagged with each
     class's TOS/DSCP value (VIP=16, MEDIUM=32, LOW=48) to measure average
     RTT and packet loss for all three classes, in parallel.
  3. Labels every measurement `is_anomaly` (0/1) using per-class thresholds.
  4. Streams everything to a CSV, flushed after every cycle, so no data is
     lost even if the run is stopped early.

PREREQUISITES (please read before running)
--------------------------------------------
1. The Mininet topology must already be up, with OVS QoS queues configured
   on the bottleneck link:
       Queue 0 = VIP    -> 70 Mbps   (TOS 16)
       Queue 1 = MEDIUM -> 20 Mbps   (TOS 32)
       Queue 2 = LOW    -> 10 Mbps   (TOS 48)
   and Ryu classifying flows into those queues by TOS/DSCP.
2. A CLASSIC iperf (v2.x -- the `iperf` binary, NOT iperf3) UDP server must
   already be running on the target host (default 10.0.0.2), e.g. on h2:
       iperf -s -u -i 1 &
   Classic iperf is required because its server happily accepts multiple
   *simultaneous* client streams; iperf3's server only runs one test at a
   time and would silently break the "MEDIUM + LOW simultaneously" part of
   this experiment.
3. Run this script from/inside h1, e.g. from the Mininet CLI:
       mininet> h1 python3 advanced_data_generator.py &
   or from an `xterm h1` shell.

USAGE
-----
    python3 advanced_data_generator.py [--duration MIN] [--output PATH]
                                        [--target-ip IP] [--path-label LABEL]
                                        [--seed N]

    --duration     Total run time in minutes (default: 180). Use 0 to run
                   until Ctrl+C.
    --output       Output CSV path (default: qos_anomaly_dataset.csv).
    --target-ip    Target server IP (default: 10.0.0.2).
    --path-label   Label recorded in the 'path_label' column (default: 11ms).
    --seed         Random seed, for reproducible traffic patterns.

Press Ctrl+C (or send SIGTERM) at any time -- all iperf child processes are
terminated and reaped (no zombies), the CSV is flushed and closed, and a
summary is printed before exit. Requires Python 3.6+ (f-strings).
"""

import argparse
import csv
import datetime
import math
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Tuple

# =====================================================================
# CONFIGURATION -- tune these to change traffic/experiment behaviour
# =====================================================================

TARGET_SERVER_IP = "10.0.0.2"      # default target (h2), overridable via --target-ip
PATH_LABEL = "11ms"                 # default path label, overridable via --path-label
IPERF_PORT = 5001                   # must match the iperf server's listening port

# QoS class name -> TOS/DSCP decimal value, used for BOTH ping -Q and
# iperf -S. Kept as a single source of truth so probes and background
# traffic always agree on how each class is marked.
QOS_TOS = {
    "VIP": 16,      # Queue 0, 70 Mbps  -- reference only, configured on the switch
    "MEDIUM": 32,   # Queue 1, 20 Mbps  -- reference only, configured on the switch
    "LOW": 48,      # Queue 2, 10 Mbps  -- reference only, configured on the switch
}

# Per-class anomaly thresholds, exactly as specified.
ANOMALY_THRESHOLDS = {
    "VIP":    {"rtt_ms": 20,  "loss_pct": 1},
    "MEDIUM": {"rtt_ms": 100, "loss_pct": 5},
    "LOW":    {"rtt_ms": 500, "loss_pct": 10},
}

# Only MEDIUM and LOW get dedicated background traffic generators; VIP is
# left clean and only probed, so its measurements show how well the QoS
# scheme protects it while the other two classes are under load.
BACKGROUND_TRAFFIC_CLASSES = ["MEDIUM", "LOW"]

# --- Traffic-shape randomisation -----------------------------------------
# NOTE: MEDIUM and LOW each independently random-walk across the FULL
# 1-100 Mbps range, so their *combined* instantaneous load can occasionally
# reach ~200 Mbps even though the bottleneck's total physical capacity is
# only ~100 Mbps (70+20+10 across the three queues). This is intentional:
# it guarantees the dataset contains enough genuinely congested/anomalous
# samples -- including occasional VIP degradation -- for a classifier to
# actually learn from, instead of being 99% "normal" traffic. Narrow
# MIN/MAX_BW_MBPS if you want a gentler load profile.
MIN_BW_MBPS = 1.0
MAX_BW_MBPS = 100.0
MACRO_STEP_MAX = 15.0        # max random-walk jump between macro phases (Mbps)
MACRO_DURATION_MIN = 20.0    # macro phase length, seconds
MACRO_DURATION_MAX = 90.0
MICRO_JITTER_PCT = 0.20      # +/-20% burst jitter applied within a macro phase
MICRO_DURATION_MIN = 2.0     # micro-burst length, seconds
MICRO_DURATION_MAX = 6.0

# --- Ping probe settings ---------------------------------------------------
PING_COUNT = 5                  # packets per probe; more = more stable avg, slower
PING_INTERVAL_SEC = 0.2         # 0.2s is the minimum interval unprivileged users
                                 # can set; if this script runs as root you can
                                 # lower it (e.g. 0.05) for faster data collection
PING_WAIT_SEC = 2                # -W: per-packet reply timeout (comfortably above
                                  # the 500 ms LOW RTT anomaly threshold)
PING_SUBPROCESS_TIMEOUT = 15     # hard safety cap on the whole subprocess call

# --- Output -----------------------------------------------------------------
CSV_OUTPUT_PATH = "qos_anomaly_dataset.csv"
CSV_COLUMNS = [
    "timestamp", "path_label", "target_load_mbps",
    "qos_class", "avg_rtt_ms", "packet_loss_pct", "is_anomaly",
]

PRINT_EVERY_N_CYCLES = 1   # raise this to reduce console spam on long runs

# =====================================================================

stop_event = threading.Event()

PING_LOSS_REGEX = re.compile(r"(\d+(?:\.\d+)?)\s*%\s*packet loss")
PING_RTT_REGEX = re.compile(r"=\s*[\d.]+/([\d.]+)/[\d.]+/[\d.]+\s*ms")


class SharedLoadState:
    """Thread-safe record of the instantaneous background bandwidth each
    traffic-generator thread is currently pushing. The main measurement
    loop reads this to know what load was active when a probe was taken."""

    def __init__(self, classes):
        self._lock = threading.Lock()
        self._loads: Dict[str, float] = {c: 0.0 for c in classes}

    def set_load(self, qos_class: str, mbps: float) -> None:
        with self._lock:
            self._loads[qos_class] = mbps

    def get_load(self, qos_class: str) -> float:
        with self._lock:
            return self._loads.get(qos_class, 0.0)

    def total_load(self) -> float:
        with self._lock:
            return round(sum(self._loads.values()), 2)


def interruptible_sleep(duration: float, evt: threading.Event, step: float = 0.5) -> None:
    """Sleep for `duration` seconds but wake up within `step` seconds if
    `evt` gets set, so shutdown (Ctrl+C / SIGTERM) stays responsive even
    while a long micro-burst is 'in flight'."""
    deadline = time.time() + duration
    while not evt.is_set():
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        time.sleep(min(step, remaining))


def start_iperf_udp(target_ip: str, tos_value: int, bandwidth_mbps: float,
                     duration_sec: float) -> subprocess.Popen:
    """Launch one short-lived classic-iperf UDP client process tagged with
    the given TOS/DSCP value. stdout/stderr go to DEVNULL: we don't need
    iperf's own reports (ping supplies our measurements), and discarding
    them avoids the classic hang-on-a-full-pipe subprocess bug."""
    duration_int = max(1, int(round(duration_sec)))
    cmd = [
        "iperf", "-c", target_ip, "-u",
        "-b", f"{bandwidth_mbps:.2f}M",
        "-t", str(duration_int),
        "-S", str(tos_value),
        "-p", str(IPERF_PORT),
    ]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # own process group: isolates it from signals
    )                            # sent to the parent's terminal foreground group


def stop_iperf(proc: Optional[subprocess.Popen]) -> None:
    """Terminate an iperf process and ALWAYS reap it -- whether it was still
    running (needs terminate/kill) or had already finished on its own
    (just needs wait()) -- so no zombie processes are ever left behind."""
    if proc is None:
        return
    if proc.poll() is None:          # still running -> ask nicely, then force
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    else:
        proc.wait()                  # already exited; reap to avoid a zombie


def traffic_generator_worker(qos_class: str, tos_value: int, target_ip: str,
                              shared_state: SharedLoadState,
                              evt: threading.Event) -> None:
    """
    Continuously generate dynamic, bursty UDP background load for ONE QoS
    class. Two timescales create the "realistic bursty traffic" requested:

      MACRO phase    - a longer-lived bandwidth "trend", moved by a random
                        step each phase so load drifts up/down over minutes
                        instead of jumping to unrelated values constantly.
      MICRO interval - within each macro phase, bandwidth is perturbed by a
                        small random +/- percentage every few seconds, with
                        a fresh short-lived iperf process per micro-burst.
    """
    macro_bw = random.uniform(MIN_BW_MBPS, MAX_BW_MBPS)
    proc: Optional[subprocess.Popen] = None
    try:
        while not evt.is_set():
            # ---- MACRO: random-walk step across the full 1-100 Mbps range ----
            step = random.uniform(-MACRO_STEP_MAX, MACRO_STEP_MAX)
            macro_bw = min(MAX_BW_MBPS, max(MIN_BW_MBPS, macro_bw + step))
            macro_duration = random.uniform(MACRO_DURATION_MIN, MACRO_DURATION_MAX)
            phase_deadline = time.time() + macro_duration

            # ---- MICRO: fluctuate around macro_bw until the phase ends ----
            while not evt.is_set() and time.time() < phase_deadline:
                jitter = random.uniform(-MICRO_JITTER_PCT, MICRO_JITTER_PCT)
                micro_bw = min(MAX_BW_MBPS, max(MIN_BW_MBPS, macro_bw * (1 + jitter)))
                remaining = phase_deadline - time.time()
                micro_duration = min(
                    random.uniform(MICRO_DURATION_MIN, MICRO_DURATION_MAX), remaining
                )
                if micro_duration < 1.0:
                    break  # not worth spinning up a process for < 1s

                shared_state.set_load(qos_class, round(micro_bw, 2))
                proc = start_iperf_udp(target_ip, tos_value, micro_bw, micro_duration)
                interruptible_sleep(micro_duration, evt)
                stop_iperf(proc)
                proc = None
    finally:
        stop_iperf(proc)                  # safety net on the way out
        shared_state.set_load(qos_class, 0.0)


def parse_ping_output(output: str) -> Tuple[float, float]:
    """Extract (avg_rtt_ms, packet_loss_pct) from `ping` stdout. avg_rtt_ms
    is NaN when every packet was lost (no rtt summary line is printed)."""
    loss_match = PING_LOSS_REGEX.search(output)
    packet_loss_pct = float(loss_match.group(1)) if loss_match else 100.0
    rtt_match = PING_RTT_REGEX.search(output)
    avg_rtt_ms = float(rtt_match.group(1)) if rtt_match else float("nan")
    return avg_rtt_ms, packet_loss_pct


def run_ping_probe(qos_class: str, tos_value: int, target_ip: str) -> Tuple[float, float]:
    """Run one `ping` probe tagged with the class's TOS/DSCP value and
    return (avg_rtt_ms, packet_loss_pct). Any failure (timeout, exception)
    is treated as a total outage -- 100% loss / NaN RTT -- which correctly
    flows into the anomaly logic as an anomalous sample."""
    cmd = [
        "ping", "-Q", str(tos_value),
        "-c", str(PING_COUNT),
        "-i", str(PING_INTERVAL_SEC),
        "-W", str(PING_WAIT_SEC),
        target_ip,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=PING_SUBPROCESS_TIMEOUT
        )
        return parse_ping_output(result.stdout)
    except subprocess.TimeoutExpired:
        print(f"[!] {qos_class} ping probe timed out; recording as outage.")
        return float("nan"), 100.0
    except Exception as exc:  # defensive: never let one bad probe kill the run
        print(f"[!] {qos_class} ping probe failed ({exc}); recording as outage.")
        return float("nan"), 100.0


def compute_is_anomaly(qos_class: str, avg_rtt_ms: float, packet_loss_pct: float) -> int:
    """Apply the exact per-class thresholds. A NaN RTT (zero replies
    received) is always anomalous -- it means the class was unreachable."""
    thresholds = ANOMALY_THRESHOLDS[qos_class]
    rtt_bad = math.isnan(avg_rtt_ms) or avg_rtt_ms > thresholds["rtt_ms"]
    loss_bad = packet_loss_pct > thresholds["loss_pct"]
    return 1 if (rtt_bad or loss_bad) else 0


def init_csv(path: str):
    """Open the CSV for continuous appending. The header is written only
    if the file is new/empty, so re-running the script safely resumes or
    extends a dataset instead of duplicating headers or losing old data."""
    is_new = not (os.path.isfile(path) and os.path.getsize(path) > 0)
    f = open(path, "a", newline="")
    writer = csv.writer(f)
    if is_new:
        writer.writerow(CSV_COLUMNS)
        f.flush()
    return f, writer


def check_dependencies() -> None:
    """Fail fast with a clear message if a required external tool is
    missing, rather than dying deep inside a background thread later."""
    missing = []
    if shutil.which("ping") is None:
        missing.append("ping")
    if shutil.which("iperf") is None:
        missing.append("iperf")
    if missing:
        sys.exit(
            "[!] ERROR: missing required tool(s): "
            + ", ".join(missing)
            + ".\n    Install classic iperf (v2.x, NOT iperf3) with: "
              "sudo apt-get install iperf"
        )


def preflight_connectivity_check(target_ip: str) -> None:
    """Quick sanity ping before committing to a long data-collection run."""
    print(f"[*] Pre-flight: checking connectivity to {target_ip} ...")
    try:
        result = subprocess.run(
            ["ping", "-c", "2", "-W", "2", target_ip],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            print(
                f"[!] WARNING: {target_ip} did not respond to a plain ping.\n"
                f"    Check that Mininet/OVS/Ryu are up and that a classic\n"
                f"    iperf UDP server is running on the target, e.g. on h2:\n"
                f"    `iperf -s -u -i 1 &`.  Continuing in 5s (Ctrl+C to abort)..."
            )
            time.sleep(5)
        else:
            print("[*] Target is reachable.\n")
    except Exception as exc:
        print(f"[!] Pre-flight check raised an error ({exc}); continuing anyway.\n")


def handle_shutdown_signal(signum, frame) -> None:
    print(f"\n[*] Caught signal {signum}; shutting down gracefully "
          f"(stopping iperf, closing CSV)...")
    stop_event.set()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a large QoS anomaly-detection dataset from Mininet host h1."
    )
    parser.add_argument("--duration", type=float, default=180.0,
                         help="Total run time in MINUTES (default: 180). Use 0 for unlimited (Ctrl+C to stop).")
    parser.add_argument("--output", type=str, default=CSV_OUTPUT_PATH,
                         help=f"Output CSV path (default: {CSV_OUTPUT_PATH}).")
    parser.add_argument("--target-ip", type=str, default=TARGET_SERVER_IP,
                         help=f"Target server IP (default: {TARGET_SERVER_IP}).")
    parser.add_argument("--path-label", type=str, default=PATH_LABEL,
                         help=f"Value recorded in 'path_label' column (default: {PATH_LABEL}).")
    parser.add_argument("--seed", type=int, default=None,
                         help="Random seed for reproducible traffic patterns (default: fully random).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    check_dependencies()
    preflight_connectivity_check(args.target_ip)

    print("=" * 70)
    print(f"  Target server   : {args.target_ip}")
    print(f"  Path label      : {args.path_label}")
    print(f"  Output CSV      : {args.output}")
    print(f"  Duration        : {'unlimited (Ctrl+C to stop)' if args.duration <= 0 else f'{args.duration:.0f} min'}")
    print(f"  QoS classes     : {QOS_TOS}")
    print("=" * 70 + "\n")

    shared_state = SharedLoadState(BACKGROUND_TRAFFIC_CLASSES)
    generator_threads = []
    for cls in BACKGROUND_TRAFFIC_CLASSES:
        t = threading.Thread(
            target=traffic_generator_worker,
            args=(cls, QOS_TOS[cls], args.target_ip, shared_state, stop_event),
            daemon=True,
            name=f"traffic-{cls}",
        )
        t.start()
        generator_threads.append(t)

    csv_file, writer = init_csv(args.output)
    row_count = 0
    anomaly_count = 0
    cycle_count = 0
    end_time = None if args.duration <= 0 else time.time() + args.duration * 60

    try:
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="ping") as executor:
            while not stop_event.is_set() and (end_time is None or time.time() < end_time):
                futures = {
                    executor.submit(run_ping_probe, cls, tos, args.target_ip): cls
                    for cls, tos in QOS_TOS.items()
                }
                results: Dict[str, Tuple[float, float]] = {}
                for fut in futures:
                    results[futures[fut]] = fut.result()

                timestamp = datetime.datetime.now().isoformat(timespec="seconds")
                cycle_count += 1

                # Design choice: target_load_mbps = TOTAL combined MEDIUM+LOW
                # background load at this instant, logged identically on all
                # 3 rows of the cycle. This gives the model one consistent
                # "how congested is the network right now" feature that's
                # meaningful even for VIP (which has no dedicated background
                # flow of its own). For a PER-QUEUE value instead (0 for VIP,
                # that class's own bandwidth for MEDIUM/LOW), replace
                # `total_load` below with `shared_state.get_load(cls)`.
                total_load = shared_state.total_load()

                log_parts = [f"[{timestamp}] load={total_load:6.2f}Mbps"]
                for cls in ("VIP", "MEDIUM", "LOW"):
                    avg_rtt, loss_pct = results[cls]
                    anomaly = compute_is_anomaly(cls, avg_rtt, loss_pct)
                    row_count += 1
                    anomaly_count += anomaly

                    writer.writerow([
                        timestamp,
                        args.path_label,
                        total_load,
                        cls,
                        "" if math.isnan(avg_rtt) else round(avg_rtt, 3),
                        round(loss_pct, 2),
                        anomaly,
                    ])

                    rtt_display = "timeout" if math.isnan(avg_rtt) else f"{avg_rtt:.1f}ms"
                    flag = " <ANOMALY>" if anomaly else ""
                    log_parts.append(f"{cls}:{rtt_display}/{loss_pct:.1f}%{flag}")

                csv_file.flush()  # push each cycle to disk immediately

                if cycle_count % PRINT_EVERY_N_CYCLES == 0:
                    print(" | ".join(log_parts) + f" | rows={row_count} anomalies={anomaly_count}")
    finally:
        print("\n[*] Stopping background traffic generators...")
        stop_event.set()
        for t in generator_threads:
            t.join(timeout=5)
        csv_file.close()
        print(f"[*] Finished. {row_count} rows written to {args.output} "
              f"({anomaly_count} labeled anomalous).")


if __name__ == "__main__":
    main()