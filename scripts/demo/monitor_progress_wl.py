#!/usr/bin/env python3
import argparse
import os
import re
import sys
import time
import shutil
import threading

# ---------- regex ----------
LINE_RE = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2}\.\d+).*total size = (?P<total>\d+), in queue = (?P<queue>\d+), running = (?P<running>\d+)"
)
LATENCY_RE = re.compile(r"took\s+(?P<latency>\d+)us")

# ---------- progress parsing ----------
def parse_progress(line):
    m = LINE_RE.search(line)
    if not m:
        return None
    total = int(m.group("total"))
    queue = int(m.group("queue"))
    running = int(m.group("running"))
    completed = max(0, total - queue - running)
    pct = (completed / total * 100.0) if total > 0 else 0.0
    return total, completed, pct

def make_bar(pct, width):
    pct = max(0.0, min(100.0, pct))
    filled = int(round((pct / 100.0) * width))
    return "#" * filled + "." * (width - filled)

def format_progress_line(elapsed, total, completed, pct, bar_width):
    bar = make_bar(pct, bar_width)
    return f"+{elapsed:6.1f}s  [{bar}] {pct:6.2f}%  completed={completed}/{total}"

def format_latency_line(latest, count, total, min_v, max_v):
    if count == 0:
        return "latency (microseconds): count=0"
    avg = total / count
    return (
        f"latency (microseconds): "
        f"latest={latest}  min={min_v}  avg={avg:.1f}  max={max_v}  count={count}"
    )

class TwoLineRenderer:
    def __init__(self):
        self.first = True

    def draw(self, line1, line2):
        if not sys.stdout.isatty():
            sys.stdout.write(line1 + "\n")
            sys.stdout.write(line2 + "\n")
            sys.stdout.flush()
            return

        cols = shutil.get_terminal_size(fallback=(80, 24)).columns
        # 端での折り返し事故を防ぐため -1 しておく
        maxw = max(1, cols - 1)

        def safe(s):
            return s if len(s) <= maxw else s[:maxw]

        l1 = safe(line1)
        l2 = safe(line2)

        if self.first:
            # 初回は普通に2行表示
            sys.stdout.write(l1 + "\n")
            sys.stdout.write(l2)
            sys.stdout.flush()
            self.first = False
            return

        # 2回目以降の更新ロジック (ここを変更)
        # 1. カーソルを行頭に戻す
        # 2. 1行上 (line1の位置) に戻る
        # 3. 行をクリアして line1 を書き、改行する
        # 4. 行をクリアして line2 を書く

        sys.stdout.write("\r")          # カーソルを行頭へ
        sys.stdout.write("\x1b[1A")     # 1行上へ移動
        sys.stdout.write("\x1b[2K")     # その行(line1)をクリア
        sys.stdout.write(l1 + "\n")     # line1を書いて改行 (これで自然にline2へ移動)
        sys.stdout.write("\x1b[2K")     # その行(line2)をクリア
        sys.stdout.write(l2)            # line2を書く
        sys.stdout.flush()

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()

# ---------- shared state ----------
class State:
    def __init__(self):
        self.total = None
        self.completed = None
        self.pct = None
        self.first_time = None
        self.stdin_done_time = None

        self.lat_latest = None
        self.lat_count = 0
        self.lat_sum = 0
        self.lat_min = None
        self.lat_max = None

        self.dirty = False

# ---------- readers ----------
def progress_reader(args, state, lock, stop_event):
    if args.path == "-" or not args.path:
        while not stop_event.is_set():
            line = sys.stdin.readline()
            if not line:
                with lock:
                    if state.stdin_done_time is None:
                        state.stdin_done_time = time.monotonic()
                        if state.total is not None:
                            state.completed = state.total
                            state.pct = 100.0
                        state.dirty = True
                return

            rec = parse_progress(line)
            if not rec:
                continue

            total, completed, pct = rec
            with lock:
                if state.first_time is None:
                    state.first_time = time.monotonic()
                state.total = total
                state.completed = completed
                state.pct = pct
                state.dirty = True
    else:
        with open(args.path, "r", encoding="utf-8", errors="replace") as fp:
            for line in fp:
                if stop_event.is_set():
                    return
                rec = parse_progress(line)
                if not rec:
                    continue
                total, completed, pct = rec
                with lock:
                    if state.first_time is None:
                        state.first_time = time.monotonic()
                    state.total = total
                    state.completed = completed
                    state.pct = pct
                    state.dirty = True

def latency_reader(path, state, lock, stop_event):
    if not os.path.exists(path):
        sys.stderr.write(f"error: latency log not found: {path}\n")
        sys.stderr.flush()
        return

    with open(path, "r", encoding="utf-8", errors="replace") as fp:
        fp.seek(0, os.SEEK_END)
        while not stop_event.is_set():
            line = fp.readline()
            if not line:
                time.sleep(0.2)
                continue
            m = LATENCY_RE.search(line)
            if not m:
                continue
            lat = int(m.group("latency"))
            with lock:
                state.lat_latest = lat
                state.lat_count += 1
                state.lat_sum += lat
                state.lat_min = lat if state.lat_min is None else min(state.lat_min, lat)
                state.lat_max = lat if state.lat_max is None else max(state.lat_max, lat)
                state.dirty = True

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", nargs="?", default="-")
    ap.add_argument("--bar-width", type=int, default=40)
    ap.add_argument("--latency-log", required=True)
    ap.add_argument("--exit-on-done", action="store_true")
    ap.add_argument("--done-wait", type=float, default=3.0)
    args = ap.parse_args()

    state = State()
    lock = threading.Lock()
    stop_event = threading.Event()
    renderer = TwoLineRenderer()

    t1 = threading.Thread(target=progress_reader, args=(args, state, lock, stop_event), daemon=True)
    t2 = threading.Thread(target=latency_reader, args=(args.latency_log, state, lock, stop_event), daemon=True)
    t1.start()
    t2.start()

    exit_deadline = None

    try:
        while True:
            time.sleep(0.1)
            with lock:
                if state.dirty and state.first_time is not None:
                    state.dirty = False
                    elapsed = time.monotonic() - state.first_time
                    l1 = format_progress_line(elapsed, state.total, state.completed, state.pct, args.bar_width)
                    l2 = format_latency_line(
                        state.lat_latest,
                        state.lat_count,
                        state.lat_sum,
                        state.lat_min,
                        state.lat_max,
                    )
                    renderer.draw(l1, l2)

                if args.exit_on_done and state.stdin_done_time is not None:
                    if exit_deadline is None:
                        exit_deadline = state.stdin_done_time + args.done_wait

            if exit_deadline is not None and time.monotonic() >= exit_deadline:
                break

            if not t1.is_alive() and not args.exit_on_done:
                break
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        renderer.finish()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

