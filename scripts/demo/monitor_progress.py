#!/usr/bin/env python3
import argparse
import os
import re
import sys
import time

# Regex for progress lines
LINE_RE = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2}\.\d+).*total size = (?P<total>\d+), in queue = (?P<queue>\d+), running = (?P<running>\d+)"
)
# Regex for "done" line (customizable via CLI)
def build_done_re(pattern):
    return re.compile(rf"(?P<time>\d{{2}}:\d{{2}}:\d{{2}}\.\d+).*(?:{pattern})")

def parse_progress(line):
    """Parse a progress line and return (time, total, queue, running, completed, pct) or None."""
    m = LINE_RE.search(line)
    if not m:
        return None
    total = int(m.group("total"))
    queue = int(m.group("queue"))
    running = int(m.group("running"))
    completed = max(0, total - queue - running)
    pct = (completed / total * 100) if total > 0 else 0.0
    return (m.group("time"), total, queue, running, completed, pct)

def make_bar(pct, width):
    """Build ASCII progress bar using '#' and '.'."""
    pct_clamped = max(0.0, min(100.0, pct))
    filled = int(round((pct_clamped / 100.0) * width))
    return "#" * filled + "." * (width - filled)

def format_line(time_s, total, queue, running, completed, pct, bar_width):
    bar = make_bar(pct, bar_width)
    return f"{time_s}  [{bar}] {pct:6.2f}%  completed={completed}/{total}  queue={queue}  running={running}"

def print_single_line(msg):
    """Print a single updatable console line."""
    sys.stdout.write("\r" + msg.ljust(140))
    sys.stdout.flush()

def follow_file(fp):
    """Yield new lines appended to an open file (tail -f)."""
    fp.seek(0, os.SEEK_END)
    buf = ""
    while True:
        chunk = fp.read()
        if not chunk:
            time.sleep(0.2)
            continue
        buf += chunk
        while True:
            nl = buf.find("\n")
            if nl == -1:
                break
            line = buf[:nl + 1]
            buf = buf[nl + 1:]
            yield line

def stream_lines(args):
    """Iterate lines from stdin or a file, optionally following."""
    if args.path == "-" or (not args.path and not args.follow):
        for line in sys.stdin:
            yield line
        return
    with open(args.path, "r", encoding="utf-8", errors="replace") as fp:
        if args.follow:
            yield from follow_file(fp)
        else:
            for line in fp:
                yield line

def main():
    ap = argparse.ArgumentParser(
        description="Monitor app log, show single-line progress bar, and mark 100% on done pattern."
    )
    ap.add_argument("path", nargs="?", default="-", help="Log file path or '-' for stdin")
    ap.add_argument("-f", "--follow", action="store_true", help="Follow file like tail -f")
    ap.add_argument("--bar-width", type=int, default=40, help="Progress bar width (default 40)")
    ap.add_argument("--done-pattern", default=r"Billings calculated in",
                    help="Regex pattern to detect completion (default: 'Billings calculated in')")
    ap.add_argument("--exit-on-done", action="store_true",
                    help="Exit immediately after printing 100%% on completion")
    args = ap.parse_args()

    done_re = build_done_re(args.done_pattern)

    last_total = None
    last_time = None

    try:
        for line in stream_lines(args):
            # Update progress if a progress line
            rec = parse_progress(line)
            if rec:
                t, total, q, r, c, pct = rec
                last_total = total
                last_time = t
                print_single_line(format_line(t, total, q, r, c, pct, args.bar_width))
                continue

            # If done-pattern appears, force 100%
            m_done = done_re.search(line)
            if m_done:
                t = m_done.group("time") if "time" in m_done.groupdict() else (last_time or "--:--:--.---")
                if last_total and last_total > 0:
                    total = last_total
                    msg = format_line(t, total, 0, 0, total, 100.0, args.bar_width)
                else:
                    # Fallback if total was never seen; show 100% with unknown totals
                    bar = make_bar(100.0, args.bar_width)
                    msg = f"{t}  [{bar}] 100.00%  completed=?/?  queue=0  running=0"
                print_single_line(msg)
                if args.exit_on_done:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    return
        # Finish cleanly when input ends (non-follow case)
        sys.stdout.write("\n")
        sys.stdout.flush()
    except KeyboardInterrupt:
        sys.stdout.write("\n")
        sys.stdout.flush()

if __name__ == "__main__":
    main()

