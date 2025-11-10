#!/usr/bin/env python3
import argparse
import os
import re
import sys
import time
import shutil  # for terminal size

LINE_RE = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2}\.\d+).*total size = (?P<total>\d+), in queue = (?P<queue>\d+), running = (?P<running>\d+)"
)

def build_done_re(pattern):
    return re.compile(rf"(?P<time>\d{{2}}:\d{{2}}:\d{{2}}\.\d+).*(?:{pattern})")

def parse_progress(line):
    """Parse a progress line and return (total, completed, pct) or None."""
    m = LINE_RE.search(line)
    if not m:
        return None
    total = int(m.group("total"))
    queue = int(m.group("queue"))
    running = int(m.group("running"))
    completed = max(0, total - queue - running)
    pct = (completed / total * 100) if total > 0 else 0.0
    return (total, completed, pct)

def make_bar(pct, width):
    pct_clamped = max(0.0, min(100.0, pct))
    filled = int(round((pct_clamped / 100.0) * width))
    return "#" * filled + "." * (width - filled)

def format_line(elapsed_sec, total, completed, pct, bar_width):
    bar = make_bar(pct, bar_width)
    return f"+{elapsed_sec:6.1f}s  [{bar}] {pct:6.2f}%  completed={completed}/{total}"

def print_single_line(msg):
    """Print a single updatable console line with terminal-resize safety."""
    if not sys.stdout.isatty():
        # When stdout is not a TTY (e.g., piped/recorded), fall back to newline
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()
        return

    cols = shutil.get_terminal_size(fallback=(80, 24)).columns
    maxw = max(1, cols - 1)
    safe = msg if len(msg) <= maxw else msg[:maxw]

    # Move to start of line and clear below to erase any previous wrap remnants
    sys.stdout.write("\r\x1b[0J" + safe)
    sys.stdout.flush()

def follow_file(fp):
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
        description="Monitor app log and show progress bar with elapsed seconds (resizes safely)."
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
    first_log_time = None  # start time set when first progress appears

    try:
        for line in stream_lines(args):
            rec = parse_progress(line)
            if rec:
                total, c, pct = rec
                last_total = total

                # initialize timer on first progress
                if first_log_time is None:
                    first_log_time = time.monotonic()

                elapsed = time.monotonic() - first_log_time
                print_single_line(format_line(elapsed, total, c, pct, args.bar_width))
                continue

            m_done = done_re.search(line)
            if m_done:
                now = time.monotonic()
                elapsed = (now - first_log_time) if first_log_time else 0.0
                if last_total and last_total > 0:
                    total = last_total
                    msg = format_line(elapsed, total, total, 100.0, args.bar_width)
                else:
                    bar = make_bar(100.0, args.bar_width)
                    msg = f"+{elapsed:6.1f}s  [{bar}] 100.00%  completed=?/?"
                print_single_line(msg)
                if args.exit_on_done:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    return
        sys.stdout.write("\n")
        sys.stdout.flush()
    except KeyboardInterrupt:
        sys.stdout.write("\n")
        sys.stdout.flush()

if __name__ == "__main__":
    main()

