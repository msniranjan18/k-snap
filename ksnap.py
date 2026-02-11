#!/usr/bin/env python3
"""
ksnap: Kubernetes object history recorder and browser.
Uses a Checkpoint + Delta strategy for space-efficient history storage.
"""

import argparse
import copy
import curses
import difflib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

METADATA_FIELDS_TO_STRIP = ("managedFields", "resourceVersion", "generation")


def clean_object(obj: dict) -> dict:
    """Strip volatile metadata fields from a Kubernetes object.

    Status is preserved intentionally so CRD state history is not lost.
    Only noisy/volatile metadata fields that cause false-positive diffs are
    removed: managedFields, resourceVersion, and generation.
    """
    obj = copy.deepcopy(obj)
    meta = obj.get("metadata", {})
    for field in METADATA_FIELDS_TO_STRIP:
        meta.pop(field, None)
    obj["metadata"] = meta
    # NOTE: status is intentionally kept — CRDs encode meaningful state there.
    return obj


def obj_to_yaml(obj: dict) -> str:
    return yaml.dump(obj, default_flow_style=False, allow_unicode=True)


def _sanitize_path_component(s: str) -> str:
    """Replace filesystem-unsafe characters so CRD names don't break paths."""
    # Replace forward-slash (e.g. from group/version CRD names) and other
    # problematic characters with a hyphen.
    return re.sub(r"[/\\:*?\"<>|]", "-", s)


def history_dir(namespace: str, kind: str) -> Path:
    safe_ns = _sanitize_path_component(namespace)
    safe_kind = _sanitize_path_component(kind.lower())
    return Path("history") / f"{safe_ns}_{safe_kind}"


def parse_filename(path: Path):
    """
    Return (name, timestamp_str, kind) where kind is 'FULL' or 'DIFF'.
    Filename format: <name>_<timestamp>_FULL.yaml  or  <name>_<timestamp>_DIFF.patch
    Timestamps are ISO-like with no colons: 20240101T120000_000000
    """
    stem = path.stem  # strip extension
    # stem examples: nginx_20240101T120000_000000_FULL
    #                nginx_20240101T120000_000000_DIFF
    m = re.match(r"^(.+?)_(\d{8}T\d{6}_\d{6})_(FULL|DIFF)$", stem)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def timestamp_to_sort_key(ts: str) -> str:
    """Timestamps are already sortable strings (no colons)."""
    return ts


# ---------------------------------------------------------------------------
# Recorder
# ---------------------------------------------------------------------------

class Recorder:
    def __init__(self, resource: str, namespace: str, kind: str):
        # Support "resource/name" syntax (e.g. "deployments/my-app")
        if "/" in resource:
            resource_type, target_name = resource.split("/", 1)
        else:
            resource_type, target_name = resource, None

        self.resource = resource_type          # bare resource type for kubectl
        self.target_name = target_name         # optional per-object filter
        self.namespace = namespace
        self.kind = kind
        self.out_dir = history_dir(namespace, kind)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        # Per-object tracking
        self._prev_content: dict[str, str] = {}   # name -> last saved YAML
        self._change_count: dict[str, int] = {}   # name -> count of saved changes

        # Track the last resourceVersion we successfully processed so that on
        # reconnection we can pass --resource-version to avoid re-replaying the
        # full initial LIST and only receive events we haven't seen yet.
        self._last_resource_version: str | None = None

        self._total_events = 0
        self._saved_files = 0
        self._running = True

        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, signum, frame):
        self._running = False
        print(
            f"\n\n[ksnap] Recorder stopped.\n"
            f"  Total events processed : {self._total_events}\n"
            f"  Files saved            : {self._saved_files}\n"
            f"  Output directory       : {self.out_dir}\n",
            file=sys.stderr,
        )
        sys.exit(0)

    @staticmethod
    def _now_ts() -> str:
        """Return a filesystem-safe timestamp string."""
        return datetime.utcnow().strftime("%Y%m%dT%H%M%S_%f")

    def _save_full(self, name: str, content: str):
        ts = self._now_ts()
        path = self.out_dir / f"{name}_{ts}_FULL.yaml"
        path.write_text(content, encoding="utf-8")
        self._saved_files += 1
        print(f"  [FULL] {path.name}", file=sys.stderr)

    def _save_diff(self, name: str, prev: str, curr: str):
        ts = self._now_ts()
        path = self.out_dir / f"{name}_{ts}_DIFF.patch"
        diff = list(
            difflib.unified_diff(
                prev.splitlines(keepends=True),
                curr.splitlines(keepends=True),
                fromfile="prev",
                tofile="curr",
            )
        )
        path.write_text("".join(diff), encoding="utf-8")
        self._saved_files += 1
        print(f"  [DIFF] {path.name}", file=sys.stderr)

    def _handle_event(self, event_obj: dict):
        """Process a single watch event object."""
        event_type = event_obj.get("type", "")
        obj = event_obj.get("object", event_obj)  # Some events wrap, some don't

        # Safely extract name
        name = obj.get("metadata", {}).get("name", "unknown")

        # ---- Resource-name filter (kind/name syntax) ----
        if self.target_name and name != self.target_name:
            return

        # Track the latest resourceVersion for reconnection continuity.
        rv = obj.get("metadata", {}).get("resourceVersion")
        if rv:
            self._last_resource_version = rv

        cleaned = clean_object(obj)
        content = obj_to_yaml(cleaned)

        prev = self._prev_content.get(name)
        if prev == content:
            # Deduplicate: nothing changed
            return

        count = self._change_count.get(name, 0)
        is_full = (count % 10 == 0)

        if is_full:
            self._save_full(name, content)
        else:
            if prev is None:
                # No previous state in memory; save full anyway to anchor diffs
                self._save_full(name, content)
            else:
                self._save_diff(name, prev, content)

        self._prev_content[name] = content
        self._change_count[name] = count + 1
        self._total_events += 1

    def _stream_watch(self):
        """Run kubectl --watch and yield parsed JSON objects.

        Passes --resource-version on reconnections so we resume exactly where
        we left off rather than receiving a full re-LIST that would replay
        already-seen events (or, worse, miss events that occurred during the
        gap if the API server's watch cache has been compacted).
        """
        cmd = [
            "kubectl", "get", self.resource,
            "-n", self.namespace,
            "--watch-only",          # skip the initial LIST; only stream deltas
            "-o", "json",
        ]
        # On the very first connection we don't have a resourceVersion yet, so
        # we omit --watch-only and let kubectl do the initial LIST so that we
        # capture the current state of every object before watching for changes.
        if self._last_resource_version is None:
            cmd = [
                "kubectl", "get", self.resource,
                "-n", self.namespace,
                "--watch",
                "-o", "json",
            ]
        else:
            cmd = [
                "kubectl", "get", self.resource,
                "-n", self.namespace,
                "--watch-only",
                f"--resource-version={self._last_resource_version}",
                "-o", "json",
            ]
        print(f"[ksnap] Starting: {' '.join(cmd)}", file=sys.stderr)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        buffer = ""
        brace_depth = 0
        in_object = False

        try:
            for char in iter(lambda: proc.stdout.read(1), ""):
                if not self._running:
                    proc.terminate()
                    return

                buffer += char
                if char == "{":
                    brace_depth += 1
                    in_object = True
                elif char == "}":
                    brace_depth -= 1

                if in_object and brace_depth == 0:
                    text = buffer.strip()
                    buffer = ""
                    in_object = False
                    if text:
                        try:
                            yield json.loads(text)
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
        finally:
            proc.wait()

    def run(self):
        """Main record loop with reconnection."""
        backoff = 2
        while self._running:
            try:
                for event in self._stream_watch():
                    if not self._running:
                        break
                    self._handle_event(event)
                if self._running:
                    print(
                        f"[ksnap] kubectl exited, reconnecting in {backoff}s…",
                        file=sys.stderr,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            except Exception as exc:
                print(f"[ksnap] Error: {exc}; retrying in {backoff}s…", file=sys.stderr)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            else:
                # Clean exit from the stream — reset backoff so a healthy
                # reconnection doesn't inherit a previous failure's penalty.
                backoff = 2


# ---------------------------------------------------------------------------
# State reconstructor
# ---------------------------------------------------------------------------

def apply_patch(base: str, patch_text: str) -> str:
    """
    Apply a unified diff patch to a base string and return the result.
    Uses difflib-compatible patch parsing (no external tools needed).
    """
    base_lines = base.splitlines(keepends=True)
    patch_lines = patch_text.splitlines(keepends=True)

    result_lines = list(base_lines)
    offset = 0  # cumulative line offset from applied hunks

    i = 0
    while i < len(patch_lines):
        line = patch_lines[i]
        # Parse hunk header: @@ -start,count +start,count @@
        if line.startswith("@@"):
            m = re.match(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if not m:
                i += 1
                continue
            old_start = int(m.group(1)) - 1  # 0-indexed
            old_count = int(m.group(2)) if m.group(2) else 1
            i += 1

            # Collect hunk body
            hunk = []
            while i < len(patch_lines) and not patch_lines[i].startswith("@@"):
                if patch_lines[i].startswith(("---", "+++")):
                    i += 1
                    continue
                hunk.append(patch_lines[i])
                i += 1

            # Build new segment from hunk
            new_segment = []
            for h in hunk:
                if h.startswith("+"):
                    new_segment.append(h[1:])
                elif h.startswith(" "):
                    new_segment.append(h[1:])
                # lines starting with '-' are removed

            # Replace old_start..old_start+old_count in result
            actual_start = old_start + offset
            actual_end = actual_start + old_count
            result_lines[actual_start:actual_end] = new_segment
            offset += len(new_segment) - old_count
        else:
            i += 1

    return "".join(result_lines)


def collect_versions(directory: Path, resource_name: str = None):
    """
    Return an ordered list of (sort_key, path) for all FULL and DIFF files
    in directory, optionally filtered by resource_name.
    """
    files = []
    for p in directory.iterdir():
        parsed = parse_filename(p)
        if parsed is None:
            continue
        name, ts, kind = parsed
        if resource_name and name != resource_name:
            continue
        files.append((ts, kind, name, p))
    files.sort(key=lambda x: x[0])
    return files


def reconstruct_version(directory: Path, target_index: int, resource_name: str = None) -> str:
    """
    Reconstruct the YAML content of the target_index-th version by finding
    the nearest preceding FULL snapshot and applying all subsequent DIFFs.
    """
    files = collect_versions(directory, resource_name)
    if not files or target_index >= len(files):
        return ""

    # Find the latest FULL at or before target_index
    full_idx = None
    for i in range(target_index, -1, -1):
        if files[i][1] == "FULL":
            full_idx = i
            break

    if full_idx is None:
        return "(No FULL snapshot found before this version)"

    content = files[full_idx][3].read_text(encoding="utf-8")

    # Apply DIFFs from full_idx+1 to target_index
    for i in range(full_idx + 1, target_index + 1):
        ftype = files[i][1]
        if ftype == "FULL":
            content = files[i][3].read_text(encoding="utf-8")
        elif ftype == "DIFF":
            patch_text = files[i][3].read_text(encoding="utf-8")
            content = apply_patch(content, patch_text)

    return content


def list_resource_names(directory: Path) -> list[str]:
    """Return sorted unique resource names recorded in a directory."""
    names = set()
    for p in directory.iterdir():
        parsed = parse_filename(p)
        if parsed:
            names.add(parsed[0])
    return sorted(names)


# ---------------------------------------------------------------------------
# Browser (curses TUI)
# ---------------------------------------------------------------------------

class Browser:
    HEADER_LINES = 3
    FOOTER_LINES = 2
    POLL_INTERVAL = 2.0  # seconds between live-mode polls

    def __init__(self, directory: Path, resource_name: str = None):
        self.directory = directory
        self.resource_name = resource_name

        self._version_count = 0
        self._current_idx = 1      # current T (right pane); left pane is T-1
        self._scroll_left = 0
        self._scroll_right = 0
        self._left_content: list[str] = []
        self._right_content: list[str] = []
        self._lock = threading.Lock()
        self._running = True

    # ---- Live polling thread ----

    def _poll_versions(self):
        while self._running:
            time.sleep(self.POLL_INTERVAL)
            with self._lock:
                new_count = len(collect_versions(self.directory, self.resource_name))
                self._version_count = new_count

    # ---- Content loading ----

    def _load_pair(self):
        """Reconstruct left (T-1) and right (T) pane contents."""
        t = self._current_idx
        left_raw = reconstruct_version(self.directory, t - 1, self.resource_name)
        right_raw = reconstruct_version(self.directory, t, self.resource_name)
        self._left_content = left_raw.splitlines()
        self._right_content = right_raw.splitlines()

    # ---- Drawing ----

    def _draw_header(self, stdscr, total_versions: int):
        h, w = stdscr.getmaxyx()
        title = f" ksnap Browser  |  {self.directory}  |  Versions: {total_versions} "
        nav = f" Showing: T={self._current_idx-1} (left)  T={self._current_idx} (right) "
        keys = " ^N=Next  ^P=Prev  ^Q=Quit "

        stdscr.attron(curses.color_pair(1))
        stdscr.addstr(0, 0, title.ljust(w)[:w])
        stdscr.addstr(1, 0, nav.ljust(w)[:w])
        stdscr.addstr(2, 0, keys.ljust(w)[:w])
        stdscr.attroff(curses.color_pair(1))

    def _draw_footer(self, stdscr):
        h, w = stdscr.getmaxyx()
        left_info = f" Left scroll: {self._scroll_left} "
        right_info = f" Right scroll: {self._scroll_right} "
        line = (left_info + " " * (w // 2 - len(left_info)) + right_info).ljust(w)[:w]
        stdscr.attron(curses.color_pair(2))
        stdscr.addstr(h - self.FOOTER_LINES, 0, line[:w])
        stdscr.attroff(curses.color_pair(2))

    def _draw_pane(self, stdscr, lines: list[str], col_start: int, pane_w: int,
                   pane_h: int, scroll: int, label: str, highlight_diffs: bool = False,
                   other_lines: list[str] = None):
        """Render one pane's content."""
        h, w = stdscr.getmaxyx()
        # Pane header
        hdr = f"─── {label} ".ljust(pane_w, "─")[:pane_w]
        try:
            stdscr.attron(curses.color_pair(3))
            stdscr.addstr(self.HEADER_LINES, col_start, hdr[:pane_w])
            stdscr.attroff(curses.color_pair(3))
        except curses.error:
            pass

        # Content rows
        for row in range(pane_h):
            line_idx = scroll + row
            y = self.HEADER_LINES + 1 + row
            if y >= h - self.FOOTER_LINES:
                break
            if line_idx < len(lines):
                text = lines[line_idx].rstrip()[:pane_w - 1].ljust(pane_w - 1)
                # Highlight differing lines
                if highlight_diffs and other_lines is not None:
                    other_text = (
                        other_lines[line_idx].rstrip()
                        if line_idx < len(other_lines)
                        else ""
                    )
                    if text.strip() != other_text.strip():
                        try:
                            stdscr.attron(curses.color_pair(4))
                            stdscr.addstr(y, col_start, text)
                            stdscr.attroff(curses.color_pair(4))
                            continue
                        except curses.error:
                            pass
                try:
                    stdscr.addstr(y, col_start, text)
                except curses.error:
                    pass
            else:
                try:
                    stdscr.addstr(y, col_start, " " * (pane_w - 1))
                except curses.error:
                    pass

    def _redraw(self, stdscr, total_versions: int):
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        pane_w = w // 2
        pane_h = h - self.HEADER_LINES - 1 - self.FOOTER_LINES  # rows for content

        self._draw_header(stdscr, total_versions)
        self._draw_footer(stdscr)

        # Divider
        for row in range(self.HEADER_LINES, h - self.FOOTER_LINES):
            try:
                stdscr.addch(row, pane_w, "│")
            except curses.error:
                pass

        # Left pane (T-1)
        left_label = f"Version T={self._current_idx - 1}"
        self._draw_pane(
            stdscr, self._left_content,
            col_start=0, pane_w=pane_w, pane_h=pane_h,
            scroll=self._scroll_left, label=left_label,
        )
        # Right pane (T) — highlight diffs vs left
        right_label = f"Version T={self._current_idx}"
        self._draw_pane(
            stdscr, self._right_content,
            col_start=pane_w + 1, pane_w=pane_w - 1, pane_h=pane_h,
            scroll=self._scroll_right, label=right_label,
            highlight_diffs=True, other_lines=self._left_content,
        )
        stdscr.refresh()

    # ---- Main curses loop ----

    def _curses_main(self, stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        stdscr.timeout(500)  # half-second timeout so we can poll

        # Colour pairs
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)   # header
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)  # footer
        curses.init_pair(3, curses.COLOR_CYAN, -1)                   # pane header
        curses.init_pair(4, curses.COLOR_YELLOW, -1)                 # diff highlight

        # Initial load
        with self._lock:
            self._version_count = len(collect_versions(self.directory, self.resource_name))

        if self._version_count < 2:
            # Wait for enough versions
            stdscr.nodelay(False)
            stdscr.clear()
            stdscr.addstr(0, 0, "Waiting for at least 2 recorded versions…  (^Q to quit)")
            stdscr.refresh()
            while self._version_count < 2 and self._running:
                k = stdscr.getch()
                if k == 17:  # Ctrl+Q
                    return
                with self._lock:
                    self._version_count = len(collect_versions(self.directory, self.resource_name))
                time.sleep(0.5)
            stdscr.nodelay(True)
            stdscr.timeout(500)

        self._current_idx = 1
        self._load_pair()

        while self._running:
            with self._lock:
                total = self._version_count

            self._redraw(stdscr, total)

            key = stdscr.getch()

            if key == curses.ERR:
                # Timeout — just redraw (handles live count updates)
                continue

            if key == 14:  # Ctrl+N
                max_idx = total - 1
                if self._current_idx < max_idx:
                    self._current_idx += 1
                    self._scroll_left = 0
                    self._scroll_right = 0
                    self._load_pair()

            elif key == 16:  # Ctrl+P
                if self._current_idx > 1:
                    self._current_idx -= 1
                    self._scroll_left = 0
                    self._scroll_right = 0
                    self._load_pair()

            elif key == 17:  # Ctrl+Q
                self._running = False
                return

            # Arrow keys for scrolling
            elif key == curses.KEY_DOWN:
                max_scroll = max(0, len(self._right_content) - 5)
                self._scroll_right = min(self._scroll_right + 1, max_scroll)
                self._scroll_left = min(self._scroll_left + 1,
                                        max(0, len(self._left_content) - 5))

            elif key == curses.KEY_UP:
                self._scroll_right = max(0, self._scroll_right - 1)
                self._scroll_left = max(0, self._scroll_left - 1)

            elif key == curses.KEY_NPAGE:  # Page Down
                h, _ = stdscr.getmaxyx()
                step = h - self.HEADER_LINES - self.FOOTER_LINES - 2
                self._scroll_right = min(self._scroll_right + step,
                                         max(0, len(self._right_content) - 5))
                self._scroll_left = min(self._scroll_left + step,
                                        max(0, len(self._left_content) - 5))

            elif key == curses.KEY_PPAGE:  # Page Up
                h, _ = stdscr.getmaxyx()
                step = h - self.HEADER_LINES - self.FOOTER_LINES - 2
                self._scroll_right = max(0, self._scroll_right - step)
                self._scroll_left = max(0, self._scroll_left - step)

            elif key == curses.KEY_RESIZE:
                curses.resize_term(*stdscr.getmaxyx())

    def run(self):
        # Start live polling thread
        poll_thread = threading.Thread(target=self._poll_versions, daemon=True)
        poll_thread.start()
        try:
            curses.wrapper(self._curses_main)
        finally:
            self._running = False


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def cmd_record(args):
    # Derive a human-readable kind from the resource type portion only.
    # args.resource may be "deployments" or "deployments/my-app" — strip /name.
    resource_type = args.resource.split("/")[0]
    default_kind = resource_type.rstrip("s").capitalize()
    recorder = Recorder(
        resource=args.resource,
        namespace=args.namespace,
        kind=args.kind or default_kind,
    )
    recorder.run()


def cmd_browse(args):
    # Determine directory
    if args.dir:
        directory = Path(args.dir)
    else:
        if not args.namespace or not args.kind:
            print(
                "Error: provide --namespace and --kind, or --dir directly.",
                file=sys.stderr,
            )
            sys.exit(1)
        directory = history_dir(args.namespace, args.kind)

    if not directory.exists():
        print(f"Error: directory '{directory}' does not exist.", file=sys.stderr)
        sys.exit(1)

    resource_name = getattr(args, "name", None)

    # Only prompt when the caller has NOT already specified a name.
    if resource_name is None:
        names = list_resource_names(directory)
        if not names:
            print("No recorded versions found in directory.", file=sys.stderr)
            sys.exit(1)
        if len(names) > 1:
            print("Multiple resources found:")
            for i, n in enumerate(names):
                print(f"  [{i}] {n}")
            choice = input("Select index [0]: ").strip() or "0"
            resource_name = names[int(choice)]
        else:
            resource_name = names[0]
    else:
        # Validate that the requested name actually exists in the directory.
        names = list_resource_names(directory)
        if resource_name not in names:
            print(
                f"Error: no history found for '{resource_name}' in {directory}.\n"
                f"  Available: {', '.join(names) if names else '(none)'}",
                file=sys.stderr,
            )
            sys.exit(1)

    print(f"[ksnap] Browsing '{resource_name}' in {directory}")
    browser = Browser(directory, resource_name)
    browser.run()


def main():
    parser = argparse.ArgumentParser(
        prog="ksnap",
        description="Record and browse Kubernetes object history.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- record ---
    rec = sub.add_parser("record", help="Watch and record a Kubernetes resource.")
    rec.add_argument("resource", help='Resource type, e.g. "deployments" or "pods"')
    rec.add_argument("-n", "--namespace", default="default", help="Kubernetes namespace")
    rec.add_argument(
        "--kind",
        help="Override the Kind name used for the storage directory (default: derived from resource)",
    )
    rec.set_defaults(func=cmd_record)

    # --- browse ---
    brw = sub.add_parser("browse", help="Browse recorded history in a TUI.")
    brw.add_argument("-n", "--namespace", help="Kubernetes namespace")
    brw.add_argument("-k", "--kind", help="Resource kind (e.g. Deployment)")
    brw.add_argument("--dir", help="Explicit history directory path")
    brw.add_argument("--name", help="Filter to a specific resource name")
    brw.set_defaults(func=cmd_browse)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
