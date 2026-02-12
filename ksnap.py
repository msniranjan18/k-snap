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
# Added imports for 3.6 compatibility
from typing import List, Dict, Set, Optional, Tuple, Union

import yaml


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

METADATA_FIELDS_TO_STRIP = ("managedFields", "resourceVersion", "generation")


def clean_object(obj: dict) -> dict:
    """Strip volatile metadata fields from a Kubernetes object."""
    obj = copy.deepcopy(obj)
    meta = obj.get("metadata", {})
    for field in METADATA_FIELDS_TO_STRIP:
        meta.pop(field, None)
    obj["metadata"] = meta
    return obj


def obj_to_yaml(obj: dict) -> str:
    return yaml.dump(obj, default_flow_style=False, allow_unicode=True)


def _sanitize_path_component(s: str) -> str:
    """Replace filesystem-unsafe characters."""
    return re.sub(r"[/\\:*?\"<>|]", "-", s)


def history_dir(namespace: str, kind: str) -> Path:
    safe_ns = _sanitize_path_component(namespace)
    safe_kind = _sanitize_path_component(kind.lower())
    return Path("history") / "{}_{}".format(safe_ns, safe_kind)


def parse_filename(path: Path):
    """Return (name, timestamp_str, kind) where kind is 'FULL' or 'DIFF'."""
    stem = path.stem
    m = re.match(r"^(.+?)_(\d{8}T\d{6}_\d{6})_(FULL|DIFF)$", stem)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def timestamp_to_sort_key(ts: str) -> str:
    return ts


# ---------------------------------------------------------------------------
# Recorder
# ---------------------------------------------------------------------------

class Recorder:
    def __init__(self, resource: str, namespace: str, kind: str):
        if "/" in resource:
            resource_type, target_name = resource.split("/", 1)
        else:
            resource_type, target_name = resource, None

        self.resource = resource_type
        self.target_name = target_name
        self.namespace = namespace
        self.kind = kind
        self.out_dir = history_dir(namespace, kind)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        # Updated type hints for Python 3.6
        self._prev_content = {}  # type: Dict[str, str]
        self._change_count = {}  # type: Dict[str, int]

        # Updated Optional for Python 3.6
        self._last_resource_version = None  # type: Optional[str]

        self._total_events = 0
        self._saved_files = 0
        self._running = True

        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, signum, frame):
        self._running = False
        sys.stderr.write(
            "\n\n[ksnap] Recorder stopped.\n"
            "  Total events processed : {}\n"
            "  Files saved            : {}\n"
            "  Output directory       : {}\n".format(
                self._total_events, self._saved_files, self.out_dir
            )
        )
        sys.exit(0)

    @staticmethod
    def _now_ts() -> str:
        return datetime.utcnow().strftime("%Y%m%dT%H%M%S_%f")

    def _save_full(self, name: str, content: str):
        ts = self._now_ts()
        path = self.out_dir / "{}_{}_FULL.yaml".format(name, ts)
        path.write_text(content, encoding="utf-8")
        self._saved_files += 1
        print("  [FULL] {}".format(path.name), file=sys.stderr)

    def _save_diff(self, name: str, prev: str, curr: str):
        ts = self._now_ts()
        path = self.out_dir / "{}_{}_DIFF.patch".format(name, ts)
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
        print("  [DIFF] {}".format(path.name), file=sys.stderr)

    def _handle_event(self, event_obj: dict):
        event_type = event_obj.get("type", "")
        obj = event_obj.get("object", event_obj)
        name = obj.get("metadata", {}).get("name", "unknown")

        if self.target_name and name != self.target_name:
            return

        rv = obj.get("metadata", {}).get("resourceVersion")
        if rv:
            self._last_resource_version = rv

        cleaned = clean_object(obj)
        content = obj_to_yaml(cleaned)

        prev = self._prev_content.get(name)
        if prev == content:
            return

        count = self._change_count.get(name, 0)
        is_full = (count % 10 == 0)

        if is_full:
            self._save_full(name, content)
        else:
            if prev is None:
                self._save_full(name, content)
            else:
                self._save_diff(name, prev, content)

        self._prev_content[name] = content
        self._change_count[name] = count + 1
        self._total_events += 1

    def _stream_watch(self):
        if self._last_resource_version is None:
            cmd = ["kubectl", "get", self.resource, "-n", self.namespace, "--watch", "-o", "json"]
        else:
            cmd = [
                "kubectl", "get", self.resource, "-n", self.namespace,
                "--watch-only", "--resource-version={}".format(self._last_resource_version),
                "-o", "json"
            ]
        
        print("[ksnap] Starting: {}".format(" ".join(cmd)), file=sys.stderr)
        
        # Changed text=True to universal_newlines=True for 3.6
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
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
        backoff = 2
        while self._running:
            try:
                for event in self._stream_watch():
                    if not self._running:
                        break
                    self._handle_event(event)
                if self._running:
                    print("[ksnap] kubectl exited, reconnecting in {}s…".format(backoff), file=sys.stderr)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            except Exception as exc:
                print("[ksnap] Error: {}; retrying in {}s…".format(exc, backoff), file=sys.stderr)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            else:
                backoff = 2


# ---------------------------------------------------------------------------
# State reconstructor
# ---------------------------------------------------------------------------

def apply_patch(base: str, patch_text: str) -> str:
    base_lines = base.splitlines(keepends=True)
    patch_lines = patch_text.splitlines(keepends=True)
    result_lines = list(base_lines)
    offset = 0

    i = 0
    while i < len(patch_lines):
        line = patch_lines[i]
        if line.startswith("@@"):
            m = re.match(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if not m:
                i += 1
                continue
            old_start = int(m.group(1)) - 1
            old_count = int(m.group(2)) if m.group(2) else 1
            i += 1
            hunk = []
            while i < len(patch_lines) and not patch_lines[i].startswith("@@"):
                if patch_lines[i].startswith(("---", "+++")):
                    i += 1
                    continue
                hunk.append(patch_lines[i])
                i += 1
            new_segment = []
            for h in hunk:
                if h.startswith("+"):
                    new_segment.append(h[1:])
                elif h.startswith(" "):
                    new_segment.append(h[1:])
            actual_start = old_start + offset
            actual_end = actual_start + old_count
            result_lines[actual_start:actual_end] = new_segment
            offset += len(new_segment) - old_count
        else:
            i += 1
    return "".join(result_lines)


def collect_versions(directory: Path, resource_name: str = None):
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
    files = collect_versions(directory, resource_name)
    if not files or target_index >= len(files):
        return ""
    full_idx = None
    for i in range(target_index, -1, -1):
        if files[i][1] == "FULL":
            full_idx = i
            break
    if full_idx is None:
        return "(No FULL snapshot found before this version)"
    content = files[full_idx][3].read_text(encoding="utf-8")
    for i in range(full_idx + 1, target_index + 1):
        ftype = files[i][1]
        if ftype == "FULL":
            content = files[i][3].read_text(encoding="utf-8")
        elif ftype == "DIFF":
            patch_text = files[i][3].read_text(encoding="utf-8")
            content = apply_patch(content, patch_text)
    return content


# Fixed type hint for 3.6
def list_resource_names(directory: Path) -> List[str]:
    names = set()
    for p in directory.iterdir():
        parsed = parse_filename(p)
        if parsed:
            names.add(parsed[0])
    return sorted(list(names))


# ---------------------------------------------------------------------------
# Browser (curses TUI)
# ---------------------------------------------------------------------------

class Browser:
    HEADER_LINES = 3
    FOOTER_LINES = 2
    POLL_INTERVAL = 2.0

    def __init__(self, directory: Path, resource_name: str = None):
        self.directory = directory
        self.resource_name = resource_name
        self._version_count = 0
        self._current_idx = 1
        self._scroll_left = 0
        self._scroll_right = 0
        self._left_content = []  # type: List[str]
        self._right_content = []  # type: List[str]
        self._left_highlights = set()  # type: Set[int]
        self._right_highlights = set()  # type: Set[int]
        self._lock = threading.Lock()
        self._running = True

    def _poll_versions(self):
        while self._running:
            time.sleep(self.POLL_INTERVAL)
            with self._lock:
                new_count = len(collect_versions(self.directory, self.resource_name))
                self._version_count = new_count

    def _load_pair(self):
        t = self._current_idx
        left_raw = reconstruct_version(self.directory, t - 1, self.resource_name)
        right_raw = reconstruct_version(self.directory, t, self.resource_name)
        self._left_content = left_raw.splitlines()
        self._right_content = right_raw.splitlines()
        matcher = difflib.SequenceMatcher(None, self._left_content, self._right_content)
        self._left_highlights = set()
        self._right_highlights = set()
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "replace":
                for i in range(i1, i2): self._left_highlights.add(i)
                for j in range(j1, j2): self._right_highlights.add(j)
            elif tag == "delete":
                for i in range(i1, i2): self._left_highlights.add(i)
            elif tag == "insert":
                for j in range(j1, j2): self._right_highlights.add(j)

    def _draw_header(self, stdscr, total_versions: int):
        h, w = stdscr.getmaxyx()
        title = " ksnap Browser  |  {}  |  Versions: {} ".format(self.directory, total_versions)
        nav = " Showing: T={} (left)  T={} (right) ".format(self._current_idx-1, self._current_idx)
        keys = " ^N=Next  ^P=Prev  ^Q=Quit "
        stdscr.attron(curses.color_pair(1))
        stdscr.addstr(0, 0, title.ljust(w)[:w])
        stdscr.addstr(1, 0, nav.ljust(w)[:w])
        stdscr.addstr(2, 0, keys.ljust(w)[:w])
        stdscr.attroff(curses.color_pair(1))

    def _draw_footer(self, stdscr):
        h, w = stdscr.getmaxyx()
        left_info = " Left scroll: {} ".format(self._scroll_left)
        right_info = " Right scroll: {} ".format(self._scroll_right)
        line = (left_info + " " * (w // 2 - len(left_info)) + right_info).ljust(w)[:w]
        stdscr.attron(curses.color_pair(2))
        stdscr.addstr(h - self.FOOTER_LINES, 0, line[:w])
        stdscr.attroff(curses.color_pair(2))

    # Fixed highlight_set type hint for 3.6
    def _draw_pane(self, stdscr, lines, col_start, pane_w,
                   pane_h, scroll, label, highlight_set=None):
        h, w = stdscr.getmaxyx()
        hdr = "─── {} ".format(label).ljust(pane_w, "─")[:pane_w]
        try:
            stdscr.attron(curses.color_pair(3))
            stdscr.addstr(self.HEADER_LINES, col_start, hdr[:pane_w])
            stdscr.attroff(curses.color_pair(3))
        except curses.error:
            pass
        for row in range(pane_h):
            line_idx = scroll + row
            y = self.HEADER_LINES + 1 + row
            if y >= h - self.FOOTER_LINES:
                break
            if line_idx < len(lines):
                text = lines[line_idx].rstrip()[:pane_w - 1].ljust(pane_w - 1)
                should_highlight = highlight_set is not None and line_idx in highlight_set
                if should_highlight:
                    try:
                        stdscr.attron(curses.color_pair(4))
                        stdscr.addstr(y, col_start, text)
                        stdscr.attroff(curses.color_pair(4))
                        continue
                    except curses.error: pass
                try:
                    stdscr.addstr(y, col_start, text)
                except curses.error: pass
            else:
                try:
                    stdscr.addstr(y, col_start, " " * (pane_w - 1))
                except curses.error: pass

    def _redraw(self, stdscr, total_versions: int):
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        pane_w = w // 2
        pane_h = h - self.HEADER_LINES - 1 - self.FOOTER_LINES
        self._draw_header(stdscr, total_versions)
        self._draw_footer(stdscr)
        for row in range(self.HEADER_LINES, h - self.FOOTER_LINES):
            try:
                stdscr.addch(row, pane_w, "│")
            except curses.error: pass
        self._draw_pane(stdscr, self._left_content, 0, pane_w, pane_h,
                        self._scroll_left, "Version T={}".format(self._current_idx - 1), self._left_highlights)
        self._draw_pane(stdscr, self._right_content, pane_w + 1, pane_w - 1, pane_h,
                        self._scroll_right, "Version T={}".format(self._current_idx), self._right_highlights)
        stdscr.refresh()

    def _curses_main(self, stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        stdscr.timeout(500)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(3, curses.COLOR_CYAN, -1)
        curses.init_pair(4, curses.COLOR_YELLOW, -1)
        with self._lock:
            self._version_count = len(collect_versions(self.directory, self.resource_name))
        if self._version_count < 2:
            stdscr.nodelay(False)
            stdscr.clear()
            stdscr.addstr(0, 0, "Waiting for at least 2 recorded versions…  (^Q to quit)")
            stdscr.refresh()
            while self._version_count < 2 and self._running:
                k = stdscr.getch()
                if k == 17: return
                with self._lock:
                    self._version_count = len(collect_versions(self.directory, self.resource_name))
                time.sleep(0.5)
            stdscr.nodelay(True)
        self._current_idx = 1
        self._load_pair()
        while self._running:
            with self._lock: total = self._version_count
            self._redraw(stdscr, total)
            key = stdscr.getch()
            if key == curses.ERR: continue
            if key == 14:
                if self._current_idx < total - 1:
                    self._current_idx += 1
                    self._scroll_left = self._scroll_right = 0
                    self._load_pair()
            elif key == 16:
                if self._current_idx > 1:
                    self._current_idx -= 1
                    self._scroll_left = self._scroll_right = 0
                    self._load_pair()
            elif key == 17:
                self._running = False
                return
            elif key == curses.KEY_DOWN:
                self._scroll_right = min(self._scroll_right + 1, max(0, len(self._right_content) - 5))
                self._scroll_left = min(self._scroll_left + 1, max(0, len(self._left_content) - 5))
            elif key == curses.KEY_UP:
                self._scroll_right = max(0, self._scroll_right - 1)
                self._scroll_left = max(0, self._scroll_left - 1)
            elif key == curses.KEY_RESIZE:
                curses.resize_term(*stdscr.getmaxyx())

    def run(self):
        poll_thread = threading.Thread(target=self._poll_versions)
        poll_thread.daemon = True
        poll_thread.start()
        try:
            curses.wrapper(self._curses_main)
        finally:
            self._running = False

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def cmd_record(args):
    resource_type = args.resource.split("/")[0]
    default_kind = resource_type.rstrip("s").capitalize()
    recorder = Recorder(resource=args.resource, namespace=args.namespace, kind=args.kind or default_kind)
    recorder.run()

def cmd_browse(args):
    if args.dir:
        directory = Path(args.dir)
    else:
        if not args.namespace or not args.kind:
            print("Error: provide --namespace and --kind, or --dir directly.", file=sys.stderr)
            sys.exit(1)
        directory = history_dir(args.namespace, args.kind)
    if not directory.exists():
        print("Error: directory '{}' does not exist.".format(directory), file=sys.stderr)
        sys.exit(1)
    resource_name = getattr(args, "name", None)
    if resource_name is None:
        names = list_resource_names(directory)
        if not names:
            print("No recorded versions found.", file=sys.stderr); sys.exit(1)
        if len(names) > 1:
            print("Multiple resources found:")
            for i, n in enumerate(names): print("  [{}] {}".format(i, n))
            choice = input("Select index [0]: ").strip() or "0"
            resource_name = names[int(choice)]
        else:
            resource_name = names[0]
    print("[ksnap] Browsing '{}' in {}".format(resource_name, directory))
    browser = Browser(directory, resource_name)
    browser.run()

def main():
    parser = argparse.ArgumentParser(prog="ksnap", description="Record and browse Kubernetes object history.")
    sub = parser.add_subparsers(dest="command")
    if sys.version_info >= (3, 7):
        sub.required = True
    
    rec = sub.add_parser("record", help="Watch and record a Kubernetes resource.")
    rec.add_argument("resource")
    rec.add_argument("-n", "--namespace", default="default")
    rec.add_argument("--kind")
    rec.set_defaults(func=cmd_record)

    brw = sub.add_parser("browse", help="Browse recorded history in a TUI.")
    brw.add_argument("-n", "--namespace")
    brw.add_argument("-k", "--kind")
    brw.add_argument("--dir")
    brw.add_argument("--name")
    brw.set_defaults(func=cmd_browse)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)

if __name__ == "__main__":
    main()
