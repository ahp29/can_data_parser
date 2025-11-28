#!/usr/bin/env python3
"""
CAN Log Parser with DBC Support

This tool parses CAN (Controller Area Network) log files using a DBC (CAN database) file
to extract and visualize signal timeseries data.

USAGE:
    python CAN_parser.py --dbc <path_to_dbc> --log <path_to_log> [OPTIONS]

REQUIRED ARGUMENTS:
    --dbc PATH          Path to the DBC file (database definition)
    --log PATH          Path to the CAN log file (CSV, text, or BusMaster format)

OPTIONAL ARGUMENTS:
    --signals NAMES     Comma-separated list of signal names to extract (e.g., "Speed,Temperature")
                        If not provided, interactive selection will be shown
    --excel PATH        Export collected timeseries data to Excel file with charts
    --html PATH         Export collected timeseries data to interactive HTML file (requires plotly)
    --verbose           Enable verbose logging for debugging

SUPPORTED LOG FORMATS:
    - BusMaster-style: "14:38:10:7213 Rx 1 0x18F0F4F9 x 8 4F 01 00 00 18 08 04 00"
    - Simple formats with: timestamp, CAN ID (hex or decimal), and data bytes

EXAMPLES:

    1. Interactive mode (choose signals from menu):
       python CAN_parser.py --dbc database.dbc --log capture.log

    2. Extract specific signals:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "EngineSpeed,Throttle"

    3. Export to Excel with charts:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "Speed" --excel output.xlsx

    4. Export to interactive HTML plot:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "Speed,RPM" --html plot.html

    5. Extract all signals and export:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "all" --excel results.xlsx

REQUIREMENTS:
    pip install cantools
    pip install pandas xlsxwriter  # For Excel export
    pip install plotly             # For HTML export
"""
import argparse
import logging
import re
import sys
from typing import Dict, List, Tuple, Optional, Generator

try:
    import cantools  # type: ignore
except Exception:
    print("ERROR: cantools is required. Install with: pip install cantools", file=sys.stderr)
    sys.exit(1)

# optional deps for Excel export
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None

# try xlsxwriter for charts
try:
    import xlsxwriter  # type: ignore
    _XLSXWRITER_AVAILABLE = True
except Exception:
    _XLSXWRITER_AVAILABLE = False

# Setup logging
logger = logging.getLogger(__name__)

# Regex patterns with comments
HEX_PAIR_RE = re.compile(r'(?:[0-9A-Fa-f]{2})(?:[\s:][0-9A-Fa-f]{2})*|[0-9A-Fa-f]{2,}')  # hex pairs or contiguous hex
TIMESTAMP_RE = re.compile(r'(\d+\.\d+|\d+)')  # floating point or integer timestamps
ID_HEX_RE = re.compile(r'0x[0-9A-Fa-f]+')  # hexadecimal CAN IDs
ID_DEC_RE = re.compile(r'\b\d+\b')  # decimal CAN IDs

# BusMaster / similar log line:
# e.g. "14:38:10:7213 Rx 1 0x18F0F4F9 x 8 4F 01 00 00 18 08 04 00"
BM_RE = re.compile(
    r'^\s*(?P<time>\d{1,2}:\d{2}:\d{2}:\d+)\s+\S+\s+\d+\s+(?P<id>0x[0-9A-Fa-f]+|\d+)\s+\S+\s+(?P<dlc>\d+)(?:\s+(?P<data>(?:[0-9A-Fa-f]{2}(?:\s+[0-9A-Fa-f]{2})*)))?',
    re.IGNORECASE
)


class CANLogParser:
    """Stateful CAN log parser to avoid global state issues."""
    
    def __init__(self, dbc: cantools.database.Database) -> None:
        self.dbc = dbc
        self._start_seconds: Optional[float] = None
    
    def _bm_time_to_seconds(self, timestr: str) -> Optional[float]:
        """Convert BusMaster timestamp (HH:MM:SS:FFFF) to seconds."""
        # timestr like HH:MM:SS:FFFF  (fractional part variable length)
        parts = timestr.split(':')
        if len(parts) != 4:
            logger.warning(f"Invalid BusMaster time format: {timestr}")
            return None
        h, m, s, frac = parts
        try:
            h = int(h)
            m = int(m)
            s = int(s)
            frac = int(frac)
        except ValueError:
            logger.warning(f"Failed to parse BusMaster time components: {timestr}")
            return None
        # fractional seconds: interpret as fraction with length-dependent denominator
        denom = 10 ** (len(str(frac)))
        fractional = frac / denom
        total_seconds = h * 3600 + m * 60 + s + fractional
        logger.debug(f"Converted BusMaster time {timestr} to {total_seconds}s")
        return total_seconds
    
    def parse_line(self, line: str) -> Optional[Tuple[float, int, bytes]]:
        """
        Try to extract (timestamp: float, id: int, data: bytes) from a log line.
        Supports BusMaster-like logs (timestamps like HH:MM:SS:ffff) and simple formats.
        Returns (timestamp, arb_id, data_bytes) or None if can't parse.
        """
        line = line.strip()
        if not line:
            return None

        # Try BusMaster-style line first
        bm = BM_RE.match(line)
        if bm:
            time_str = bm.group('time')
            sec = self._bm_time_to_seconds(time_str)
            if sec is None:
                return None
            if self._start_seconds is None:
                self._start_seconds = sec
            timestamp = sec - self._start_seconds

            id_str = bm.group('id')
            if id_str.startswith('0x') or id_str.startswith('0X'):
                try:
                    arb_id = int(id_str, 16)
                except ValueError:
                    logger.warning(f"Failed to parse hex ID: {id_str}")
                    return None
            else:
                try:
                    arb_id = int(id_str, 10)
                except ValueError:
                    logger.warning(f"Failed to parse decimal ID: {id_str}")
                    return None

            data_str = bm.group('data') or ''
            try:
                data_bytes = parse_data_bytes(data_str)
            except Exception as e:
                logger.debug(f"Failed to parse data bytes: {e}")
                data_bytes = b''
            return timestamp, arb_id, data_bytes

        # Fallback to original parsing for other log styles

        # timestamp: first number float-like
        ts_match = TIMESTAMP_RE.search(line)
        if not ts_match:
            logger.debug(f"No timestamp found in line: {line[:50]}")
            return None
        try:
            timestamp = float(ts_match.group(1))
        except ValueError:
            logger.warning(f"Failed to convert timestamp to float: {ts_match.group(1)}")
            return None

        # arbitration id: try hex 0xNNN then decimal numbers after timestamp
        id_match = ID_HEX_RE.search(line)
        if id_match:
            arb_id = int(id_match.group(0), 16)
        else:
            # find decimal numbers; prefer a number after the timestamp
            rest = line[ts_match.end():]
            decs = ID_DEC_RE.findall(rest)
            if not decs:
                logger.debug(f"No CAN ID found in line: {line[:50]}")
                return None
            arb_id = int(decs[0])

        # data: find last hex sequence on the line
        hex_matches = HEX_PAIR_RE.findall(line)
        data_bytes = b''
        if hex_matches:
            # choose the last match that has at least 1 byte
            for hx in reversed(hex_matches):
                s = re.sub(r'[^0-9A-Fa-f]', '', hx)
                if len(s) >= 2:
                    try:
                        data_bytes = parse_data_bytes(hx)
                        break
                    except Exception as e:
                        logger.debug(f"Failed to parse hex data '{hx}': {e}")
                        continue

        return timestamp, arb_id, data_bytes
    
    def parse_log_file(self, path: str) -> Generator[Tuple[float, int, bytes], None, None]:
        """
        Generator yielding (timestamp, arb_id, data_bytes) for each parsed line.
        """
        # reset start seconds for each new file parse
        self._start_seconds = None
        lines_parsed = 0
        lines_failed = 0

        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    parsed = self.parse_line(line)
                    if parsed:
                        lines_parsed += 1
                        yield parsed
                    else:
                        lines_failed += 1
        except Exception as e:
            logger.error(f"Error reading log file '{path}': {e}")
            raise
        
        logger.info(f"Parsed log file: {lines_parsed} lines successful, {lines_failed} lines failed")


def parse_data_bytes(hex_str: str) -> bytes:
    """Convert hex string to bytes, handling various separators."""
    if not hex_str:
        return b''
    # remove separators
    s = re.sub(r'[^0-9A-Fa-f]', '', hex_str)
    # if odd length, assume a leading 0
    if len(s) % 2 != 0:
        s = '0' + s
    return bytes.fromhex(s)


def load_dbc(path: str) -> cantools.database.Database:
    """Load DBC database file."""
    logger.info(f"Loading DBC file: {path}")
    return cantools.database.load_file(path)


def discover_signals(dbc: cantools.database.Database) -> List[Tuple[int, str, str]]:
    """
    Return list of tuples: (frame_id: int, message_name: str, signal_name: str)
    """
    out = []
    for m in dbc.messages:
        for s in m.signals:
            out.append((m.frame_id, m.name, s.name))
    logger.info(f"Discovered {len(out)} signals in DBC")
    return out


def collect_signals(parser: CANLogParser, log_path: str, requested_entries: List[Tuple[int, str, str]]) -> Dict[str, List[Tuple[float, any]]]:
    """
    requested_entries: list of tuples (frame_id:int, signal_name:str, display_key:str)

    Returns dict display_key -> list of (timestamp, value)
    """
    series: Dict[str, List[Tuple[float, any]]] = {entry[2]: [] for entry in requested_entries}

    # pre-build mapping from message id -> message to speed up
    msg_by_id = {m.frame_id: m for m in parser.dbc.messages}

    # Build a lookup from (frame_id, signal_name) to display_key for quick append
    lookup = {(entry[0], entry[1]): entry[2] for entry in requested_entries}

    lines_decoded = 0
    lines_matched = 0

    for timestamp, arb_id, data in parser.parse_log_file(log_path):
        if not data:
            # skip lines with no data bytes
            continue
        msg = msg_by_id.get(arb_id)
        if not msg:
            continue
        try:
            decoded = parser.dbc.decode_message(arb_id, data)
            lines_decoded += 1
        except Exception as e:
            # cantools may raise if data length mismatches; ignore
            logger.debug(f"Failed to decode message 0x{arb_id:X}: {e}")
            continue

        # For each requested signal that belongs to this message id, append if present
        for (fid, sig_name), display in lookup.items():
            if fid != arb_id:
                continue
            if sig_name in decoded and decoded[sig_name] is not None:
                series[display].append((timestamp, decoded[sig_name]))
                lines_matched += 1

    logger.info(f"Decoded {lines_decoded} messages, matched {lines_matched} signal values")
    return series


def print_series(series: Dict[str, List[Tuple[float, any]]]) -> None:
    """
    Print a concise summary of collected data instead of dumping all samples.
    Summary includes per-signal count and duration, and an overall total duration
    and total number of data points.
    """
    total_points = 0
    starts = []
    ends = []

    for sig, data in series.items():
        if not data:
            print(f"{sig}: 0 points")
            continue
        # data is list of (timestamp, value)
        timestamps = [t for t, _ in data]
        start = min(timestamps)
        end = max(timestamps)
        count = len(timestamps)
        duration = end - start
        total_points += count
        starts.append(start)
        ends.append(end)
        print(f"{sig}: {count} points, duration={duration:.6f}s (start={start:.6f}, end={end:.6f})")

    if total_points == 0:
        print("No data collected.")
        return

    overall_start = min(starts) if starts else 0.0
    overall_end = max(ends) if ends else 0.0
    overall_duration = overall_end - overall_start
    print(f"\nOverall: total points={total_points}, total duration={overall_duration:.6f}s (start={overall_start:.6f}, end={overall_end:.6f})")


def _sanitize_sheet_name(name: str, existing_names: Dict[str, str]) -> str:
    """
    Excel sheet name restrictions: <=31 chars and cannot contain : \ / ? * [ ]
    Ensures unique sheet names by appending counter if needed.
    """
    s = re.sub(r'[:\\\/\?\*\[\]]', '_', name)
    s = s[:31]
    
    # Check for collisions
    if s not in existing_names.values():
        return s
    
    # Append counter to ensure uniqueness
    base = s[:27]
    idx = 1
    while f"{base}_{idx}" in existing_names.values():
        idx += 1
    return f"{base}_{idx}"


def export_to_excel(series: Dict[str, List[Tuple[float, any]]], excel_path: str) -> None:
    """
    Write each signal time series to its own sheet and create a single multiline
    scatter chart on the 'Plots' sheet with all selected signals plotted together.

    If xlsxwriter is not installed, the function will still write sheets but
    will skip creating the combined chart.
    """
    if pd is None:
        print("Pandas is required for Excel export. Install with: pip install pandas xlsxwriter", file=sys.stderr)
        return

    # ensure .xlsx extension
    if not excel_path.lower().endswith('.xlsx'):
        logger.warning("Output file should have .xlsx extension. Appending .xlsx")
        excel_path = excel_path + '.xlsx'

    if _XLSXWRITER_AVAILABLE:
        engine = 'xlsxwriter'
    else:
        engine = None
        print("xlsxwriter not available — charts will be skipped. Install with: pip install xlsxwriter", file=sys.stderr)

    try:
        if engine:
            with pd.ExcelWriter(excel_path, engine=engine) as writer:
                workbook = writer.book
                sheet_names = {}

                # write each series into its own sheet
                for display, data in series.items():
                    if not data:
                        logger.debug(f"Skipping empty series: {display}")
                        continue
                    
                    sheet = _sanitize_sheet_name(display, sheet_names)
                    sheet_names[display] = sheet

                    df = pd.DataFrame(data, columns=['timestamp', 'value'])
                    df = df.sort_values(by='timestamp', ignore_index=True)
                    df.to_excel(writer, sheet_name=sheet, index=False)
                    logger.debug(f"Wrote sheet '{sheet}' with {len(df)} rows")

                # create Plots sheet and add one combined scatter chart
                plots_ws = workbook.add_worksheet('Plots')

                # create single chart and add each signal as a series
                chart = workbook.add_chart({'type': 'scatter', 'subtype': 'straight_with_markers'})
                any_series = False
                for display, sheet in sheet_names.items():
                    df = pd.DataFrame(series[display], columns=['timestamp', 'value']).sort_values(by='timestamp', ignore_index=True)
                    nrows = len(df)
                    if nrows < 1:
                        continue

                    start_row = 2  # Excel rows are 1-based; header is row1, data starts at row2
                    end_row = nrows + 1
                    sheet_ref = f"'{sheet}'"
                    chart.add_series({
                        'name':       display,
                        'categories': f"={sheet_ref}!$A${start_row}:$A${end_row}",
                        'values':     f"={sheet_ref}!$B${start_row}:$B${end_row}",
                        'marker':     {'type': 'circle', 'size': 3},
                        'line':       {'width': 1},
                    })
                    any_series = True

                if any_series:
                    chart.set_title({'name': 'Combined Signals'})
                    chart.set_x_axis({'name': 'timestamp'})
                    chart.set_y_axis({'name': 'value'})
                    chart.set_legend({'position': 'bottom'})
                    # place a reasonably large chart on the Plots sheet
                    plots_ws.insert_chart('B2', chart, {'x_scale': 2.0, 'y_scale': 1.2})
                    logger.info(f"Created combined chart with {sum(1 for _ in sheet_names)} signals")

            print(f"Excel exported to: {excel_path}")
            logger.info(f"Excel file saved: {excel_path}")
        else:
            # fallback: write sheets only (no charts) using pandas default engine
            with pd.ExcelWriter(excel_path) as writer:
                for display, data in series.items():
                    if not data:
                        logger.debug(f"Skipping empty series: {display}")
                        continue
                    sheet = _sanitize_sheet_name(display, {})
                    df = pd.DataFrame(data, columns=['timestamp', 'value']).sort_values(by='timestamp', ignore_index=True)
                    df.to_excel(writer, sheet_name=sheet, index=False)
            print(f"Excel exported (no charts) to: {excel_path}")
            logger.info(f"Excel file saved (no charts): {excel_path}")
    except Exception as e:
        logger.error(f"Failed to create/save Excel file: {e}")
        print(f"Failed to create/save Excel file: {e}", file=sys.stderr)


def export_to_html(series: Dict[str, List[Tuple[float, any]]], html_path: str) -> None:
    """
    Write an interactive HTML file with a combined timeseries plot (all selected signals).
    Uses plotly if available. Skips non-numeric values.
    """
    try:
        import plotly.graph_objects as go  # type: ignore
        import plotly.io as pio  # type: ignore
    except Exception:
        print("Plotly is required for HTML export. Install with: pip install plotly", file=sys.stderr)
        return

    # ensure .html extension
    if not html_path.lower().endswith('.html'):
        html_path = html_path + '.html'

    fig = go.Figure()
    any_series = False
    for display, data in series.items():
        xs = []
        ys = []
        for t, v in data:
            # try to coerce to float; skip non-numeric values
            try:
                y = float(v) if not isinstance(v, bool) else float(int(v))
            except Exception:
                logger.debug(f"Skipping non-numeric value for {display}: {v}")
                continue
            xs.append(t)
            ys.append(y)
        if not xs:
            logger.debug(f"No numeric data for signal: {display}")
            continue
        fig.add_trace(go.Scatter(x=xs, y=ys, mode='lines+markers', name=display))
        any_series = True

    if not any_series:
        print("No numeric data available to plot in HTML.", file=sys.stderr)
        logger.warning("No numeric data available for HTML export")
        return

    fig.update_layout(
        title="Combined Signals",
        xaxis_title="timestamp",
        yaxis_title="value",
        legend=dict(orientation="h", y=-0.2),
        hovermode="closest",
    )

    try:
        pio.write_html(fig, file=html_path, full_html=True, include_plotlyjs='cdn')
        print(f"HTML exported to: {html_path}")
        logger.info(f"HTML file saved: {html_path}")
    except Exception as e:
        logger.error(f"Failed to write HTML file: {e}")
        print(f"Failed to write HTML file: {e}", file=sys.stderr)


def choose_signals_interactive(available: List[Tuple[int, str, str]]) -> List[Tuple[int, str, str]]:
    """
    available: list of (frame_id, message_name, signal_name)
    Returns list of selected tuples (frame_id, signal_name, display_key)

    Accepts:
     - comma separated indices: 0,2,5
     - ranges: 1-4
     - combination: 0,2-4,7
     - 'all' or 'a' to select everything
    """
    for i, (fid, mname, sname) in enumerate(available):
        print(f"[{i}] 0x{fid:X} {mname}.{sname}")

    if not sys.stdin.isatty():
        print("\nNon-interactive session — provide --signals or run interactively.", file=sys.stderr)
        sys.exit(0)

    sel = input("\nSelect signals by index (comma-separated), ranges allowed (e.g. 0,2-4), or 'all': ").strip()
    if not sel:
        print("No selection made.", file=sys.stderr)
        sys.exit(1)

    sel_lower = sel.lower()
    if sel_lower in ('all', 'a'):
        indices = list(range(len(available)))
    else:
        tokens = [t.strip() for t in sel.split(',') if t.strip()]
        indices_set = set()
        for tok in tokens:
            if '-' in tok:
                parts = tok.split('-', 1)
                if parts[0].isdigit() and parts[1].isdigit():
                    start = int(parts[0])
                    end = int(parts[1])
                    if start <= end:
                        rng = range(start, end + 1)
                    else:
                        rng = range(end, start + 1)
                    for idx in rng:
                        indices_set.add(idx)
                else:
                    print(f"Ignoring invalid range token: {tok}", file=sys.stderr)
            elif tok.isdigit():
                indices_set.add(int(tok))
            else:
                print(f"Ignoring invalid token: {tok}", file=sys.stderr)
        # filter out-of-range indices and sort
        indices = sorted(i for i in indices_set if 0 <= i < len(available))

    if not indices:
        print("No valid selections.", file=sys.stderr)
        sys.exit(1)

    chosen = []
    for idx in indices:
        fid, mname, sname = available[idx]
        chosen.append((fid, sname, f"{mname}.{sname}"))
    return chosen


def build_requested_from_names(dbc: cantools.database.Database, names: List[str]) -> List[Tuple[int, str, str]]:
    """
    names: list of signal name strings provided via --signals
    Return list of tuples (frame_id, signal_name, display_key). If a name matches multiple messages,
    all matches are included.
    """
    available = discover_signals(dbc)
    chosen = []
    name_set = set(names)
    found_names = set()
    
    for fid, mname, sname in available:
        if sname in name_set:
            chosen.append((fid, sname, f"{mname}.{sname}"))
            found_names.add(sname)
    
    # Log which names were not found
    missing = name_set - found_names
    if missing:
        logger.warning(f"Signals not found in DBC: {', '.join(missing)}")
    
    return chosen


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse CAN log with DBC and extract signal timeseries.")
    parser.add_argument('--dbc', required=True, help='Path to DBC file')
    parser.add_argument('--log', required=True, help='Path to CAN log file (CSV or text)')
    parser.add_argument('--signals', required=False, help='Comma-separated list of signal names to extract')
    parser.add_argument('--excel', required=False, help='Path to output Excel file (optional)')
    parser.add_argument('--html', required=False, help='Path to output interactive HTML file (optional)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        dbc = load_dbc(args.dbc)
    except Exception as e:
        logger.error(f"Failed to load DBC: {e}")
        print(f"Failed to load DBC: {e}", file=sys.stderr)
        sys.exit(2)

    # Create parser instance (avoids global state)
    can_parser = CANLogParser(dbc)
    available = discover_signals(dbc)

    requested_entries = []
    if args.signals:
        names = [s.strip() for s in args.signals.split(',') if s.strip()]
        if not names:
            logger.error("No signals requested.")
            print("No signals requested.", file=sys.stderr)
            sys.exit(1)
        requested_entries = build_requested_from_names(dbc, names)
        if not requested_entries:
            logger.error("No matching signals found in DBC for provided names.")
            print("No matching signals found in DBC for provided names.", file=sys.stderr)
            sys.exit(1)
    else:
        # interactive selection
        requested_entries = choose_signals_interactive(available)

    series = collect_signals(can_parser, args.log, requested_entries)
    print_series(series)

    if args.excel:
        export_to_excel(series, args.excel)

    if args.html:
        export_to_html(series, args.html)


if __name__ == "__main__":
    main()