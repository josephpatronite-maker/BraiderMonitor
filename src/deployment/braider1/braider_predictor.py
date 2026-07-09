"""
Wire Break Prediction Loop — braider_predictor.py
===================================================
Runs as a background thread inside braider_monitor.py. Polls PLC tags every
0.5 seconds, maintains a rolling 3-second window, and fires a prediction alert
when the signal profile matches the pre-fault signature observed in historical
hires_events data.

WHAT IT DOES:
- Runs entirely in RAM (no PLC writes, no machine commands — read only)
- Maintains a 6-sample (3-second at 0.5s resolution) rolling window
- Evaluates two thresholds every 0.5 seconds:
    1. Max velocity error spike in the window exceeds VEL_ERROR_THRESHOLD
    2. Speed ratio volatility in the window exceeds VOLATILITY_THRESHOLD
- When BOTH thresholds are exceeded simultaneously, fires a prediction
- Records every prediction to a CSV log (prediction_log.csv)
- Monitors for a real hires_event (ANY type) in the following
  VALIDATION_WINDOW_SECS (default 10 seconds) and records whether the
  prediction was correct
- Suppresses repeat predictions within COOLDOWN_SECS (default 15 seconds)

EVENT TYPE NOTE:
Both WIRE_BREAK and ESTOP_OR_ABORT hires events are treated as the same
validation target — a confirmed fault. This is because ESTOP_OR_ABORT events
typically represent wire breaks that the proximity sensor didn't catch in time,
not genuine manual operator stops. The distinction between the two event types
is which detection mechanism fired first, not a meaningful difference in the
underlying physical event. The prediction log records which type occurred for
reference, but both count as a validated true positive.

THRESHOLD CALIBRATION (from aggregated_braider_trends.csv analysis):
- Starting conservatively: VEL_ERROR_THRESHOLD = 0.15, VOLATILITY_THRESHOLD = 0.04
- These were derived from fault-event data only. Once normal-running baseline
  samples are added to the dataset, thresholds should be recalibrated against
  the full distribution (fault vs. normal) rather than fault-vs-fault.
- Lower thresholds = more sensitivity, more false positives
- Higher thresholds = fewer false positives, more missed events

PREDICTION LOG SCHEMA (Braider_X_prediction_log.csv):
  Timestamp                — when the prediction was made
  Braider_ID               — which braider
  Max_Vel_Error            — the spike value that triggered the alert
  Speed_Ratio_Volatility   — the volatility value at time of alert
  Validated                — TRUE / FALSE / PENDING
  Validation_Timestamp     — timestamp of the real hires_event that followed
  Hires_Event_Type         — WIRE_BREAK / ESTOP_OR_ABORT / NONE
  Notes                    — additional context
"""

import os
import csv
import time
import glob
import threading
import logging
from datetime import datetime, timedelta
from collections import deque

log = logging.getLogger(__name__)

# ── Tunable thresholds ────────────────────────────────────────────────────────
# Calibrated from normal-running baseline (40,237 windows) vs 100 fault events.
#
# PREDICTION LOGIC (OR):
#   Fires when EITHER condition is true:
#     A) VelErr >= VEL_HIGH_THRESHOLD                          (high spike alone)
#     B) VelErr >= VEL_MID_THRESHOLD  AND  Vol >= VOL_THRESHOLD (moderate spike + volatility)
#
# Rationale: normal running in the 0.03-0.08 vel band has near-zero volatility
# (avg=0.0007), while fault events in that same band average 0.055 volatility.
# The two populations are cleanly separated by volatility once velocity is
# in the moderate range — so the AND gate in condition B adds real information
# rather than just being a noise filter.
#
# Performance at these thresholds (vs 40,237 normal windows, 100 fault events):
#   Condition A alone (vel >= 0.08):              37% recall, 0.02% FP, 93% precision
#   Condition B alone (vel 0.03-0.08, vol>=0.035): 20 extra faults, 1 extra FP, 95% precision
#   Combined OR:                                  57% recall, 0.02% FP, ~95% precision
#
# Lower VEL_HIGH_THRESHOLD toward 0.06 if you want more sensitivity at the cost
# of a small precision drop. Lower VEL_MID_THRESHOLD toward 0.025 carefully —
# the false positive count rises sharply below 0.025 in the mid-vel band.
VEL_HIGH_THRESHOLD  = 0.08    # High-velocity spike: fires alone, no volatility needed
VEL_MID_THRESHOLD   = 0.03    # Moderate spike: only fires when volatility also elevated
VOL_THRESHOLD       = 0.035   # Volatility gate for moderate spikes (nearly zero in normal running)
WINDOW_SECS         = 3.0     # Rolling window length (seconds)
POLL_INTERVAL       = 0.5     # Poll rate (seconds) — matches HIRES loop
VALIDATION_WINDOW_SECS = 10.0 # How long to watch for a real event after prediction
COOLDOWN_SECS       = 15.0    # Minimum seconds between consecutive predictions

# PLC tags to read
# Puller_VelCmdErr is computed by the monitoring script on Braider 2
# (CommandVelocity - ActualVelocity) and stored as a derived tag in the CSV,
# but it doesn't exist as a real PLC tag on Braider 1. The predictor reads
# the two component tags directly and computes the same value at runtime,
# which works correctly on both braiders.
PRED_TAGS = [
    'servoPuller_Axis.CommandVelocity',
    'servoPuller_Axis.ActualVelocity',
    'realTableSpeed',
    'Puller_Actual_Speed',
    'Machine_State',
]


class PredictorLoop:
    """
    Wire break prediction loop. Instantiate once, pass to a daemon thread.
    Exposes latest_alert dict for the dashboard API endpoint.
    """

    def __init__(self, braider_id: str, log_dir: str, hires_events_dir: str, plc_ip: str):
        self.braider_id       = braider_id
        self.log_dir          = log_dir
        self.hires_events_dir = hires_events_dir
        self.plc_ip           = plc_ip

        self.prediction_log   = os.path.join(log_dir, f'{braider_id}_prediction_log.csv')
        self._lock            = threading.Lock()

        # Rolling window: deque of (timestamp, vel_error, speed_ratio) tuples
        # Max size = window length / poll interval
        maxlen = int(WINDOW_SECS / POLL_INTERVAL) + 1
        self._window = deque(maxlen=maxlen)

        # State
        self._last_prediction_time = None
        self._pending_validations  = []  # list of dicts waiting to be validated

        # Dashboard-accessible latest alert
        self.latest_alert = {
            'active':          False,
            'timestamp':       None,
            'max_vel_error':   None,
            'volatility':      None,
            'validated':       None,
            'hires_event_type': None,
        }

        self._ensure_log_header()

    def _ensure_log_header(self):
        """Create the prediction log file with headers if it doesn't exist."""
        if not os.path.exists(self.prediction_log):
            os.makedirs(self.log_dir, exist_ok=True)
            with open(self.prediction_log, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([
                    'Timestamp', 'Braider_ID', 'Max_Vel_Error',
                    'Speed_Ratio_Volatility', 'Validated',
                    'Validation_Timestamp', 'Hires_Event_Type', 'Notes'
                ])

    def run(self):
        """Main loop — call this from a daemon thread."""
        log.info(f'[{self.braider_id}] Predictor loop started '
                 f'(vel_high={VEL_HIGH_THRESHOLD}, vel_mid={VEL_MID_THRESHOLD}, vol={VOL_THRESHOLD})')

        from pycomm3 import LogixDriver

        while True:
            try:
                with LogixDriver(self.plc_ip) as plc:
                    log.info(f'[{self.braider_id}] Predictor: PLC connected at {self.plc_ip}')
                    while True:
                        t_start = time.monotonic()
                        self._tick(plc)
                        elapsed = time.monotonic() - t_start
                        sleep_for = max(0.0, POLL_INTERVAL - elapsed)
                        time.sleep(sleep_for)

            except Exception as e:
                log.warning(f'[{self.braider_id}] Predictor connection error: {e} — retrying in 5s')
                time.sleep(5)

    def _tick(self, plc):
        """Single poll cycle — read tags, update window, evaluate thresholds."""
        now = datetime.now()

        results = plc.read(*PRED_TAGS)
        if not isinstance(results, list):
            results = [results]

        # Parse tag values safely
        tag_vals = {r.tag: r.value for r in results if r.value is not None}

        cmd_vel      = tag_vals.get('servoPuller_Axis.CommandVelocity')
        act_vel      = tag_vals.get('servoPuller_Axis.ActualVelocity')
        table_speed  = tag_vals.get('realTableSpeed')
        pull_speed   = tag_vals.get('Puller_Actual_Speed')
        machine_state = tag_vals.get('Machine_State')

        # Only evaluate during active production — clears the window on any
        # non-running state so stale samples don't bleed across a stop/start
        if machine_state != 16:
            self._window.clear()
            return

        # Compute velocity command error — same calculation the monitoring
        # script uses to derive Puller_VelCmdErr for the CSV logs
        if cmd_vel is None or act_vel is None:
            return
        vel_err = cmd_vel - act_vel

        if table_speed is None or pull_speed is None:
            return

        # Speed ratio (same calculation as main loop)
        speed_ratio = (vel_err / pull_speed) if pull_speed and abs(pull_speed) > 0.001 else 0.0

        # Add to rolling window
        self._window.append((now, abs(vel_err), speed_ratio))

        # Need a full window before evaluating
        if len(self._window) < self._window.maxlen:
            return

        # Compute rolling features
        vel_errors   = [s[1] for s in self._window]
        speed_ratios = [s[2] for s in self._window]

        max_vel_error = max(vel_errors)
        avg_ratio     = sum(speed_ratios) / len(speed_ratios)
        volatility    = (sum((r - avg_ratio) ** 2 for r in speed_ratios) / len(speed_ratios)) ** 0.5

        # Check for pending validations before evaluating new predictions
        self._check_validations(now)

        # ── OR prediction logic ──────────────────────────────────────────────
        # Condition A: large velocity spike alone (high confidence)
        condition_a = max_vel_error >= VEL_HIGH_THRESHOLD
        # Condition B: moderate velocity spike + elevated volatility (together significant)
        condition_b = (VEL_MID_THRESHOLD <= max_vel_error < VEL_HIGH_THRESHOLD) and (volatility >= VOL_THRESHOLD)

        if condition_a or condition_b:
            trigger_condition = 'A (high spike)' if condition_a else 'B (moderate spike + volatility)'
            if (self._last_prediction_time is None or
                    (now - self._last_prediction_time).total_seconds() >= COOLDOWN_SECS):
                self._fire_prediction(now, max_vel_error, volatility, trigger_condition)

    def _fire_prediction(self, now: datetime, max_vel_error: float, volatility: float, trigger_condition: str):
        """Record a prediction and start watching for validation."""
        self._last_prediction_time = now
        ts_str = now.strftime('%Y-%m-%d %H:%M:%S')

        log.warning(f'[{self.braider_id}] ⚠ WIRE BREAK PREDICTION at {ts_str} '
                    f'(max_vel_err={max_vel_error:.4f}, volatility={volatility:.4f}, '
                    f'condition={trigger_condition})')

        row = {
            'Timestamp':              ts_str,
            'Braider_ID':             self.braider_id,
            'Max_Vel_Error':          round(max_vel_error, 6),
            'Speed_Ratio_Volatility': round(volatility, 6),
            'Validated':              'PENDING',
            'Validation_Timestamp':   '',
            'Hires_Event_Type':       '',
            'Notes':                  (f'Condition {trigger_condition} — '
                                       f'thresholds: high={VEL_HIGH_THRESHOLD}, '
                                       f'mid={VEL_MID_THRESHOLD}, vol={VOL_THRESHOLD}'),
        }
        self._append_log_row(row)

        # Track for validation
        validation_entry = {
            'prediction_ts':       now,
            'prediction_ts_str':   ts_str,
            'max_vel_error':       max_vel_error,
            'volatility':          volatility,
            'deadline':            now + timedelta(seconds=VALIDATION_WINDOW_SECS),
            'validated':           False,
        }
        self._pending_validations.append(validation_entry)

        # Update dashboard alert
        with self._lock:
            self.latest_alert = {
                'active':          True,
                'timestamp':       ts_str,
                'max_vel_error':   round(max_vel_error, 4),
                'volatility':      round(volatility, 4),
                'validated':       None,
                'hires_event_type': None,
            }

    def _check_validations(self, now: datetime):
        """
        Check if any pending predictions have been validated or timed out.

        Both WIRE_BREAK and ESTOP_OR_ABORT hires events count as a validated
        true positive — both represent mechanical faults (wire break or similar)
        regardless of which detection mechanism fired first.
        """
        if not self._pending_validations:
            return

        still_pending = []
        for entry in self._pending_validations:
            if entry['validated']:
                continue

            # Look for ANY hires_event file that appeared after the prediction
            # (both WIRE_BREAK and ESTOP_OR_ABORT count as true positives)
            hires_event, event_type = self._find_any_hires_event_after(entry['prediction_ts'])

            if hires_event:
                event_ts = self._extract_event_timestamp(hires_event)
                latency_secs = (datetime.strptime(event_ts, '%Y-%m-%d %H:%M:%S')
                                - entry['prediction_ts']).total_seconds() \
                               if event_ts else '?'
                self._update_log_row(
                    prediction_ts_str=entry['prediction_ts_str'],
                    validated='TRUE',
                    validation_ts=event_ts,
                    event_type=event_type,
                    notes=f'Fault confirmed ({event_type}) in ~{latency_secs:.1f}s — file: {os.path.basename(hires_event)}'
                )
                log.info(f'[{self.braider_id}] ✓ Prediction VALIDATED — {event_type} at {event_ts} '
                         f'(~{latency_secs:.1f}s after prediction)')
                entry['validated'] = True
                with self._lock:
                    self.latest_alert['validated']        = True
                    self.latest_alert['hires_event_type'] = event_type

            elif now > entry['deadline']:
                self._update_log_row(
                    prediction_ts_str=entry['prediction_ts_str'],
                    validated='FALSE',
                    validation_ts='',
                    event_type='NONE',
                    notes=f'No hires event (wire break or e-stop) within {VALIDATION_WINDOW_SECS}s — likely false positive'
                )
                log.info(f'[{self.braider_id}] ✗ Prediction NOT validated (false positive)')
                entry['validated'] = True
                with self._lock:
                    self.latest_alert['validated']        = False
                    self.latest_alert['hires_event_type'] = 'NONE'

            else:
                still_pending.append(entry)

        self._pending_validations = still_pending

    def _find_any_hires_event_after(self, after_dt: datetime):
        """
        Look for ANY hires_events CSV file (wire break OR e-stop/abort) that
        was created after after_dt. Both count as true positives since both
        represent mechanical faults regardless of which sensor detected it.
        Returns (filepath, event_type) or (None, None).
        Checks WIRE_BREAK first since that's the primary target event.
        """
        patterns = [
            (os.path.join(self.hires_events_dir, f'{self.braider_id}_hires_wire_break_*.csv'),     'WIRE_BREAK'),
            (os.path.join(self.hires_events_dir, f'{self.braider_id}_hires_estop_or_abort_*.csv'), 'ESTOP_OR_ABORT'),
        ]
        for pattern, event_type in patterns:
            for filepath in glob.glob(pattern):
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    if mtime > after_dt:
                        return filepath, event_type
                except OSError:
                    pass
        return None, None

    def _extract_event_timestamp(self, filepath: str) -> str:
        """Extract the timestamp string from a hires_event filename."""
        # e.g. Braider_1_hires_wire_break_20260624_082114.csv → 2026-06-24 08:21:14
        try:
            name = os.path.basename(filepath).replace('.csv', '')
            parts = name.split('_')
            date_str, time_str = parts[-2], parts[-1]
            dt = datetime.strptime(date_str + time_str, '%Y%m%d%H%M%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return ''

    def _append_log_row(self, row: dict):
        """Append a row to the prediction log CSV."""
        try:
            with open(self.prediction_log, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'Timestamp', 'Braider_ID', 'Max_Vel_Error',
                    'Speed_Ratio_Volatility', 'Validated',
                    'Validation_Timestamp', 'Hires_Event_Type', 'Notes'
                ])
                writer.writerow(row)
        except Exception as e:
            log.error(f'[{self.braider_id}] Predictor log write error: {e}')

    def _update_log_row(self, prediction_ts_str: str, validated: str,
                        validation_ts: str, event_type: str, notes: str):
        """
        Rewrite the PENDING row for prediction_ts_str with final validation result.
        Reads the full file, updates the matching row, rewrites.
        """
        try:
            rows = []
            with open(self.prediction_log, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    if (row['Timestamp'] == prediction_ts_str and
                            row['Validated'] == 'PENDING'):
                        row['Validated']           = validated
                        row['Validation_Timestamp'] = validation_ts
                        row['Hires_Event_Type']    = event_type
                        row['Notes']               = notes
                    rows.append(row)

            with open(self.prediction_log, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except Exception as e:
            log.error(f'[{self.braider_id}] Predictor log update error: {e}')
