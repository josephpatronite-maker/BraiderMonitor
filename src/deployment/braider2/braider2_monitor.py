"""
braider_monitor.py
Noble Gas Systems — Steeger HS120/48 IMC-7K Braider Monitor
Raspberry Pi production logger — runs as a systemd service

Logs:
  - process_log.csv           : 2s poll, full telemetry
  - oee_log.csv               : 60s poll, cumulative state time + recipe context
  - event_log.csv             : RBE — discrete transitions
  - wire_break_log.csv        : one row per wire break event
  - hires_events/<event>.csv  : 0.5s ring buffer flushed on wire break / e-stop
                                Contains 10s pre-event + 5s post-event at 0.5s resolution

Archiving:
  - process_log.csv      : weekly split every Sunday midnight (Pi clock)
  - event_log / oee_log / wire_break_log : monthly split on 1st of month

Flask dashboard at http://<pi-ip>:5000
Floor report   at http://<pi-ip>:5000/floor

Author: Joseph Patronite, Noble Gas Systems
"""

import csv
import os
import time
import threading
import logging
from datetime import datetime
from collections import deque
from pycomm3 import LogixDriver
from flask import Flask, jsonify, render_template_string

# ── Configuration ────────────────────────────────────────────────────────────

PLC_IP     = '192.168.1.102'
BRAIDER_ID = 'Braider_2'

LOG_DIR      = os.path.expanduser('~/braider_logs')
HIRES_LOG_DIR = os.path.join(LOG_DIR, 'hires_events')

PROCESS_LOG    = os.path.join(LOG_DIR, f'{BRAIDER_ID}_process_log.csv')
EVENT_LOG      = os.path.join(LOG_DIR, f'{BRAIDER_ID}_event_log.csv')
WIRE_BREAK_LOG = os.path.join(LOG_DIR, f'{BRAIDER_ID}_wire_break_log.csv')
OEE_LOG        = os.path.join(LOG_DIR, f'{BRAIDER_ID}_oee_log.csv')

# Polling intervals
FAST_POLL_INTERVAL  = 2.0   # seconds — process_log write cadence
HIRES_POLL_INTERVAL = 0.5   # seconds — ring buffer sample rate
OEE_POLL_INTERVAL   = 60    # seconds

# Pre/post event capture windows
PRE_EVENT_SECONDS  = 10     # seconds of history kept in RAM ring buffer
POST_EVENT_SECONDS = 5      # seconds captured after event fires

HIRES_RING_SIZE = int(PRE_EVENT_SECONDS / HIRES_POLL_INTERVAL)  # 20 rows

# Startup grace period
STARTUP_GRACE_SECONDS = 5

STATE_CODES = {
    0:   'OFF',
    1:   'STOPPED',
    2:   'STARTING',
    4:   'READY',
    16:  'RUNNING',
    32:  'STOPPING',
    64:  'PAUSING',
    128: 'PAUSED',
    256: 'ABORTING',
    512: 'ABORTED',
}

# ── Local:1:I.Data bit mapping ────────────────────────────────────────────────
# Source: HS120/48-2021-IMC-7K-NOBLE GAS electrical schematic, sheet 08 (PLC-DI 1.0-11)
# Local:1:I.Data is a packed 12-bit integer representing all embedded PLC digital inputs.
# Each bit maps to a specific physical input terminal on the 1769-L18ERM-BB1B controller.
#
# Normal running value = 3 (binary 011) = EStop_OK + Door_Closed
# Normal stopped/off   = 1 (binary 001) = EStop_OK only
#
# Wire break is NOT detected via this tag — use WIre_Break_Detected or Machine_Faults.
# Bit 10 (WireBreak_SW) reflects the WB1-WB4 series circuit on net 215 but the
# machine aborts too fast to catch it reliably at 2s polling.
#
# This tag is collected in process_log as a composite operator input snapshot.
# Decode with decode_io_bits() / decode_io_str() for human-readable active inputs.

IO_BIT_LABELS = {
    0:  'EStop_OK',          # I:1.1/00 — High = all E-stops released
    1:  'Door_Closed',       # I:1.1/01 — High = door interlock satisfied (DI3)
    2:  'Puller_SSW_Close',  # I:1.1/02 — Puller selector switch: close position
    3:  'Puller_SSW_Open',   # I:1.1/03 — Puller selector switch: open position
    4:  'Start_PB',          # I:1.1/04 — Start pushbutton pressed
    5:  'Stop_PB',           # I:1.1/05 — Stop pushbutton pressed
    6:  'Jog_Fwd',           # I:1.1/06 — Jog forward button pressed
    7:  'TakeUp_OL',         # I:1.1/07 — Take-up motor overload relay tripped
    8:  'Upper_Prox',        # I:1.1/08 — Upper proximity sensor active
    9:  'Lower_Prox',        # I:1.1/09 — Lower proximity sensor active
    10: 'WireBreak_SW',      # I:1.1/10 — WB1-WB4 series circuit on net 215
    11: 'Triaxial_SW',       # I:1.1/11 — Triaxial wire break microswitch
}

IO_NORMAL_RUNNING = 3   # EStop_OK + Door_Closed
IO_NORMAL_STOPPED = 1   # EStop_OK only


def decode_io_bits(value):
    """Return list of active input label strings for a Local:1:I.Data integer value."""
    if value is None or not isinstance(value, int) or value < 0:
        return []
    return [label for bit, label in IO_BIT_LABELS.items() if value & (1 << bit)]


def decode_io_str(value):
    """Return comma-separated string of active inputs, or INVALID for bad values."""
    if value is None or not isinstance(value, int) or value < 0:
        return 'INVALID'
    labels = decode_io_bits(value)
    return ', '.join(labels) if labels else 'NONE'

# ── Tag lists ─────────────────────────────────────────────────────────────────
#
# FAST_TAGS — 2s process telemetry written to process_log.csv
# Only true process signals that change frequently during production.
# OEE/recipe/config tags have been removed — they belong in oee_log or event_log.

FAST_TAGS = [
    # Core process speeds & position
    'Machine_State',
    'Puller_Actual_Speed',
    'Puller_Pos_Feet',
    'realTableSpeed',
    'Table_Position',
    'Active_Segment',
    # Wire break detection
    'Local:1:I.Data',
    'Local:1:I.Fault',
    'WIre_Break_Detected',
    'Core_Break',
    # Safety inputs (contextual — stay in process_log as state columns)
    'I_Door_Interlock_Ok',
    'I_Emergency_Stop_Ok',
    'Machine.Guards_Ok',
    'Machine.All_Safties_Ok',
    'Machine.All_Axes_Ok',
    'Machine.All_Axes_Running',
    # State elapsed time
    'Current_Hours.ACC',
    'Current_Minutes.ACC',
    'Current_Seconds.ACC',
    # VFD feedback
    'Table_Drive:I.OutputFreq',
    'Table_Drive:O.FreqCommand',
    'Table_Drive:I.Faulted',
    'Table_Drive:I.Active',
    'Table_Drive:I.AtReference',
    # Taper sensor
    'Taper_Sensor_Input',
    'Sensor_Mode_Enable',
    # Run state signals
    'Run_Complete',
    'Length_To_Run',
    'Transition_Active',
    'No_Machine_Msgs',
    'Machine_Faults',
    # Wire break recovery & events
    'WireBreak_Move',
    'EStop_Recover',
    'New_Part_Latch',
    'New_Part_ONS',
    'PPI_Change_ONS',
    'Puller_Position_Error',
    # Sequence / state machine
    'Sequence_Step',
    'PullerMasterAxis',
    # Program-scoped
    'Program:MainProgram.Fault_WireBreak',
    'Program:MainProgram.Fault_EStop',
    'Program:MainProgram.Fault_GuardDoor',
    'Program:MainProgram.Fault_PullerServo',
    'Program:MainProgram.Fault_TableServo',
    'Program:MainProgram.Recover_Step',
    'Program:MainProgram.Puller_Current_Dist',
    'Program:MainProgram.Table_Current_Dist',
    'Program:P01_TableDrive.Servo_Axis_Faults',
    # Servo axis sub-tags
    'servoPuller_Axis.ActualPosition',
    'servoPuller_Axis.CommandPosition',
    'servoPuller_Axis.ActualVelocity',
    'servoPuller_Axis.CommandVelocity',
    'servoPuller_Axis.MotionStatus',
    'servoPuller_Axis.TorqueReference',   # confirmed live on this drive config
    'servoTable_Axis.VelocityFeedback',
]

# OEE_TAGS — 60s poll written to oee_log.csv
# Recipe params and lifetime accumulators. Not included in FAST_TAGS to keep
# the 2s packet small.
OEE_TAGS = [
    'Machine_Statistics',
    'CurrentRecipe',
    'HMI_NumberCarriers',
    'HMI_Recipe_Number',
    'Recipe_Modified',
    'HMI_Mandrel_Mode',
    'PowerOn_Days.ACC',
    'PowerOn_Hours.ACC',
    'Triaxial_Enable',
    'Active_Segment',
    # Recipe params
    'Discrete_Distance',
    'Discrete_Loops',
    'Loop_Length_Feet',
    'Carrier_Mode',
    'Current_Ratio',
    'Low_PPI',
    'Hi_PPI',
    'Hi_PPI_Running',
    'Base_Ratio',
    # Fault flags — read at OEE cadence for CHANGE_TAGS RBE detection
    'Fault_9',
    'Fault_13',
    'Fault_Cam',
    'Fault_Calc',
    'Table_Homing_Error',
    'Calc_Error',
    'Cam_Error',
    # Rarely-changing inputs read at OEE cadence
    'I_Table_Motor_OL',
    'I_CoreBreak_Sensor',
    'I_Triaxial_WB',
    'AxisSynced_OS1',
    'AxisSynced_OS2',
    'AxisSynced_OS3',
    'AxisSynced_OS4',
    'AxisSynced_OS5',
    'PullerMasterAxis',
]

# CHANGE_TAGS — RBE tags written to event_log only on 0→1 or 1→0 transition.
# These are the 11 tags previously being polled every 2s into event_log.
# All are BOOLs unless otherwise noted.
CHANGE_TAGS = [
    # Wire break & core break
    'WIre_Break_Detected',
    'Core_Break',
    'WireBreak_Move',
    # Faults
    'Fault_9',
    'Fault_13',
    'Fault_Cam',
    'Fault_Calc',
    'Calc_Error',
    'Cam_Error',
    'Table_Homing_Error',
    'Puller_Position_Error',
    'Table_Drive:I.Faulted',
    # Machine_Faults DINT — logs on any value change
    'Machine_Faults',
    # Safety inputs (rarely changing — event_log only)
    'I_Table_Motor_OL',
    'I_Door_Interlock_Ok',
    'I_Emergency_Stop_Ok',
    'I_CoreBreak_Sensor',
    'I_Triaxial_WB',
    # Servo sync
    'AxisSynced_OS1',
    'AxisSynced_OS2',
    'AxisSynced_OS3',
    'AxisSynced_OS4',
    'AxisSynced_OS5',
    # Run lifecycle
    'Run_Complete',
    'New_Part_Latch',
    'New_Part_ONS',
    'EStop_Recover',
    'Transition_Active',
    'Sequence_Step',
    'PullerMasterAxis',
    # Sensor & PPI
    'PPI_Change_ONS',
    'Sensor_Mode_Enable',
    # Recipe config — log when operator changes recipe mid-shift
    'HMI_NumberCarriers',
    'HMI_Recipe_Number',
    'Recipe_Modified',
    'Triaxial_Enable',
    'Carrier_Mode',
    'Discrete_Distance',
    'Discrete_Loops',
    'Loop_Length_Feet',
    'Base_Ratio',
    'Current_Ratio',
    'Low_PPI',
    'Hi_PPI',
    # Messages
    'No_Machine_Msgs',
]

# HIRES_TAGS — polled at 0.5s into RAM ring buffer only.
# Written to disk ONLY when a wire break or e-stop event fires (pre+post window).
# Kept small to guarantee sub-500ms round-trip over EtherNet/IP.
# Focused on signals most likely to show pre-break signatures.
HIRES_TAGS = [
    'Machine_State',
    'WIre_Break_Detected',
    'Machine_Faults',
    'Local:1:I.Data',
    'Puller_Actual_Speed',
    'realTableSpeed',
    'Puller_Pos_Feet',
    'servoPuller_Axis.ActualVelocity',
    'servoPuller_Axis.CommandVelocity',
    'servoPuller_Axis.ActualPosition',
    'servoPuller_Axis.CommandPosition',
    'servoTable_Axis.VelocityFeedback',
    'Table_Drive:I.OutputFreq',
    'Table_Drive:O.FreqCommand',
    'Core_Break',
    'I_Emergency_Stop_Ok',
    'Program:MainProgram.Fault_WireBreak',
    'Program:MainProgram.Fault_EStop',
    # Added after live verification (confirmed actively changing on puller axis;
    # TorqueReference moved 28/29 polls vs. flat OutputCurrent in the same sample) —
    # candidate for an earlier-than-velocity-error wire-break precursor signal.
    'servoPuller_Axis.TorqueReference',
    'Program:P02_PullerServo.ServoStatus.FilteredTorque',
    'Program:P02_PullerServo.ServoStatus.Motor_RPM',
    # Fault/alarm bits — cannot be evaluated by steady-state sampling since they're
    # designed to stay False except during an actual fault. Added cheaply here so the
    # next several real wire-break events in hires_events provide the real ML-relevance
    # test (did any of these fire in the pre/post window?) rather than guessing in advance.
    'servoPuller_Axis.ExcessivePositionErrorFault',
    'servoPuller_Axis.ExcessiveVelocityErrorFault',
    'servoPuller_Axis.OvertorqueLimitFault',
    'servoPuller_Axis.UndertorqueLimitFault',
    'servoPuller_Axis.AxisFault',
    'servoPuller_Axis.MotionFaultStatus',
    'Program:MainProgram.Fault_SERCOS',
    'Program:MainProgram.Fault_StartingTimeout',
    'Program:MainProgram.Table_Drive_Fault',
    'Program:P02_PullerServo.Servo_Axis_Faults',
    'Machine.Any_Stop_Pressed',
    'Machine.Any_Start_Pressed',
]


from logging.handlers import RotatingFileHandler

LOG_FILE = os.path.join(os.path.expanduser('~'), 'braider_monitor.log')

_rotating_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,
    backupCount=3
)
_rotating_handler.setFormatter(logging.Formatter('%(asctime)s  %(levelname)s  %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[
        _rotating_handler,
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

os.makedirs(LOG_DIR, exist_ok=True)


def ts():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def state_name(code):
    return STATE_CODES.get(code, f'UNKNOWN({code})')


# ── Independent archiver thread ───────────────────────────────────────────────
# Runs entirely off the Pi's system clock.  The PLC loop has zero involvement.
# Sunday midnight  → weekly split of process_log.csv
# 1st of month     → monthly split of event_log, oee_log, wire_break_log

_last_archive_check_minute = -1   # guards against multiple fires in same minute


def independent_archiver_loop():
    """
    DISABLED — archiving is now handled by cron via /home/pi/braider_archive.sh.
    This thread stub exists so the thread start call below doesn't error out.
    Crontab entries (set with: crontab -e):
      5 0 * * 0   /home/pi/braider_archive.sh weekly  >> /home/pi/braider_logs/archive_cron.log 2>&1
      5 0 1 * *   /home/pi/braider_archive.sh monthly >> /home/pi/braider_logs/archive_cron.log 2>&1
    """
    log.info('Archiver thread started (cron mode — in-process archiver disabled).')
    # Thread exits immediately; cron handles all archiving externally.
    return


def _run_archive_checks(now: datetime):
    """DISABLED — see independent_archiver_loop docstring."""
    pass


# ── Hires event notifier — dashboard alert when a fault event is captured ─────
_hires_alert_lock   = threading.Lock()
_hires_latest_alert = {
    'active':      False,
    'timestamp':   None,
    'event_type':  None,
    'puller_feet': None,
}

def _watch_hires_events():
    """Background thread: watches hires_events folder for new files."""
    seen = set(os.listdir(HIRES_LOG_DIR)) if os.path.exists(HIRES_LOG_DIR) else set()
    while True:
        time.sleep(1)
        try:
            if not os.path.exists(HIRES_LOG_DIR):
                continue
            current = set(os.listdir(HIRES_LOG_DIR))
            new_files = current - seen
            for fname in sorted(new_files):
                if not fname.endswith('.csv'):
                    seen.add(fname)
                    continue
                event_type = 'WIRE_BREAK' if 'wire_break' in fname else 'ESTOP_OR_ABORT'
                puller_feet = None
                try:
                    fpath = os.path.join(HIRES_LOG_DIR, fname)
                    with open(fpath, newline='', encoding='utf-8') as f:
                        import csv as _csv
                        rows = list(_csv.DictReader(f))
                    trigger = min(
                        (r for r in rows if r.get('Hires_Offset_s') not in ('', None)),
                        key=lambda r: abs(float(r.get('Hires_Offset_s', '999'))),
                        default=None
                    )
                    if trigger:
                        pf = trigger.get('Puller_Pos_Feet')
                        puller_feet = round(float(pf), 1) if pf else None
                except Exception:
                    pass
                with _hires_alert_lock:
                    _hires_latest_alert.update({
                        'active':      True,
                        'timestamp':   ts(),
                        'event_type':  event_type,
                        'puller_feet': puller_feet,
                    })
                log.info(f'[{BRAIDER_ID}] Hires event alert: {event_type} at {puller_feet} ft')
                seen.add(fname)
        except Exception as e:
            log.warning(f'Hires event watcher error: {e}')

hires_watcher_thread = threading.Thread(target=_watch_hires_events, daemon=True, name='hires_watcher')
hires_watcher_thread.start()


# ── CSV writer with instant column-update archive ────────────────────────────

def write_csv_row(filepath, row: dict):
    """
    Append a dict as a CSV row.  If the column schema has changed since the
    file was created (i.e. a FAST_TAGS edit mid-run), the old file is
    immediately archived with a timestamp and a fresh file is started.
    """
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0

    if file_exists:
        try:
            with open(filepath, 'r', newline='') as f:
                existing_headers = f.readline().strip().split(',')
            new_headers = list(row.keys())
            if existing_headers != new_headers:
                archive_name = filepath.replace(
                    '.csv',
                    f'_archived_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                )
                os.rename(filepath, archive_name)
                log.warning(
                    f'Column mismatch — archived {os.path.basename(filepath)} '
                    f'→ {os.path.basename(archive_name)}'
                )
                file_exists = False
        except OSError:
            file_exists = False

    with open(filepath, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ── Wire break parser ──────────────────────────────────────────────────────────





# ── Daily utilization helper ──────────────────────────────────────────────────

def calculate_daily_state_percentages():
    """
    Reads process_log.csv bottom-up and today's archived files to calculate
    state percentage breakdown for the current calendar day.
    """
    import glob
    today_str = datetime.now().strftime('%Y-%m-%d')
    state_counts = {}
    total_rows = 0

    # Scan live file bottom-up
    if os.path.exists(PROCESS_LOG) and os.path.getsize(PROCESS_LOG) > 0:
        try:
            # Read header first to find State_Name column index
            with open(PROCESS_LOG, 'r', encoding='utf-8', errors='replace') as f:
                header_line = f.readline().strip()
            headers = header_line.split(',')
            state_col = headers.index('State_Name') if 'State_Name' in headers else 3
        except Exception:
            state_col = 3

        with open(PROCESS_LOG, 'rb') as f:
            try:
                f.seek(0, os.SEEK_END)
                position = f.tell()
                buffer = bytearray()
                chunk_size = 4096
                done = False

                while position > 0 and not done:
                    if position - chunk_size > 0:
                        position -= chunk_size
                        f.seek(position)
                        chunk = f.read(chunk_size)
                    else:
                        chunk = f.read(position)
                        position = 0

                    buffer = chunk + buffer
                    lines = buffer.split(b'\n')

                    if position > 0:
                        buffer = lines[0]
                        lines = lines[1:]
                    else:
                        buffer = bytearray()

                    for line in reversed(lines):
                        line_str = line.decode('utf-8', errors='ignore').strip()
                        if not line_str or line_str.startswith('Timestamp'):
                            continue
                        parts = line_str.split(',')
                        if len(parts) > state_col:
                            row_timestamp = parts[0]
                            if row_timestamp.startswith(today_str):
                                # Treat a blank State_Name as UNKNOWN (counted in both
                                # numerator and denominator) rather than silently excluding
                                # the row from total_rows — matches /floor's accounting so
                                # the two pages always agree on running %.
                                sname = parts[state_col] or 'UNKNOWN'
                                state_counts[sname] = state_counts.get(sname, 0) + 1
                                total_rows += 1
                            else:
                                done = True
                                break
            except Exception as e:
                log.error(f'Daily OEE calc error: {e}')

    # Add any mid-shift archived files from today
    today_compact = datetime.now().strftime('%Y%m%d')
    base = PROCESS_LOG.replace('.csv', '')
    for archive in sorted(glob.glob(base + '_archived_' + today_compact + '*.csv')):
        try:
            with open(archive, newline='', encoding='utf-8', errors='replace') as af:
                for row in csv.DictReader(af):
                    if row.get('Timestamp', '').startswith(today_str):
                        sname = row.get('State_Name', 'UNKNOWN') or 'UNKNOWN'
                        state_counts[sname] = state_counts.get(sname, 0) + 1
                        total_rows += 1
        except Exception:
            pass

    if total_rows == 0:
        return {}
    return {state: round((count / total_rows) * 100, 1) for state, count in state_counts.items()}


# ── Shared state ──────────────────────────────────────────────────────────────

_lock = threading.Lock()
_latest = {
    'timestamp':              None,
    'machine_state':          None,
    'state_name':             None,
    'table_speed':            None,
    'puller_speed':           None,
    'puller_pos_feet':        None,
    'table_position':         None,
    'active_segment':         None,
    'no_faults':              None,
    'wire_break_bits':        None,
    'recipe_name':            None,
    'recipe_ppi':             None,
    'door_ok':                None,
    'estop_ok':               None,
    'guards_ok':              None,
    'all_axes_running':       None,
    'axis1_synced':           None,
    'axis2_synced':           None,
    'axis3_synced':           None,
    'axis4_synced':           None,
    'axis5_synced':           None,
    'current_state_hrs':      None,
    'current_state_mins':     None,
    'current_state_secs':     None,
    'state_elapsed_s':        None,
    'cum_running_hrs':        None,
    'cum_stopped_hrs':        None,
    'cum_ready_hrs':          None,
    'recipe_modified':        None,
    'mandrel_mode':           None,
    'taper_sensor':           None,
    'sensor_mode':            None,
    'sequence_step':          None,
    'recover_position':       None,
    'lamp_running':           None,
    'lamp_stopped':           None,
    'ok_to_jog':              None,
    'jog_fwd':                None,
    'jog_rev':                None,
    'start_pb':               None,
    'master_no_motion':       None,
    'machine_msg_scroll':     None,
    'tablepos_endofseg':      None,
    'tablepos_endoflastseg':  None,
    'puller_vel_cmd_err':     None,
    'puller_current_dist':    None,
    'table_current_dist':     None,
    'fault_wire_break':       None,
    'fault_estop':            None,
    'fault_guard_door':       None,
    'fault_puller_servo':     None,
    'fault_table_servo':      None,
    'wire_break_detected':    None,
    'recover_step':           None,
    'puller_servo_ok':        None,
    'table_servo_ok':         None,
    'group_fault':            None,
    'group_status':           None,
    'speed_ratio':            None,
    'machine_faults':         None,
    'no_msgs':                None,
    'vfd_freq_actual':        None,
    'vfd_freq_command':       None,
    'vfd_freq_delta':         None,
    'vfd_at_ref':             None,
    'vfd_faulted':            None,
    'vfd_active':             None,
    'i_table_motor_ol':       None,
    'i_triaxial_wb':          None,
    'core_break':             None,
    'all_safties_ok':         None,
    'all_axes_ok':            None,
    'puller_pos_error':       None,
    'new_part':               None,
    'run_complete':           None,
    'length_to_run':          None,
    'wire_break_move':        None,
    'estop_recover':          None,
    'abs_value_peak':         None,
    'ave_current_data':       None,
    'servo_axis_faults':      None,
    'puller_actual_vel':      None,
    'puller_cmd_vel':         None,
    'puller_actual_pos':      None,
    'puller_cmd_pos':         None,
    'puller_motion_status':   None,
    'table_vel_feedback':     None,
    'connected':              False,
    'last_error':             None,
    'daily_state_pcts':       {},
}

_rolling_buffer = deque(maxlen=HIRES_RING_SIZE)  # kept for dashboard compatibility

# ── Hi-res ring buffer ────────────────────────────────────────────────────────
# Populated by hires_loop() at 0.5s. Never written to disk unless an event fires.
_hires_ring      = deque(maxlen=HIRES_RING_SIZE)   # 20 rows = 10s at 0.5s
_hires_lock      = threading.Lock()

# Event capture state — set by monitor_loop, consumed by hires_loop
_hires_event      = threading.Event()
_hires_event_meta = {}


# ── Hi-res capture loop ───────────────────────────────────────────────────────

def flush_hires_event(pre_rows, post_rows, event_type, event_ts):
    """Write pre+post event rows to a timestamped CSV in hires_events/."""
    os.makedirs(HIRES_LOG_DIR, exist_ok=True)
    safe_ts  = event_ts.replace(':', '').replace(' ', '_').replace('-', '')
    filename = f'{BRAIDER_ID}_hires_{event_type.lower()}_{safe_ts}.csv'
    filepath = os.path.join(HIRES_LOG_DIR, filename)

    # Ground-truth check — scan the FULL window (pre + post) for an actual PLC-confirmed
    # wire break signal. This is independent of event_type/filename, which only reflects
    # which trigger fired the capture, not what physically happened. Always use this
    # column (not the filename) when building ML training labels.
    all_source_rows = list(pre_rows) + list(post_rows)
    confirmed_wire_break = any(
        bool(r.get('Wire_Break_Detected')) or bool(r.get('Fault_WireBreak'))
        for r in all_source_rows
    )
    confirmed_machine_fault_code = next(
        (r.get('Machine_Faults') for r in all_source_rows
         if r.get('Machine_Faults') not in (None, 0, 4)),
        None
    )

    all_rows = []
    for i, row in enumerate(pre_rows):
        r = dict(row)
        r['Event_Trigger']         = event_type     # WIRE_BREAK or ESTOP_OR_ABORT — capture trigger, NOT ground truth
        r['Confirmed_Wire_Break']  = confirmed_wire_break   # ground truth — use this for ML labels
        r['Confirmed_Fault_Code']  = confirmed_machine_fault_code
        r['Hires_Phase']           = 'PRE'
        r['Hires_Offset_s']        = round((i - len(pre_rows)) * HIRES_POLL_INTERVAL, 2)
        all_rows.append(r)
    for i, row in enumerate(post_rows):
        r = dict(row)
        r['Event_Trigger']         = event_type
        r['Confirmed_Wire_Break']  = confirmed_wire_break
        r['Confirmed_Fault_Code']  = confirmed_machine_fault_code
        r['Hires_Phase']           = 'POST'
        r['Hires_Offset_s']        = round((i + 1) * HIRES_POLL_INTERVAL, 2)
        all_rows.append(r)

    if not all_rows:
        return

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

    log.info(
        f'Hires event saved: {filename} '
        f'({len(pre_rows)} pre + {len(post_rows)} post rows) '
        f'confirmed_wire_break={confirmed_wire_break}'
    )


def hires_loop():
    """
    Polls HIRES_TAGS at 0.5s into an in-memory ring buffer using its OWN
    independent LogixDriver connection — never shares a socket with monitor_loop.

    The CompactLogix supports multiple simultaneous CIP sessions. Two connections
    is the correct solution; sharing one socket across threads causes CIP
    request-response interleaving which corrupts both reads.

    Never writes to disk during normal operation.
    On _hires_event signal: flushes ring buffer (PRE) + captures 5s POST to CSV.
    """
    log.info('Hires loop started — establishing own PLC connection.')
    retry_delay = 5

    def _build_row(d, phase='RING', offset=0.0):
        ps  = d.get('Puller_Actual_Speed')
        ts_ = d.get('realTableSpeed')
        pv  = d.get('servoPuller_Axis.ActualVelocity')
        cv  = d.get('servoPuller_Axis.CommandVelocity')
        st  = d.get('Machine_State')
        return {
            'Timestamp':           ts(),
            'Braider_ID':          BRAIDER_ID,
            'Machine_State':       st,
            'State_Name':          state_name(st) if st else '',
            'Wire_Break_Detected': d.get('WIre_Break_Detected'),
            'Machine_Faults':      d.get('Machine_Faults'),
            'Wire_Break_Bits':     d.get('Local:1:I.Data'),
            'IO_Decoded':          decode_io_str(d.get('Local:1:I.Data')),
            'Puller_Speed':        round(ps, 6)  if ps  else None,
            'Table_Speed':         round(ts_, 6) if ts_ else None,
            'Speed_Ratio':         round(ps / ts_, 6) if ps and ts_ and ts_ > 0 else None,
            'Puller_Pos_Feet':     d.get('Puller_Pos_Feet'),
            'Puller_ActualVel':    pv,
            'Puller_CmdVel':       cv,
            'Puller_VelCmdErr':    round(cv - pv, 6) if cv is not None and pv is not None else None,
            'Puller_ActualPos':    d.get('servoPuller_Axis.ActualPosition'),
            'Puller_CmdPos':       d.get('servoPuller_Axis.CommandPosition'),
            'Table_VelFeedback':   d.get('servoTable_Axis.VelocityFeedback'),
            'VFD_Freq_Actual':     d.get('Table_Drive:I.OutputFreq'),
            'VFD_Freq_Command':    d.get('Table_Drive:O.FreqCommand'),
            'Core_Break':          d.get('Core_Break'),
            'Estop_Ok':            d.get('I_Emergency_Stop_Ok'),
            'Fault_WireBreak':     d.get('Program:MainProgram.Fault_WireBreak'),
            'Fault_EStop':         d.get('Program:MainProgram.Fault_EStop'),
            'Recipe_Name':         _oee_derived.get('recipe_name'),
            'Active_Segment':      _latest.get('active_segment'),
            'State_Elapsed_Secs':  _latest.get('state_elapsed_s'),
            'Any_Stop_Pressed':    d.get('Machine.Any_Stop_Pressed'),
            'Any_Start_Pressed':   d.get('Machine.Any_Start_Pressed'),
            'TorqueReference':     d.get('servoPuller_Axis.TorqueReference'),
            'Hires_Phase':         phase,
            'Hires_Offset_s':      offset,
        }

    while True:
        try:
            with LogixDriver(PLC_IP) as hplc:
                if not hplc.connected:
                    raise ConnectionError('Hires LogixDriver connected=False')
                log.info('Hires loop PLC connected (own session).')
                retry_delay = 5

                while True:
                    t_poll = time.perf_counter()

                    results = hplc.read(*HIRES_TAGS)
                    d       = {r.tag: r.value for r in results if r.error is None}
                    row     = _build_row(d)

                    with _hires_lock:
                        _hires_ring.append(row)

                    # Event triggered by monitor_loop?
                    if _hires_event.is_set():
                        _hires_event.clear()
                        with _hires_lock:
                            meta     = dict(_hires_event_meta)
                            pre_rows = list(_hires_ring)

                        event_type = meta.get('type', 'EVENT')
                        event_ts_  = meta.get('timestamp', ts())
                        log.info(
                            f'Hires capture: {event_type} — '
                            f'{len(pre_rows)} pre-rows, capturing {POST_EVENT_SECONDS}s post'
                        )

                        post_rows = []
                        post_end  = time.time() + POST_EVENT_SECONDS
                        offset    = 1
                        while time.time() < post_end:
                            try:
                                pr = hplc.read(*HIRES_TAGS)
                                pd = {r.tag: r.value for r in pr if r.error is None}
                                post_rows.append(
                                    _build_row(pd, 'POST', round(offset * HIRES_POLL_INTERVAL, 2))
                                )
                                offset += 1
                            except Exception as e:
                                log.warning(f'Hires post-capture read error: {e}')
                            time.sleep(HIRES_POLL_INTERVAL)

                        # Tag pre-rows with correct negative offsets
                        n = len(pre_rows)
                        for i, r in enumerate(pre_rows):
                            r['Hires_Phase']    = 'PRE'
                            r['Hires_Offset_s'] = round((i - n) * HIRES_POLL_INTERVAL, 2)

                        flush_hires_event(pre_rows, post_rows, event_type, event_ts_)

                    # Sleep only remaining time in the 0.5s budget
                    elapsed = time.perf_counter() - t_poll
                    remaining = HIRES_POLL_INTERVAL - elapsed
                    if remaining > 0:
                        time.sleep(remaining)

        except Exception as e:
            log.warning(f'Hires loop connection error: {e} — retrying in {retry_delay}s')
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)


# ── OEE shared state — populated by oee_loop, read by monitor_loop ───────────
_oee_lock     = threading.Lock()
_oee_data     = {}          # latest od values
_oee_derived  = {           # parsed values ready to use in process_row/event
    'recipe_name':    'Unknown',
    'recipe_ppi':     None,
    'cum_running':    None,
    'cum_stopped':    None,
    'cum_ready':      None,
    'recipe_modified': None,
    'mandrel_mode':   None,
}
_oee_pending_write = threading.Event()   # set when a new OEE row is ready to write
_oee_row_buffer    = {}                  # the row to write, set by oee_loop


def oee_loop():
    """
    Polls OEE_TAGS on its own independent PLC connection every 60s.
    Parses the UDT, updates _oee_data and _oee_derived under _oee_lock,
    then signals _oee_pending_write so monitor_loop can flush to CSV
    on its next 2s cycle (keeping CSV writes single-threaded).
    """
    global _oee_data, _oee_derived, _oee_row_buffer

    log.info('OEE loop started — establishing own PLC connection.')
    retry_delay = 10

    while True:
        try:
            with LogixDriver(PLC_IP) as oplc:
                if not oplc.connected:
                    raise ConnectionError('OEE LogixDriver connected=False')
                log.info('OEE loop PLC connected (own session).')
                retry_delay = 10

                # Poll immediately on connect, then every 60s
                last_poll = 0

                while True:
                    now_oee = time.time()
                    if last_poll == 0 or (now_oee - last_poll >= OEE_POLL_INTERVAL):
                        results = oplc.read(*OEE_TAGS)
                        od = {r.tag: r.value for r in results if r.error is None}

                        # Parse UDTs
                        stats       = od.get('Machine_Statistics', {})
                        recipe_raw  = od.get('CurrentRecipe', {})
                        cum_running = cum_stopped = cum_ready = None
                        recipe_name_local = 'Unknown'
                        recipe_ppi_local  = None

                        if isinstance(stats, dict):
                            cum = stats.get('Cum_State_Time', {})
                            if isinstance(cum, dict):
                                cum_running = cum.get('Running', {}).get('Hours')
                                cum_stopped = cum.get('Stopped', {}).get('Hours')
                                cum_ready   = cum.get('Ready',   {}).get('Hours')

                        if isinstance(recipe_raw, dict):
                            recipe_name_local = recipe_raw.get('Name', 'Unknown')
                            hi_ppi     = od.get('Hi_PPI')
                            hi_running = od.get('Hi_PPI_Running')
                            try:
                                if hi_running == 1 and hi_ppi is not None:
                                    recipe_ppi_local = hi_ppi
                                else:
                                    segments  = recipe_raw.get('Segments', [])
                                    seg_idx   = int(od.get('Active_Segment') or 1)
                                    seg_data  = segments[seg_idx] if segments else None
                                    seg_picks = seg_data.get('Picks') if seg_data else None
                                    recipe_ppi_local = seg_picks if (seg_picks and seg_picks > 0) else recipe_raw.get('Connector_PPI')
                            except Exception:
                                recipe_ppi_local = recipe_raw.get('Connector_PPI')

                        # Update shared state under lock
                        with _oee_lock:
                            _oee_data = od
                            _oee_derived.update({
                                'recipe_name':     recipe_name_local,
                                'recipe_ppi':      recipe_ppi_local,
                                'cum_running':     cum_running,
                                'cum_stopped':     cum_stopped,
                                'cum_ready':       cum_ready,
                                'recipe_modified': od.get('Recipe_Modified'),
                                'mandrel_mode':    od.get('HMI_Mandrel_Mode'),
                            })
                            _oee_row_buffer = {
                                'Timestamp':          ts(),
                                'Braider_ID':         BRAIDER_ID,
                                'Machine_State':      None,   # filled in by monitor_loop
                                'State_Name':         None,
                                'Active_Segment':     od.get('Active_Segment'),
                                'Recipe_Name':        recipe_name_local,
                                'Recipe_Number':      od.get('HMI_Recipe_Number'),
                                'Recipe_PPI':         recipe_ppi_local,
                                'Recipe_Modified':    od.get('Recipe_Modified'),
                                'Mandrel_Mode':       od.get('HMI_Mandrel_Mode'),
                                'Carriers':           od.get('HMI_NumberCarriers'),
                                'Discrete_Distance':  od.get('Discrete_Distance'),
                                'Discrete_Loops':     od.get('Discrete_Loops'),
                                'Loop_Length_Feet':   od.get('Loop_Length_Feet'),
                                'Carrier_Mode':       od.get('Carrier_Mode'),
                                'Current_Ratio':      od.get('Current_Ratio'),
                                'Base_Ratio':         od.get('Base_Ratio'),
                                'Low_PPI':            od.get('Low_PPI'),
                                'Hi_PPI':             od.get('Hi_PPI'),
                                'Triaxial_Enable':    od.get('Triaxial_Enable'),
                                'PowerOn_Days':       od.get('PowerOn_Days.ACC'),
                                'PowerOn_Hours':      od.get('PowerOn_Hours.ACC'),
                                'Cum_Running_Hrs':    cum_running,
                                'Cum_Stopped_Hrs':    cum_stopped,
                                'Cum_Ready_Hrs':      cum_ready,
                                'Puller_Life_Ft':     stats.get('Puller_Life_Ft')     if isinstance(stats, dict) else None,
                                'Table_Life_1k_Revs': stats.get('Table_Life_1k_Revs') if isinstance(stats, dict) else None,
                            }

                        _oee_pending_write.set()
                        last_poll = now_oee
                        log.debug('OEE poll complete.')

                    time.sleep(1)

        except Exception as e:
            log.warning(f'OEE loop error: {e} — retrying in {retry_delay}s')
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 120)


def monitor_loop():

    prev_state     = None
    prev_wire_bits = None
    prev_change    = {}
    recipe_name    = 'Unknown'
    recipe_ppi     = None
    running_started_at = None

    monitor_loop._retry_count = 0

    log.info(f'Starting monitor loop → PLC {PLC_IP}')

    while True:
        try:
            with LogixDriver(PLC_IP) as plc:
                if not plc.connected:
                    raise ConnectionError('LogixDriver connected=False')

                log.info('PLC connected.')
                monitor_loop._retry_count = 0
                with _lock:
                    _latest['connected']   = True
                    _latest['last_error']  = None

                while True:
                    now       = time.time()
                    timestamp = ts()

                    # ── Fast poll ────────────────────────────────────────────
                    results = plc.read(*FAST_TAGS)
                    d = {r.tag: r.value for r in results if r.error is None}

                    machine_state = d.get('Machine_State')
                    puller_speed  = d.get('Puller_Actual_Speed')
                    puller_feet   = d.get('Puller_Pos_Feet')
                    table_pos     = d.get('Table_Position')
                    active_seg    = d.get('Active_Segment')
                    # no_faults removed — use Machine_Faults == 0 or 4 instead
                    wire_bits     = d.get('Local:1:I.Data')
                    table_speed   = d.get('realTableSpeed')      # filtered table rev/s

                    speed_ratio = None
                    if table_speed and puller_speed and table_speed > 0:
                        speed_ratio = round(puller_speed / table_speed, 6)

                    state_elapsed_s = None
                    ch = d.get('Current_Hours.ACC')
                    cm = d.get('Current_Minutes.ACC')
                    cs = d.get('Current_Seconds.ACC')
                    if ch is not None and cm is not None and cs is not None:
                        state_elapsed_s = (ch * 3600) + (cm * 60) + round(cs / 1000, 1)

                    puller_vel_cmd_err = None
                    pav = d.get('servoPuller_Axis.ActualVelocity')
                    pcv = d.get('servoPuller_Axis.CommandVelocity')
                    if pcv is not None and pav is not None:
                        puller_vel_cmd_err = pcv - pav

                    # ── Read OEE derived values (non-blocking from oee_loop) ──
                    with _oee_lock:
                        oee = dict(_oee_derived)
                        # Flush pending OEE row if oee_loop produced one
                        if _oee_pending_write.is_set():
                            _oee_pending_write.clear()
                            pending_oee = dict(_oee_row_buffer)
                        else:
                            pending_oee = None

                    recipe_name = oee.get('recipe_name', recipe_name)
                    recipe_ppi  = oee.get('recipe_ppi',  recipe_ppi)

                    if pending_oee:
                        pending_oee['Machine_State'] = machine_state
                        pending_oee['State_Name']    = state_name(machine_state) if machine_state is not None else ''
                        write_csv_row(OEE_LOG, pending_oee)

                    # ── Process log row ──────────────────────────────────────
                    process_row = {
                        'Timestamp':             timestamp,
                        'Braider_ID':            BRAIDER_ID,
                        'Machine_State':         machine_state,
                        'State_Name':            state_name(machine_state) if machine_state is not None else '',
                        'Table_Speed':           round(table_speed, 6)  if table_speed  else None,
                        'Puller_Speed':          round(puller_speed, 6) if puller_speed else None,
                        'Speed_Ratio':           speed_ratio,
                        'Puller_Pos_Feet':       round(puller_feet, 4)  if puller_feet  else None,
                        'Table_Position':        round(table_pos, 4)    if table_pos    else None,
                        'Active_Segment':        active_seg,
                        'State_Elapsed_Secs':    state_elapsed_s,
                        'Machine_Faults':        d.get('Machine_Faults'),
                        # Wire break
                        'Wire_Break_Bits':       wire_bits,
                        'IO_Decoded':            decode_io_str(wire_bits),
                        'Wire_Break_Detected':   d.get('WIre_Break_Detected'),
                        'Core_Break':            d.get('Core_Break'),
                        # Safety context
                        'Door_Ok':               d.get('I_Door_Interlock_Ok'),
                        'Estop_Ok':              d.get('I_Emergency_Stop_Ok'),
                        'Guards_Ok':             d.get('Machine.Guards_Ok'),
                        'All_Safties_Ok':        d.get('Machine.All_Safties_Ok'),
                        'All_Axes_Ok':           d.get('Machine.All_Axes_Ok'),
                        'All_Axes_Running':      d.get('Machine.All_Axes_Running'),
                        # Recipe context (from oee_loop)
                        'Recipe_Name':           recipe_name,
                        'Recipe_PPI':            recipe_ppi,
                        # VFD
                        'VFD_Freq_Actual':       d.get('Table_Drive:I.OutputFreq'),
                        'VFD_Freq_Command':      d.get('Table_Drive:O.FreqCommand'),
                        'VFD_Freq_Delta':        (
                            (d.get('Table_Drive:O.FreqCommand') or 0) -
                            (d.get('Table_Drive:I.OutputFreq')  or 0)
                        ),
                        'VFD_Active':            d.get('Table_Drive:I.Active'),
                        'VFD_AtReference':       d.get('Table_Drive:I.AtReference'),
                        # Run state
                        'Taper_Sensor':          d.get('Taper_Sensor_Input'),
                        'Length_To_Run':         d.get('Length_To_Run'),
                        'Run_Complete':          d.get('Run_Complete'),
                        'EStop_Recover':         d.get('EStop_Recover'),
                        'Sequence_Step':         d.get('Sequence_Step'),
                        'PullerMasterAxis':      d.get('PullerMasterAxis'),
                        'New_Part_Latch':        d.get('New_Part_Latch'),
                        'Puller_Position_Error': d.get('Puller_Position_Error'),
                        # Program-scoped faults
                        'Fault_WireBreak':       d.get('Program:MainProgram.Fault_WireBreak'),
                        'Fault_EStop':           d.get('Program:MainProgram.Fault_EStop'),
                        'Fault_GuardDoor':       d.get('Program:MainProgram.Fault_GuardDoor'),
                        'Fault_PullerServo':     d.get('Program:MainProgram.Fault_PullerServo'),
                        'Fault_TableServo':      d.get('Program:MainProgram.Fault_TableServo'),
                        'Puller_Current_Dist':   d.get('Program:MainProgram.Puller_Current_Dist'),
                        'Table_Current_Dist':    d.get('Program:MainProgram.Table_Current_Dist'),
                        # Servo sub-tags
                        'Puller_ActualVel':      d.get('servoPuller_Axis.ActualVelocity'),
                        'Puller_CmdVel':         d.get('servoPuller_Axis.CommandVelocity'),
                        'Puller_VelCmdErr':      round(puller_vel_cmd_err, 6) if puller_vel_cmd_err is not None else None,
                        'Puller_ActualPos':      d.get('servoPuller_Axis.ActualPosition'),
                        'Puller_CmdPos':         d.get('servoPuller_Axis.CommandPosition'),
                        'Puller_MotionStatus':   d.get('servoPuller_Axis.MotionStatus'),
                        'Table_VelFeedback':     d.get('servoTable_Axis.VelocityFeedback'),
                    }
                    write_csv_row(PROCESS_LOG, process_row)
                    _rolling_buffer.append(process_row.copy())

                    # ── Wire break detection → event_log + wire_break_log ────
                    # Primary signal: WIre_Break_Detected (PLC-managed BOOL, typo intentional)
                    # Local:1:I.Data is operator input context only — not used for detection.
                    # Startup grace period prevents false triggers during servo settling.
                    # Checked BEFORE state-change so WIRE_BREAK always wins the hires
                    # capture label when both conditions occur in the same poll cycle.
                    in_startup = (
                        running_started_at is not None and
                        (now - running_started_at) < STARTUP_GRACE_SECONDS
                    )

                    wire_break_detected = d.get('WIre_Break_Detected')
                    prev_wb_detected    = prev_change.get('WIre_Break_Detected')
                    wire_break_fired_this_cycle = False

                    if ((machine_state == 16 or prev_state == 16) and not in_startup and
                            wire_break_detected and not prev_wb_detected):
                        io_context = decode_io_str(wire_bits)
                        log.warning(
                            f'WIRE BREAK at {puller_feet:.2f} ft | '
                            f'IO={wire_bits} ({io_context})'
                        )
                        _write_event(timestamp, 'WIRE_BREAK',
                                     from_val='0',
                                     to_val='1',
                                     from_code=0,
                                     to_code=1,
                                     puller_feet=puller_feet,
                                     recipe_name=recipe_name,
                                     d=d,
                                     detail=f'IO_bits={wire_bits} ({io_context})')

                        wb_row = {
                            'Timestamp':        timestamp,
                            'Braider_ID':       BRAIDER_ID,
                            'IO_Raw':           wire_bits,
                            'IO_Decoded':       io_context,
                            'Puller_Feet':      round(puller_feet, 4) if puller_feet else None,
                            'Machine_State':    machine_state,
                            'State_Name':       state_name(machine_state),
                            'Recipe_Name':      recipe_name,
                            'Recipe_PPI':       recipe_ppi,
                            'Active_Segment':   active_seg,
                            'Machine_Faults':   d.get('Machine_Faults'),
                            'Recover_Step':     d.get('Program:MainProgram.Recover_Step'),
                            'Table_Speed':      round(table_speed, 4) if table_speed else None,
                            'Puller_Speed':     round(puller_speed, 4) if puller_speed else None,
                        }
                        write_csv_row(WIRE_BREAK_LOG, wb_row)

                        # Wire break always wins the hires label — claim it first,
                        # regardless of whether a state-change abort also fires this cycle.
                        with _hires_lock:
                            _hires_event_meta.update({
                                'type':      'WIRE_BREAK',
                                'timestamp': timestamp,
                            })
                        _hires_event.set()
                        wire_break_fired_this_cycle = True

                    prev_wire_bits = wire_bits

                    # ── State change → event_log ─────────────────────────────
                    if machine_state != prev_state and prev_state is not None:
                        _write_event(timestamp, 'STATE_CHANGE',
                                     from_val=state_name(prev_state),
                                     to_val=state_name(machine_state) if machine_state is not None else '',
                                     from_code=prev_state,
                                     to_code=machine_state,
                                     puller_feet=puller_feet,
                                     recipe_name=recipe_name,
                                     d=d)
                        log.info(f'State: {state_name(prev_state)} → {state_name(machine_state)}')

                        # Trigger hires capture on unexpected stops from RUNNING.
                        # Skip if wire break already claimed this cycle's capture above —
                        # wire break is always the more specific, higher-priority root cause.
                        if prev_state == 16 and machine_state in (256, 512, 32):
                            # RUNNING → ABORTING / ABORTED / STOPPING = unplanned stop
                            if not wire_break_fired_this_cycle and not _hires_event.is_set():
                                with _hires_lock:
                                    _hires_event_meta.update({
                                        'type':      'ESTOP_OR_ABORT',
                                        'timestamp': timestamp,
                                    })
                                _hires_event.set()
                                log.info(f'Hires capture triggered: unplanned stop {state_name(prev_state)} → {state_name(machine_state)}')
                            elif wire_break_fired_this_cycle:
                                log.info('Unplanned stop also detected this cycle — already captured under WIRE_BREAK label.')

                    if machine_state == 16 and prev_state != 16:
                        running_started_at = now
                    prev_state = machine_state

                    # ── RBE CHANGE_TAGS → event_log ──────────────────────────
                    with _oee_lock:
                        oee_snap = dict(_oee_data)
                    _combined = {**oee_snap, **d}  # fast poll (d) takes precedence

                    for tag in CHANGE_TAGS:
                        current_val = _combined.get(tag)
                        if current_val is None:
                            continue
                        if isinstance(current_val, int) and current_val in (-32767, -32768, -32000):
                            continue  # pycomm3 I/O fault — ignore
                        last_val = prev_change.get(tag)
                        if last_val is None:
                            prev_change[tag] = current_val
                            continue
                        if current_val != last_val:
                            if isinstance(current_val, bool) or current_val in (0, 1):
                                transition = 'TRIGGERED' if current_val else 'CLEARED'
                            else:
                                transition = f'CHANGED ({last_val} → {current_val})'

                            _write_event(timestamp, f'TAG_{tag}',
                                         from_val=str(last_val),
                                         to_val=str(current_val),
                                         from_code=last_val,
                                         to_code=current_val,
                                         puller_feet=puller_feet,
                                         recipe_name=recipe_name,
                                         d=d,
                                         detail=transition)
                            log.info(f'RBE: {tag} {transition}')
                            prev_change[tag] = current_val

                    # ── Update Flask shared state ────────────────────────────
                    with _lock:
                        _latest.update({
                            'timestamp':           timestamp,
                            'machine_state':       machine_state,
                            'state_name':          state_name(machine_state) if machine_state is not None else 'Unknown',
                            'table_speed':         round(table_speed, 4)  if table_speed  else None,
                            'puller_speed':        round(puller_speed, 4) if puller_speed else None,
                            'speed_ratio':         speed_ratio,
                            'puller_pos_feet':     round(puller_feet, 2)  if puller_feet  else None,
                            'table_position':      round(table_pos, 2)    if table_pos    else None,
                            'active_segment':      active_seg,
                            'machine_faults':      d.get('Machine_Faults'),
                            'no_faults':           (d.get('Machine_Faults') in (None, 0, 4)),
                            'wire_break_bits':     wire_bits,
                            'io_decoded':          decode_io_bits(wire_bits),
                            'recipe_name':         recipe_name,
                            'recipe_ppi':          recipe_ppi,
                            'door_ok':             d.get('I_Door_Interlock_Ok'),
                            'estop_ok':            d.get('I_Emergency_Stop_Ok'),
                            'guards_ok':           d.get('Machine.Guards_Ok'),
                            'all_safties_ok':      d.get('Machine.All_Safties_Ok'),
                            'all_axes_ok':         d.get('Machine.All_Axes_Ok'),
                            'all_axes_running':    d.get('Machine.All_Axes_Running'),
                            'axis1_synced':        d.get('AxisSynced_OS1'),
                            'axis2_synced':        d.get('AxisSynced_OS2'),
                            'axis3_synced':        d.get('AxisSynced_OS3'),
                            'axis4_synced':        d.get('AxisSynced_OS4'),
                            'axis5_synced':        d.get('AxisSynced_OS5'),
                            'current_state_hrs':   ch,
                            'current_state_mins':  cm,
                            'current_state_secs':  round(cs / 1000, 0) if cs else None,
                            'state_elapsed_s':     state_elapsed_s,
                            'cum_running_hrs':     oee.get('cum_running'),
                            'cum_stopped_hrs':     oee.get('cum_stopped'),
                            'cum_ready_hrs':       oee.get('cum_ready'),
                            'recipe_modified':     oee.get('recipe_modified'),
                            'mandrel_mode':        oee.get('mandrel_mode'),
                            'taper_sensor':        d.get('Taper_Sensor_Input'),
                            'sensor_mode':         d.get('Sensor_Mode_Enable'),
                            'vfd_freq_actual':     d.get('Table_Drive:I.OutputFreq'),
                            'vfd_freq_command':    d.get('Table_Drive:O.FreqCommand'),
                            'vfd_freq_delta':      (
                                (d.get('Table_Drive:O.FreqCommand') or 0) -
                                (d.get('Table_Drive:I.OutputFreq')  or 0)
                            ),
                            'vfd_faulted':         d.get('Table_Drive:I.Faulted'),
                            'vfd_at_ref':          d.get('Table_Drive:I.AtReference'),
                            'vfd_active':          d.get('Table_Drive:I.Active'),
                            'no_msgs':             d.get('No_Machine_Msgs'),
                            'core_break':          d.get('Core_Break'),
                            'i_table_motor_ol':    d.get('I_Table_Motor_OL'),
                            'i_triaxial_wb':       d.get('I_Triaxial_WB'),
                            'length_to_run':       d.get('Length_To_Run'),
                            'run_complete':        d.get('Run_Complete'),
                            'transition_active':   d.get('Transition_Active'),
                            'estop_recover':       d.get('EStop_Recover'),
                            'fault_wire_break':    d.get('Program:MainProgram.Fault_WireBreak'),
                            'fault_estop':         d.get('Program:MainProgram.Fault_EStop'),
                            'fault_guard_door':    d.get('Program:MainProgram.Fault_GuardDoor'),
                            'fault_puller_servo':  d.get('Program:MainProgram.Fault_PullerServo'),
                            'fault_table_servo':   d.get('Program:MainProgram.Fault_TableServo'),
                            'recover_step':        d.get('Program:MainProgram.Recover_Step'),
                            'puller_current_dist': d.get('Program:MainProgram.Puller_Current_Dist'),
                            'table_current_dist':  d.get('Program:MainProgram.Table_Current_Dist'),
                            'servo_axis_faults':   d.get('Program:P01_TableDrive.Servo_Axis_Faults'),
                            'puller_actual_vel':   d.get('servoPuller_Axis.ActualVelocity'),
                            'puller_cmd_vel':      d.get('servoPuller_Axis.CommandVelocity'),
                            'puller_vel_cmd_err':  round(puller_vel_cmd_err, 5) if puller_vel_cmd_err is not None else None,
                            'puller_actual_pos':   d.get('servoPuller_Axis.ActualPosition'),
                            'puller_cmd_pos':      d.get('servoPuller_Axis.CommandPosition'),
                            'puller_motion_status':d.get('servoPuller_Axis.MotionStatus'),
                            'table_vel_feedback':  d.get('servoTable_Axis.VelocityFeedback'),
                            'connected':           True,
                            'daily_state_pcts':    calculate_daily_state_percentages(),
                        })

                    time.sleep(FAST_POLL_INTERVAL)

        except KeyboardInterrupt:
            log.info('Shutdown requested.')
            break
        except Exception as e:
            with _lock:
                _latest['connected']  = False
                _latest['last_error'] = str(e)

            monitor_loop._retry_count += 1
            if monitor_loop._retry_count <= 3:
                wait = 10
                log.error(f'Connection lost: {e}')
            elif monitor_loop._retry_count <= 10:
                wait = 60
                if monitor_loop._retry_count == 4:
                    log.warning('PLC unreachable — switching to 60s retry interval.')
            else:
                wait = 300
                if monitor_loop._retry_count % 12 == 0:
                    log.warning('PLC still unreachable — retrying every 5 min.')

            time.sleep(wait)


# ── Event log helper ──────────────────────────────────────────────────────────

def _write_event(timestamp, event_type, *, from_val, to_val, from_code,
                 to_code, puller_feet, recipe_name, d, detail=''):
    """Write one row to event_log.csv."""
    row = {
        'Timestamp':   timestamp,
        'Braider_ID':  BRAIDER_ID,
        'Event_Type':  event_type,
        'From_Value':  from_val,
        'To_Value':    to_val,
        'From_Code':   from_code,
        'To_Code':     to_code,
        'Puller_Feet': round(puller_feet, 4) if puller_feet else None,
        'Recipe_Name': recipe_name,
        'Machine_State': d.get('Machine_State'),
        'State_Name':  state_name(d.get('Machine_State')) if d.get('Machine_State') else '',
        'Estop_Ok':    d.get('I_Emergency_Stop_Ok'),
        'Door_Ok':     d.get('I_Door_Interlock_Ok'),
        'No_Faults':   d.get('Machine_Faults') in (None, 0, 4),
        'Machine_Faults': d.get('Machine_Faults'),
        'Detail':      detail,
    }
    write_csv_row(EVENT_LOG, row)


# ── Flask dashboard ───────────────────────────────────────────────────────────
# (HTML/JS unchanged from original — only the Python data routes matter here)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Braider Monitor — Noble Gas Systems</title>
    <style>
        body  { font-family: monospace; background:#1a1a1a; color:#e0e0e0; padding:20px; margin:0; }
        h1    { color:#4fc3f7; margin-bottom:4px; }
        .sub  { color:#888; font-size:13px; margin-bottom:20px; }
        .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; }
        .card { background:#2a2a2a; border-radius:8px; padding:14px; }
        .label{ font-size:10px; color:#888; text-transform:uppercase; letter-spacing:1px; }
        .value{ font-size:26px; font-weight:bold; margin-top:4px; line-height:1.1; }
        .unit { font-size:12px; color:#888; margin-top:2px; }
        .section { font-size:11px; color:#555; text-transform:uppercase; letter-spacing:2px; margin:20px 0 8px; }
        .running { color:#66bb6a; } .stopped { color:#ef5350; } .paused  { color:#ffa726; }
        .fault   { color:#ef5350; } .ok      { color:#66bb6a; } .warn    { color:#ffa726; }
        .blink   { animation:blink 1s step-start infinite; }
        @keyframes blink { 50%{opacity:0} }
        .conn { font-size:12px; margin-top:20px; color:#888; }
        .axes { display:flex; gap:8px; margin-top:6px; flex-wrap:wrap; }
        .axis { padding:3px 8px; border-radius:4px; font-size:12px; font-weight:bold; }
        .axis-ok   { background:#1b5e20; color:#66bb6a; }
        .axis-warn { background:#b71c1c; color:#ef9a9a; }
        .checks { font-size:14px; line-height:2; }
    </style>
</head>
<body>
    <h1>Braider Monitor — {{ braider_id }}</h1>
    <div class="sub">
        Noble Gas Systems — Steeger HS120/48 &nbsp;|&nbsp;
        PLC {{ plc_ip }} &nbsp;|&nbsp;
        Updated: <span id="header-timestamp">{{ d.timestamp or '—' }}</span>
    </div>

    <!-- Fault Event Alert Banner -->
    <div id="hiresAlert" style="display:none; background:#b71c1c; color:#fff; padding:10px 16px;
         border-radius:6px; margin:10px 0; font-family:monospace; font-size:13px;
         border-left:4px solid #ef5350; animation: hiresAlertPulse 1s infinite;">
      ⚠ FAULT EVENT CAPTURED &nbsp;|&nbsp;
      <span id="hiresType"></span> &nbsp;|&nbsp;
      Position: <span id="hiresFeet"></span> ft &nbsp;|&nbsp;
      <span id="hiresTime"></span>
    </div>
    <style>
      @keyframes hiresAlertPulse { 0%,100%{opacity:1} 50%{opacity:0.65} }
    </style>

    <div class="section">Machine</div>
    <div class="grid">

        <div class="card">
            <div class="label">State</div>
            <div id="state-div" class="value {% if d.machine_state == 16 %}running
                              {% elif d.machine_state in (256,512) %}fault blink
                              {% elif d.machine_state in (64,128) %}paused
                              {% else %}stopped{% endif %}">
                <span id="state-value">{{ d.state_name or '—' }}</span>
            </div>
            <div class="unit">
                code {{ d.machine_state or 0 }} &nbsp;|&nbsp;
                <span id="elapsed-value">{% if d.state_elapsed_s %}{{ (d.state_elapsed_s // 3600)|int }}h {{ ((d.state_elapsed_s % 3600) // 60)|int }}m{% endif %}</span>
            </div>
        </div>

        <div class="card">
            <div class="label">Recipe</div>
            <div class="value" style="font-size:22px">{{ d.recipe_name or '—' }}</div>
            <div class="unit">
                PPI: {{ d.recipe_ppi }}
                {% if d.recipe_modified %}&nbsp;<span class="warn">modified</span>{% endif %}
                {% if d.mandrel_mode %}&nbsp;| mandrel{% endif %}
                {% if d.sensor_mode %}&nbsp;| <span class="ok">sensor</span>{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Production — This Session</div>
            <div class="value" id="feet-value">{{ d.puller_pos_feet or '—' }}</div>
            <div class="unit">
                feet
                {% if d.run_complete %}&nbsp;<span class="ok">✓ RUN COMPLETE</span>{% endif %}
            </div>
        </div>

        <div class="card" style="min-width:280px;">
            <div class="label">Daily Utilization</div>
            <div style="display:flex; align-items:center; gap:14px; margin-top:6px;">
                <canvas id="oeePieCanvas" width="110" height="110" style="flex-shrink:0;"></canvas>
                <div>
                    <div id="oee-value" class="value" style="font-size:26px; margin-bottom:4px;">—</div>
                    <div id="oee-legend" style="font-size:10px; line-height:1.6; color:#aaa;"></div>
                </div>
            </div>
        </div>

    </div>

    <div class="section">Process</div>
    <div class="grid">

        <div class="card">
            <div class="label">Table Speed — Actual</div>
            <div class="value" id="table-value">{{ d.table_rpm_actual or '0' }}</div>
            <div class="unit">
                rpm &nbsp;|&nbsp;
                <span id="table-vfd-actual" style="color:#4fc3f7">—</span> VFD Hz×10 actual
            </div>
            <div class="unit" style="margin-top:4px; color:#666;">
                command: <span id="table-vfd-command">—</span> Hz×10 &nbsp;|&nbsp;
                <span id="table-revs-value" title="realTableSpeed — may hold last value after stop">—</span> rev/s (PLC calc, may lag on stop)
            </div>
        </div>

        <div class="card">
            <div class="label">Puller Speed</div>
            <div class="value" id="puller-value">{{ d.puller_speed or '—' }}</div>
            <div class="unit">in/s</div>
        </div>

        <div class="card">
            <div class="label">Speed Ratio</div>
            <div class="value" style="font-size:20px">
                <span id="ratio-value">{% if d.speed_ratio %}{{ "%.5f"|format(d.speed_ratio) }}{% else %}—{% endif %}</span>
            </div>
            <div class="unit">puller ÷ table</div>
        </div>

        <div class="card">
            <div class="label">Taper Sensor</div>
            <div class="value" style="font-size:22px">
                <span id="taper-value">{% if d.taper_sensor %}{{ "%.2f"|format(d.taper_sensor) }}{% else %}—{% endif %}</span>
            </div>
            <div class="unit" id="taper-unit">{% if d.sensor_mode %}sensor active{% else %}sensor off{% endif %} — units TBD</div>
        </div>

        <div class="card">
            <div class="label">Puller Vel Error (cmd−actual)</div>
            <div class="value" style="font-size:22px">
                <span id="puller-vel-err">{% if d.puller_vel_cmd_err is not none %}{{ '%.5f'|format(d.puller_vel_cmd_err) }}{% else %}—{% endif %}</span>
            </div>
            <div class="unit">rev/s · tension proxy</div>
        </div>

        <div class="card">
            <div class="label">Table Vel Feedback (raw encoder)</div>
            <div class="value" style="font-size:22px">
                <span id="table-vel-fb">{% if d.table_vel_feedback is not none %}{{ '%.4f'|format(d.table_vel_feedback) }}{% else %}—{% endif %}</span>
            </div>
            <div class="unit">rev/s · wire break pre-detection</div>
        </div>

        <div class="card">
            <div class="label">VFD — Actual / Command</div>
            <div class="value" style="font-size:18px">
                <span id="vfd-actual">{{ d.vfd_freq_actual or '—' }}</span> / <span id="vfd-command">{{ d.vfd_freq_command or '—' }}</span>
            </div>
            <div class="unit">
                Hz×10 &nbsp;|&nbsp; delta: <span id="vfd-delta">{{ d.vfd_freq_delta or 0 }}</span>
                <span id="vfd-at-ref">{% if d.vfd_at_ref %}&nbsp;<span class="ok">AT REF</span>{% endif %}</span>
                {% if d.vfd_faulted %}&nbsp;<span class="fault blink">VFD FAULT</span>{% endif %}
            </div>
        </div>

    </div>

    <div class="section">Faults &amp; Safety</div>
    <div class="grid">

        <div class="card" style="grid-column: span 2;">
            <div class="label">Operator Inputs — Local:1:I.Data</div>
            <div style="display:flex; align-items:baseline; gap:12px; margin-top:6px;">
                <div id="wb-div" class="value ok" style="font-size:22px; min-width:40px;">
                    <span id="wb-value">{{ d.wire_break_bits if d.wire_break_bits is not none else '—' }}</span>
                </div>
                <div id="wb-binary" style="font-family:monospace; font-size:13px; color:#4fc3f7; letter-spacing:2px;">—</div>
            </div>
            <div id="io-decoded" style="margin-top:10px;"></div>
            <div class="unit" style="margin-top:6px;">normal running = 3 &nbsp;(EStop_OK + Door_Closed)</div>
        </div>

        <div class="card">
            <div class="label">Machine Faults</div>
            <div id="fault-div" class="value {% if d.no_faults %}ok{% else %}fault blink{% endif %}">
                {% if d.no_faults %}NONE{% else %}FAULT{% endif %}
            </div>
            <div id="fault-unit" class="unit">
                {% if d.machine_faults and d.machine_faults != 4 %}code: {{ d.machine_faults }}{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Safety Inputs</div>
            <div class="checks" style="font-size:13px;">
                <span id="safety-estop" class="{{ 'ok' if d.estop_ok else 'fault blink' }}">
                    {{ '✓' if d.estop_ok else '✗' }} E-Stop OK
                </span><br>
                <span id="safety-door" class="{{ 'ok' if d.door_ok else 'warn' }}">
                    {{ '✓' if d.door_ok else '✗' }} Door Closed
                </span><br>
                <span id="safety-guards" class="{{ 'ok' if d.guards_ok else 'fault' }}">
                    {{ '✓' if d.guards_ok else '✗' }} Guards OK
                </span><br>
                <span id="safety-motor" class="{{ 'fault blink' if d.i_table_motor_ol else 'ok' }}">
                    {{ '✗ Motor OL' if d.i_table_motor_ol else '✓ Motor OK' }}
                </span><br>
                <span id="safety-core" class="{{ 'fault blink' if d.core_break else 'ok' }}">
                    {{ '✗ Core Break' if d.core_break else '✓ Core OK' }}
                </span>
            </div>
        </div>

        <div class="card">
            <div class="label">Program Faults</div>
            <div class="checks" style="font-size:13px;">
                <span id="fault-wb"     class="{{ 'fault blink' if d.fault_wire_break else 'ok' }}">{{ '✗ Wire Break' if d.fault_wire_break else '✓ Wire Break' }}</span><br>
                <span id="fault-es"     class="{{ 'fault blink' if d.fault_estop else 'ok' }}">{{ '✗ E-Stop' if d.fault_estop else '✓ E-Stop' }}</span><br>
                <span id="fault-guard"  class="{{ 'fault blink' if d.fault_guard_door else 'ok' }}">{{ '✗ Guard Door' if d.fault_guard_door else '✓ Guard Door' }}</span><br>
                <span id="fault-puller" class="{{ 'fault blink' if d.fault_puller_servo else 'ok' }}">{{ '✗ Puller Servo' if d.fault_puller_servo else '✓ Puller Servo' }}</span><br>
                <span id="fault-table"  class="{{ 'fault blink' if d.fault_table_servo else 'ok' }}">{{ '✗ Table Servo' if d.fault_table_servo else '✓ Table Servo' }}</span>
            </div>
        </div>

        <div class="card">
            <div class="label">Recovery &amp; Sequence</div>
            <div class="checks" style="font-size:13px; margin-top:6px;">
                <span id="wb-detected" class="{{ 'fault' if d.wire_break_detected else 'ok' }}" style="font-weight:bold;">
                    {{ '✗ Wire Break DETECTED' if d.wire_break_detected else '✓ Wire Break clear' }}
                </span><br>
                <span style="color:#8b949e; font-size:12px; margin-top:4px; display:block;">
                    Sequence step: <span id="sequence-step" style="color:#4fc3f7;">{{ d.sequence_step or '—' }}</span>
                </span>
                <span style="color:#8b949e; font-size:12px;">
                    Puller master: <span id="puller-master" style="color:#4fc3f7;">{{ 'Yes' if d.puller_master_axis else 'No' }}</span>
                </span>
            </div>
        </div>

    </div>

    <div class="section">Live — Last 2.5 Minutes</div>
    <div style="background:#2a2a2a; border-radius:8px; padding:14px; margin-bottom:12px;">
        <canvas id="liveChart" style="width:100%; height:300px; display:block;"></canvas>
    </div>

    <div class="conn" id="conn-bar">
        PLC: <span id="conn-status" class="ok">CONNECTED</span>
        &nbsp;|&nbsp; Logs: {{ log_dir }}
        &nbsp;|&nbsp; {{ braider_id }}
        &nbsp;|&nbsp; <span id="last-update" style="color:#555"></span>
        &nbsp;|&nbsp; <button id="sound-btn" onclick="toggleSound()" style="background:none;border:1px solid #444;color:#888;cursor:pointer;padding:2px 8px;border-radius:4px;font-size:11px;">🔇 Sound OFF</button>
    </div>

<script>
// ── Constants ─────────────────────────────────────────────────────────────────
const MAX_POINTS = 75;
const IO_BIT_LABELS = [
    'EStop_OK','Door_Closed','Puller_SSW_Close','Puller_SSW_Open',
    'Start_PB','Stop_PB','Jog_Fwd','TakeUp_OL',
    'Upper_Prox','Lower_Prox','WireBreak_SW','Triaxial_SW'
];
const STATE_COLORS = {
    16:'rgba(102,187,106,0.12)', 4:'rgba(239,83,80,0.08)', 1:'rgba(144,164,174,0.05)'
};

// ── Rolling data arrays ───────────────────────────────────────────────────────
const tableSpeedArr  = Array(MAX_POINTS).fill(null);
const pullerSpeedArr = Array(MAX_POINTS).fill(null);
const speedRatioArr  = Array(MAX_POINTS).fill(null);
const timestampsArr  = Array(MAX_POINTS).fill('');
const machineStatesArr = Array(MAX_POINTS).fill(0);

// ── Chart ─────────────────────────────────────────────────────────────────────
const canvas    = document.getElementById('liveChart');
const canvasCtx = canvas.getContext('2d');

function drawChart() {
    const W = canvas.width  = canvas.parentElement.clientWidth - 28;
    const H = canvas.height = 300;
    const PAD = {top:8, right:10, bottom:22, left:52};
    const GAP = 8; const PANELS = 3;
    const plotW  = W - PAD.left - PAD.right;
    const panelH = (H - PAD.top - PAD.bottom - GAP*(PANELS-1)) / PANELS;
    canvasCtx.clearRect(0,0,W,H);
    const panelTop = p => PAD.top + p*(panelH+GAP);

    function drawBackground(p) {
        for (let i=0; i<MAX_POINTS-1; i++) {
            const x0=PAD.left+(i/(MAX_POINTS-1))*plotW;
            const x1=PAD.left+((i+1)/(MAX_POINTS-1))*plotW;
            canvasCtx.fillStyle = machineStatesArr[i]===16
                ? 'rgba(102,187,106,0.12)' : 'rgba(144,164,174,0.06)';
            canvasCtx.fillRect(x0,panelTop(p),x1-x0,panelH);
        }
        canvasCtx.strokeStyle='#333'; canvasCtx.lineWidth=0.5;
        for (let i=0; i<=3; i++) {
            const y=panelTop(p)+(i/3)*panelH;
            canvasCtx.beginPath(); canvasCtx.moveTo(PAD.left,y);
            canvasCtx.lineTo(PAD.left+plotW,y); canvasCtx.stroke();
        }
    }
    function getRange(arr) {
        const vals=arr.filter(v=>v!==null&&isFinite(v));
        if (!vals.length) return [0,1];
        const mn=Math.min(...vals), mx=Math.max(...vals);
        const pad=(mx-mn)*0.15||0.05; return [mn-pad,mx+pad];
    }
    function drawLine(p,data,color,mn,mx) {
        canvasCtx.strokeStyle=color; canvasCtx.lineWidth=1.5; canvasCtx.lineJoin='round';
        canvasCtx.beginPath(); let started=false;
        const top=panelTop(p);
        for (let i=0;i<MAX_POINTS;i++) {
            if (data[i]===null||!isFinite(data[i])) { started=false; continue; }
            const x=PAD.left+(i/(MAX_POINTS-1))*plotW;
            const y=top+panelH-((data[i]-mn)/(mx-mn))*panelH;
            if (!started) { canvasCtx.moveTo(x,y); started=true; }
            else canvasCtx.lineTo(x,y);
        }
        canvasCtx.stroke();
    }
    function labelY(p,mn,mx,color) {
        canvasCtx.fillStyle=color; canvasCtx.font='9px monospace'; canvasCtx.textAlign='right';
        canvasCtx.fillText(mx.toFixed(3),PAD.left-3,panelTop(p)+9);
        canvasCtx.fillText(mn.toFixed(3),PAD.left-3,panelTop(p)+panelH-2);
    }
    function labelPanel(p,text,color) {
        canvasCtx.fillStyle=color; canvasCtx.font='bold 10px monospace'; canvasCtx.textAlign='left';
        canvasCtx.fillText(text,PAD.left+4,panelTop(p)+11);
    }
    const [tMn,tMx]=getRange(tableSpeedArr);
    drawBackground(0); drawLine(0,tableSpeedArr,'#4fc3f7',tMn,tMx);
    labelY(0,tMn,tMx,'#4fc3f7'); labelPanel(0,'Table Speed — Actual (rpm)','#4fc3f7');
    const [pMn,pMx]=getRange(pullerSpeedArr);
    drawBackground(1); drawLine(1,pullerSpeedArr,'#81c784',pMn,pMx);
    labelY(1,pMn,pMx,'#81c784'); labelPanel(1,'Puller Speed (in/s)','#81c784');
    const [rMn,rMx]=getRange(speedRatioArr);
    drawBackground(2); drawLine(2,speedRatioArr,'#ffb74d',rMn,rMx);
    labelY(2,rMn,rMx,'#ffb74d'); labelPanel(2,'Speed Ratio','#ffb74d');
    canvasCtx.fillStyle='#555'; canvasCtx.font='9px monospace'; canvasCtx.textAlign='center';
    const xBottom=panelTop(2)+panelH+14;
    if (timestampsArr[0]) canvasCtx.fillText(timestampsArr[0],PAD.left,xBottom);
    if (timestampsArr[MAX_POINTS-1]) canvasCtx.fillText(timestampsArr[MAX_POINTS-1],PAD.left+plotW,xBottom);
    const mid=Math.floor(MAX_POINTS/2);
    if (timestampsArr[mid]) canvasCtx.fillText(timestampsArr[mid],PAD.left+plotW/2,xBottom);
}

// ── Sound system — default OFF ────────────────────────────────────────────────
let soundEnabled = false;
let audioCtx = null;

function getAudioCtx() {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    return audioCtx;
}

function beep(freq, duration, volume, type) {
    if (!soundEnabled) return;
    try {
        const ctx = getAudioCtx();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain); gain.connect(ctx.destination);
        osc.type = type || 'sine';
        osc.frequency.value = freq;
        gain.gain.setValueAtTime(volume || 0.3, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + duration);
        osc.start(ctx.currentTime);
        osc.stop(ctx.currentTime + duration);
    } catch(e) {}
}

// State change sounds — different tone per transition
function playStateChangeSound(fromState, toState) {
    if (toState === 16) {
        beep(660, 0.12, 0.25, 'sine');  // RUNNING — high pleasant
        setTimeout(() => beep(880, 0.08, 0.2, 'sine'), 100);
    } else if (toState === 512 || toState === 256) {
        beep(220, 0.3, 0.4, 'square'); // ABORTED/ABORTING — low alarm
        setTimeout(() => beep(180, 0.3, 0.4, 'square'), 250);
    } else if (toState === 4 || toState === 32) {
        beep(440, 0.15, 0.25, 'triangle'); // STOPPED/STOPPING — mid neutral
    } else if (toState === 1) {
        beep(330, 0.1, 0.2, 'sine'); // OFF — soft low
    }
}

function toggleSound() {
    soundEnabled = !soundEnabled;
    const btn = document.getElementById('sound-btn');
    if (btn) btn.textContent = soundEnabled ? '🔊 Sound ON' : '🔇 Sound OFF';
    if (soundEnabled) {
        getAudioCtx().resume();
        beep(440, 0.05, 0.1, 'sine'); // confirmation beep
    }
}


function setSafety(id, ok, okText, faultText, faultClass, isStale) {
    const el = document.getElementById(id); if (!el) return;
    el.textContent = (ok && !isStale) ? '✓ ' + okText : '✗ ' + (isStale ? 'DATA STALE' : faultText);
    el.className   = (ok && !isStale) ? 'ok' : faultClass;
}

function setFault(id, active, okText, faultText) {
    const el = document.getElementById(id); if (!el) return;
    el.textContent = active ? '✗ ' + faultText : '✓ ' + okText;
    el.className   = active ? 'fault blink' : 'ok';
}

function updateIoDecoder(wb, isStale) {
    const ioEl  = document.getElementById('io-decoded');
    const binEl = document.getElementById('wb-binary');
    if (wb !== null && wb >= 0 && !isStale) {
        let binStr = '';
        for (let b = 11; b >= 0; b--) binStr += (wb & (1 << b)) ? '1' : '0';
        if (binEl) binEl.textContent = binStr;
        if (ioEl) {
            let html = '<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:4px;margin-top:4px;">';
            IO_BIT_LABELS.forEach((label, bit) => {
                const active = (wb & (1 << bit)) !== 0;
                const bg     = active ? '#1b3a1b' : '#1a1a1a';
                const color  = active ? '#66bb6a' : '#555';
                const border = active ? '#2d5a2d' : '#2a2a2a';
                html += `<div style="background:${bg};border:1px solid ${border};border-radius:4px;padding:4px 6px;text-align:center;">
                    <div style="font-size:9px;color:#666;margin-bottom:1px;">bit ${bit}</div>
                    <div style="font-size:15px;font-weight:bold;color:${color};">${active ? '1' : '0'}</div>
                    <div style="font-size:9px;color:${color};margin-top:1px;line-height:1.3;">${label}</div>
                </div>`;
            });
            html += '</div>';
            ioEl.innerHTML = html;
        }
    } else if (wb !== null && wb < 0) {
        if (binEl) binEl.textContent = 'I/O FAULT';
        if (ioEl)  ioEl.innerHTML = '<span style="color:#ef5350;font-size:12px;">INVALID — I/O module fault during physical disturbance</span>';
    } else {
        if (binEl) binEl.textContent = '—';
        if (ioEl)  ioEl.textContent  = '—';
    }
}

function updatePie(pcts) {
    const oeeEl    = document.getElementById('oee-value');
    const legendEl = document.getElementById('oee-legend');
    const pieCanvas= document.getElementById('oeePieCanvas');
    const stateColors = {
        'RUNNING':'#66bb6a','READY':'#4fc3f7','STOPPED':'#ef5350',
        'PAUSED':'#ffa726','OFF':'#78909c','ABORTED':'#b71c1c','UNKNOWN':'#555'
    };
    if (!oeeEl || !Object.keys(pcts).length) {
        if (oeeEl) oeeEl.textContent = '—';
        if (legendEl) legendEl.textContent = 'Waiting...';
        return;
    }
    const runningPct = pcts['RUNNING'] || 0;
    oeeEl.textContent = runningPct.toFixed(1) + '%';
    oeeEl.style.color = runningPct>=50 ? '#66bb6a' : runningPct>=25 ? '#ffa726' : '#ef5350';
    if (legendEl) {
        legendEl.innerHTML = Object.entries(pcts).map(([s,p]) =>
            `<div><span style="display:inline-block;width:8px;height:8px;background:${stateColors[s]||'#999'};margin-right:4px;border-radius:2px;"></span>${s}: ${p}%</div>`
        ).join('');
    }
    if (pieCanvas) {
        const ctx=pieCanvas.getContext('2d'); ctx.clearRect(0,0,110,110);
        const cx=55,cy=55,r=50; let angle=-Math.PI/2;
        for (const [s,p] of Object.entries(pcts)) {
            if (p<=0) continue;
            const sweep=(p/100)*(2*Math.PI);
            ctx.beginPath(); ctx.moveTo(cx,cy); ctx.arc(cx,cy,r,angle,angle+sweep);
            ctx.closePath(); ctx.fillStyle=stateColors[s]||'#999'; ctx.fill();
            ctx.strokeStyle='#0d1117'; ctx.lineWidth=1.5; ctx.stroke();
            angle+=sweep;
        }
        ctx.beginPath(); ctx.arc(cx,cy,r*0.52,0,Math.PI*2);
        ctx.fillStyle='#161b22'; ctx.fill();
    }
}

// ── Main fetch loop ───────────────────────────────────────────────────────────
let lastSeenTimestamp = '', timestampAgeTicks = 0, lastMachineState = null;

async function fetchAndUpdate() {
    try {
        const res  = await fetch('/api/latest');
        const data = await res.json();
        const now    = new Date().toLocaleTimeString('en-US',{hour12:false});
        const vfdAct = data.vfd_freq_actual || 0;
        const ps     = data.puller_speed || 0;
        const sr     = data.speed_ratio  || null;
        const wb     = data.wire_break_bits;
        const st     = data.machine_state;

        // Stale detection
        let isStale = false;
        if (data.timestamp) {
            if (data.timestamp === lastSeenTimestamp) timestampAgeTicks++;
            else { lastSeenTimestamp = data.timestamp; timestampAgeTicks = 0; }
            if (timestampAgeTicks >= 5 || !data.connected) isStale = true;
        } else { isStale = true; }

        // Rolling arrays — table speed (rpm) derived from VFD_Freq_Actual, always truthful
        tableSpeedArr.shift();   tableSpeedArr.push(isStale ? null : (vfdAct * 0.0355));
        pullerSpeedArr.shift();  pullerSpeedArr.push(isStale ? null : ps);
        speedRatioArr.shift();   speedRatioArr.push(isStale ? null : sr);
        timestampsArr.shift();   timestampsArr.push(now);
        machineStatesArr.shift();machineStatesArr.push(isStale ? 0 : (st||0));
        drawChart();

        // Connection status
        const statusEl = document.getElementById('conn-status');
        if (statusEl) {
            statusEl.textContent = (!data.connected || isStale) ? 'STALE — PLC UNREACHABLE' : 'CONNECTED';
            statusEl.className   = (!data.connected || isStale) ? 'fault' : 'ok';
        }
        const luEl = document.getElementById('last-update');
        if (luEl) luEl.textContent = 'updated ' + now;
        const hts = document.getElementById('header-timestamp');
        if (hts)  hts.textContent = isStale ? '—' : (data.timestamp || '—');

        // Helper: set text, show '—' when stale
        const upd = (id, v) => { const e=document.getElementById(id); if(e) e.textContent=isStale?'—':v; };

        // Production / process cards
        upd('feet-value',      data.puller_pos_feet  ? data.puller_pos_feet.toFixed(2)  : '—');
        // Table speed card — RPM derived from VFD_Freq_Actual (always truthful, zeroes on stop).
        // Ratio derived empirically: rpm = VFD_Freq_Actual(Hz x10) * 0.0355
        // (confirmed: 4788 -> 170.0 rpm, 4225 -> 150.0 rpm)
        const VFD_TO_RPM = 0.0355;
        const vfdActualRaw  = data.vfd_freq_actual;
        const vfdCommandRaw = data.vfd_freq_command;
        const tableRpmActual = (vfdActualRaw != null) ? (vfdActualRaw * VFD_TO_RPM).toFixed(1) : null;
        const tableRevs = data.table_speed;  // realTableSpeed — may lag/hold on stop

        const tableEl = document.getElementById('table-value');
        if (tableEl) tableEl.textContent = isStale ? '—' : (tableRpmActual != null ? tableRpmActual : '0');
        const vfdActEl = document.getElementById('table-vfd-actual');
        if (vfdActEl) vfdActEl.textContent = (!isStale && vfdActualRaw != null) ? vfdActualRaw : '—';
        const vfdCmdEl = document.getElementById('table-vfd-command');
        if (vfdCmdEl) vfdCmdEl.textContent = (!isStale && vfdCommandRaw != null) ? vfdCommandRaw : '—';
        const revsEl = document.getElementById('table-revs-value');
        if (revsEl) revsEl.textContent = (!isStale && tableRevs) ? tableRevs.toFixed(4) : '—';
        upd('puller-value',    ps ? ps.toFixed(4) : '—');
        upd('ratio-value',     sr ? sr.toFixed(5) : '—');
        upd('vfd-actual',      data.vfd_freq_actual  != null ? data.vfd_freq_actual  : '—');
        upd('vfd-command',     data.vfd_freq_command != null ? data.vfd_freq_command : '—');
        upd('vfd-delta',       data.vfd_freq_delta   != null ? data.vfd_freq_delta   : '0');
        upd('puller-vel-err',  data.puller_vel_cmd_err  != null ? data.puller_vel_cmd_err.toFixed(5)  : '—');
        upd('table-vel-fb',    data.table_vel_feedback  != null ? data.table_vel_feedback.toFixed(4)  : '—');

        const taperEl = document.getElementById('taper-value');
        if (taperEl) taperEl.textContent = (data.taper_sensor && data.taper_sensor > 0 && !isStale)
            ? data.taper_sensor.toFixed(2) : '—';
        const vfdRef = document.getElementById('vfd-at-ref');
        if (vfdRef) vfdRef.innerHTML = (!isStale && data.vfd_at_ref) ? '&nbsp;<span class="ok">AT REF</span>' : '';

        // State card + sound on change
        const stateEl  = document.getElementById('state-value');
        const stateDiv = document.getElementById('state-div');
        if (stateEl) stateEl.textContent = isStale ? 'UNKNOWN (DISCONNECTED)' : (data.state_name || '—');
        if (stateDiv) stateDiv.className = 'value ' + (
            isStale ? 'stopped' : st===16 ? 'running' :
            (st===256||st===512) ? 'fault blink' : (st===64||st===128) ? 'paused' : 'stopped'
        );
        if (!isStale && st !== lastMachineState && lastMachineState !== null) {
            playStateChangeSound(lastMachineState, st);
        }
        lastMachineState = isStale ? lastMachineState : st;

        // Elapsed time
        const elapsedEl = document.getElementById('elapsed-value');
        if (elapsedEl) {
            const elapsed = data.state_elapsed_s;
            if (elapsed && !isStale) {
                const h=Math.floor(elapsed/3600), m=Math.floor((elapsed%3600)/60);
                elapsedEl.textContent = h+'h '+m+'m';
            } else { elapsedEl.textContent = '—'; }
        }

        // IO / wire break card
        upd('wb-value', wb != null ? wb : '—');
        updateIoDecoder(wb, isStale);
        const wbDiv = document.getElementById('wb-div');
        if (wbDiv) {
            const abnormal = wb !== null && wb !== 3 && wb !== 1 && wb >= 0 && !isStale;
            wbDiv.className = 'value ' + (abnormal ? 'warn' : 'ok');
        }

        // Machine Faults card
        const faultDiv  = document.getElementById('fault-div');
        const faultUnit = document.getElementById('fault-unit');
        if (faultDiv) {
            const hasFault = !data.no_faults;
            faultDiv.className   = 'value ' + ((hasFault && !isStale) ? 'fault blink' : 'ok');
            faultDiv.textContent = isStale ? 'UNKNOWN' : (hasFault ? 'FAULT' : 'NONE');
        }
        if (faultUnit) faultUnit.textContent = (!isStale && data.machine_faults && data.machine_faults !== 4)
            ? 'code: ' + data.machine_faults : '';

        // Safety inputs
        setSafety('safety-estop',  data.estop_ok,          'E-Stop OK',  'E-STOP PRESSED', 'fault blink', isStale);
        setSafety('safety-door',   data.door_ok,           'Door Closed','Door Open',       'warn',        isStale);
        setSafety('safety-guards', data.guards_ok,         'Guards OK',  'Guards Open',     'fault',       isStale);
        setSafety('safety-motor',  !data.i_table_motor_ol, 'Motor OK',   'MOTOR OL',        'fault blink', isStale);
        setSafety('safety-core',   !data.core_break,       'Core OK',    'CORE BREAK',      'fault blink', isStale);

        // Program Faults
        setFault('fault-wb',     data.fault_wire_break,   'Wire Break',   'Wire Break');
        setFault('fault-es',     data.fault_estop,        'E-Stop',       'E-Stop');
        setFault('fault-guard',  data.fault_guard_door,   'Guard Door',   'Guard Door');
        setFault('fault-puller', data.fault_puller_servo, 'Puller Servo', 'Puller Servo');
        setFault('fault-table',  data.fault_table_servo,  'Table Servo',  'Table Servo');

        // Recovery card
        upd('sequence-step',  data.sequence_step  != null ? data.sequence_step  : '—');
        const pmEl = document.getElementById('puller-master');
        if (pmEl && !isStale) pmEl.textContent = data.puller_master_axis ? 'Yes' : 'No';
        const wbDetEl = document.getElementById('wb-detected');
        if (wbDetEl && !isStale) {
            wbDetEl.textContent = data.wire_break_detected ? '✗ Wire Break DETECTED' : '✓ Wire Break clear';
            wbDetEl.className   = data.wire_break_detected ? 'fault' : 'ok';
        }

        // Daily utilization pie
        updatePie(data.daily_state_pcts || {});

    } catch(e) {
        const cs = document.getElementById('conn-status');
        if (cs) { cs.textContent = 'DISCONNECTED'; cs.className = 'fault'; }
    }
}

window.addEventListener('resize', drawChart);
setInterval(fetchAndUpdate, 2000);
fetchAndUpdate();

// ── Fault event alert banner ──────────────────────────────────────────────────
let hiresAlertTimer = null;

function updateHiresAlert() {
  fetch('/api/hires_alert')
    .then(r => r.json())
    .then(data => {
      const el = document.getElementById('hiresAlert');
      if (!el || !data.active) return;
      document.getElementById('hiresType').textContent  = data.event_type || '';
      document.getElementById('hiresFeet').textContent  = data.puller_feet !== null ? data.puller_feet : '—';
      document.getElementById('hiresTime').textContent  = data.timestamp || '';
      el.style.display = 'block';
      clearTimeout(hiresAlertTimer);
      hiresAlertTimer = setTimeout(() => { el.style.display = 'none'; }, 30000);
    })
    .catch(() => {});
}

setInterval(updateHiresAlert, 1000);
updateHiresAlert();
</script>
</body>
</html>
"""

app = Flask(__name__)


@app.route('/')
def dashboard():
    with _lock:
        data = dict(_latest)
    return render_template_string(
        DASHBOARD_HTML,
        d=type('D', (), data)(),
        plc_ip=PLC_IP,
        log_dir=LOG_DIR,
        braider_id=BRAIDER_ID,
        normal_wire_bits=IO_NORMAL_RUNNING,
    )

@app.route('/api/hires_alert')
def api_hires_alert():
    """Latest hires event alert — polled by dashboard to show fault notifications."""
    import json
    with _hires_alert_lock:
        return json.dumps(_hires_latest_alert), 200, {'Content-Type': 'application/json'}


@app.route('/api/latest')
def api_latest():
    with _lock:
        return jsonify(_latest)


@app.route('/favicon.ico')
def favicon():
    return '', 204


@app.route('/api/hires_events')
def api_hires_events():
    """List recent hi-res event capture files with metadata."""
    import glob
    files = sorted(
        glob.glob(os.path.join(HIRES_LOG_DIR, '*.csv')),
        reverse=True
    )[:20]
    result = []
    for f in files:
        name = os.path.basename(f)
        size = os.path.getsize(f)
        try:
            with open(f, newline='') as fh:
                rows = list(csv.DictReader(fh))
            pre_rows  = sum(1 for r in rows if r.get('Hires_Phase') == 'PRE')
            post_rows = sum(1 for r in rows if r.get('Hires_Phase') == 'POST')
            first_ts  = rows[0].get('Timestamp', '') if rows else ''
            event_ts  = next((r.get('Timestamp','') for r in rows if r.get('Hires_Phase')=='POST'), '')
            # Prefer the explicit column; fall back to parsing the filename for older files
            trigger = rows[0].get('Event_Trigger') if rows else None
            if not trigger:
                if 'wire_break' in name.lower():
                    trigger = 'WIRE_BREAK'
                elif 'estop' in name.lower() or 'abort' in name.lower():
                    trigger = 'ESTOP_OR_ABORT'
                else:
                    trigger = 'UNKNOWN'

            # Ground truth — prefer the stored column; recompute from raw signals
            # for older files that predate this column.
            if rows and 'Confirmed_Wire_Break' in rows[0]:
                raw_cwb = rows[0].get('Confirmed_Wire_Break')
                confirmed_wb = str(raw_cwb).strip().lower() in ('true', '1')
            else:
                confirmed_wb = any(
                    str(r.get('Wire_Break_Detected','')).strip().lower() in ('true','1') or
                    str(r.get('Fault_WireBreak','')).strip().lower()    in ('true','1')
                    for r in rows
                ) if rows else False
        except Exception:
            pre_rows = post_rows = 0
            first_ts = event_ts = ''
            trigger = 'UNKNOWN'
            confirmed_wb = False
        result.append({
            'filename':            name,
            'size_kb':             round(size / 1024, 1),
            'pre_rows':            pre_rows,
            'post_rows':           post_rows,
            'first_ts':            first_ts,
            'event_ts':            event_ts,
            'event_trigger':       trigger,
            'confirmed_wire_break':confirmed_wb,
        })
    return jsonify(result)


# ── Floor Report ──────────────────────────────────────────────────────────────

def get_floor_report_data():
    import glob
    from datetime import date, timedelta, datetime as dt

    today_str     = date.today().isoformat()
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()

    def read_today_rows(filepath, today):
        rows = []
        if os.path.exists(filepath):
            try:
                with open(filepath, newline='', encoding='utf-8', errors='replace') as f:
                    all_rows = list(csv.DictReader(f))
                for row in reversed(all_rows):
                    if row.get('Timestamp', '').startswith(today):
                        rows.append(row)
                    elif rows:
                        break
                rows.reverse()
            except Exception:
                pass
        base = filepath.replace('.csv', '')
        for archive in sorted(glob.glob(base + '_archived_' + today.replace('-','') + '*.csv')):
            try:
                with open(archive, newline='', encoding='utf-8', errors='replace') as f:
                    rows = [r for r in csv.DictReader(f) if r.get('Timestamp','').startswith(today)] + rows
            except Exception:
                pass
        # NOTE: no timestamp-based dedup here. Archiving uses an atomic os.rename(),
        # so a row can never legitimately appear in both the live file and an archive —
        # any two rows sharing the same second-precision timestamp are distinct real
        # samples (e.g. during a PLC reconnect burst), not duplicates. Dropping them
        # by timestamp previously caused this function's running% to read lower than
        # calculate_daily_state_percentages() (the pie chart), which performs no dedup.
        return rows

    proc_rows  = read_today_rows(PROCESS_LOG, today_str)
    event_rows = read_today_rows(EVENT_LOG,   today_str)

    state_counts = {}
    for row in proc_rows:
        s = row.get('State_Name','UNKNOWN') or 'UNKNOWN'
        state_counts[s] = state_counts.get(s,0) + 1

    total_rows = len(proc_rows)
    def pct(n): return round(100*n/total_rows,1) if total_rows else 0
    def hrs(n): return round(n*2/3600,2) if n else 0

    running_rows = state_counts.get('RUNNING',0)
    off_rows     = state_counts.get('OFF',0)
    stopped_rows = state_counts.get('STOPPED',0)

    vessels = 0; prev_s = None; run_start_feet = None; VESSEL_MIN_FEET = 25.0
    for row in proc_rows:
        s = row.get('State_Name','')
        try: feet = float(row.get('Puller_Pos_Feet') or 0)
        except: feet = 0.0
        if s=='RUNNING' and prev_s!='RUNNING': run_start_feet=feet
        elif prev_s=='RUNNING' and s in ('OFF','STOPPED','STOPPING'):
            if run_start_feet is not None and abs(feet-run_start_feet)>=VESSEL_MIN_FEET: vessels+=1
            run_start_feet=None
        prev_s=s

    changeovers=[]; in_off=False; off_start_ts=None
    for row in proc_rows:
        s=row.get('State_Name',''); ts_=row.get('Timestamp','')
        if s=='OFF' and not in_off: in_off=True; off_start_ts=ts_
        elif s!='OFF' and in_off:
            in_off=False
            if off_start_ts:
                try:
                    dur=(dt.fromisoformat(ts_)-dt.fromisoformat(off_start_ts)).total_seconds()/60
                    if dur>5: changeovers.append({'start':off_start_ts,'end':ts_,'min':round(dur,1)})
                except Exception: pass

    avg_co=round(sum(c['min'] for c in changeovers)/len(changeovers),1) if changeovers else None
    min_co=min((c['min'] for c in changeovers),default=None)
    max_co=max((c['min'] for c in changeovers),default=None)

    timeline=[]; wire_breaks=0
    for row in event_rows:
        # Support both old schema (Event / From_State / To_State) and new schema (Event_Type / From_Value / To_Value)
        etype = row.get('Event_Type') or row.get('Event','')
        from_s = row.get('From_Value') or row.get('From_State','')
        to_s   = row.get('To_Value')   or row.get('To_State','')
        if etype=='STATE_CHANGE':
            timeline.append({'time':row.get('Timestamp','')[:19],'from_state':from_s,'to_state':to_s,'feet':row.get('Puller_Feet','')})
        elif etype=='WIRE_BREAK':
            wire_breaks+=1
            timeline.append({'time':row.get('Timestamp','')[:19],'from_state':'WIRE BREAK','to_state':row.get('Detail',''),'feet':row.get('Puller_Feet','')})

    if os.path.exists(PROCESS_LOG):
        try:
            with open(PROCESS_LOG, newline='', encoding='utf-8', errors='replace') as f:
                yest_rows=[r for r in csv.DictReader(f) if r.get('Timestamp','').startswith(yesterday_str)]
        except Exception: yest_rows=[]
    else: yest_rows=[]
    yest_total=len(yest_rows)
    yest_running=sum(1 for r in yest_rows if r.get('State_Name')=='RUNNING')
    yest_run_pct=round(100*yest_running/yest_total,1) if yest_total else None

    return {
        'date':               today_str,
        'total_hrs':          hrs(total_rows),
        'running_hrs':        hrs(running_rows),
        'running_pct':        pct(running_rows),
        'off_hrs':            hrs(off_rows),
        'off_pct':            pct(off_rows),
        'stopped_pct':        pct(stopped_rows),
        'vessels':            vessels,
        'vessel_target':      4,
        'running_target_pct': 55.0,
        'changeovers':        changeovers,
        'avg_changeover':     avg_co,
        'min_changeover':     min_co,
        'max_changeover':     max_co,
        'wire_breaks':        wire_breaks,
        'timeline':           timeline,
        'yest_running_pct':   yest_run_pct,
    }


FLOOR_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Braider 2 — Floor Report</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background:#0d1117; color:#e6edf3; font-family:'Segoe UI',Arial,sans-serif; padding:20px; }
h1 { font-size:2rem; color:#58a6ff; margin-bottom:4px; }
.subtitle { color:#8b949e; font-size:1rem; margin-bottom:24px; }
.scorecard { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin-bottom:28px; }
.card { background:#161b22; border:1px solid #30363d; border-radius:10px; padding:18px 16px; text-align:center; }
.card.green  { border-color:#238636; }
.card.red    { border-color:#da3633; }
.card.yellow { border-color:#9e6a03; }
.card.blue   { border-color:#1f6feb; }
.card-label  { font-size:.78rem; color:#8b949e; text-transform:uppercase; letter-spacing:.06em; margin-bottom:6px; }
.card-value  { font-size:2.6rem; font-weight:700; line-height:1; }
.card-sub    { font-size:.82rem; color:#8b949e; margin-top:5px; }
.card-vs     { font-size:.8rem; margin-top:6px; }
.up   { color:#3fb950; } .down { color:#f85149; } .same { color:#8b949e; }
.bar-wrap { background:#21262d; border-radius:4px; height:8px; margin-top:10px; overflow:hidden; }
.bar-fill { height:100%; border-radius:4px; }
.bar-green  { background:#238636; } .bar-yellow { background:#9e6a03; } .bar-red { background:#da3633; }
.section-title { font-size:1.1rem; font-weight:600; color:#58a6ff; margin-bottom:12px;
                 border-bottom:1px solid #21262d; padding-bottom:6px; }
.section { margin-bottom:28px; }
table { width:100%; border-collapse:collapse; font-size:.9rem; }
th { background:#161b22; color:#8b949e; text-align:left; padding:8px 12px; font-weight:600;
     font-size:.78rem; text-transform:uppercase; letter-spacing:.05em; }
td { padding:8px 12px; border-top:1px solid #21262d; }
tr:hover td { background:#161b22; }
.footer { color:#444; font-size:.78rem; margin-top:20px; text-align:center; }
</style>
</head>
<body>

<h1>Braider 2 — Daily Performance</h1>
<div class="subtitle">{{ d.date }} &nbsp;·&nbsp; {{ d.total_hrs }}h logged today &nbsp;·&nbsp; <span id="last-refresh" style="color:#58a6ff;">Loading...</span></div>

<div class="scorecard">

  {% set r_color = 'green' if d.running_pct >= d.running_target_pct else ('yellow' if d.running_pct >= d.running_target_pct - 10 else 'red') %}
  <div class="card {{ r_color }}">
    <div class="card-label">Running Today</div>
    <div class="card-value" style="color:{% if r_color=='green' %}#3fb950{% elif r_color=='yellow' %}#d29922{% else %}#f85149{% endif %}">{{ d.running_pct }}%</div>
    <div class="card-sub">{{ d.running_hrs }}h · Target {{ d.running_target_pct }}%</div>
    {% if d.yest_running_pct is not none %}
    <div class="card-vs">
      {% set diff = (d.running_pct - d.yest_running_pct)|round(1) %}
      {% if diff > 0 %}<span class="up">▲ {{ diff }}% vs yesterday</span>
      {% elif diff < 0 %}<span class="down">▼ {{ diff|abs }}% vs yesterday</span>
      {% else %}<span class="same">= same as yesterday</span>{% endif %}
    </div>{% endif %}
    <div class="bar-wrap"><div class="bar-fill bar-{{ r_color }}" style="width:{{ [100,d.running_pct|int]|min }}%"></div></div>
  </div>

  {% set c_color = 'green' if d.avg_changeover and d.avg_changeover <= 50 else ('yellow' if d.avg_changeover and d.avg_changeover <= 65 else 'red') %}
  <div class="card {{ c_color if d.avg_changeover else 'blue' }}">
    <div class="card-label">Avg Changeover</div>
    <div class="card-value" style="color:{% if c_color=='green' %}#3fb950{% elif c_color=='yellow' %}#d29922{% else %}#f85149{% endif %}">
      {% if d.avg_changeover %}{{ d.avg_changeover }}<span style="font-size:1.2rem">m</span>
      {% else %}<span style="font-size:1.4rem;color:#8b949e">—</span>{% endif %}
    </div>
    <div class="card-sub">{% if d.min_changeover %}Best {{ d.min_changeover }}m · Worst {{ d.max_changeover }}m{% else %}No changeovers yet{% endif %}</div>
    <div class="card-sub" style="margin-top:4px">Target: ≤50 min</div>
  </div>

  {% set w_color = 'green' if d.wire_breaks == 0 else ('yellow' if d.wire_breaks <= 2 else 'red') %}
  <div class="card {{ w_color }}">
    <div class="card-label">Wire Breaks</div>
    <div class="card-value" style="color:{% if w_color=='green' %}#3fb950{% elif w_color=='yellow' %}#d29922{% else %}#f85149{% endif %}">{{ d.wire_breaks }}</div>
    <div class="card-sub">{% if d.wire_breaks == 0 %}Clean day ✓{% elif d.wire_breaks == 1 %}1 break{% else %}{{ d.wire_breaks }} breaks{% endif %}</div>
  </div>

</div>

<!-- ── Utilization Pie Charts ── -->
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:28px;">

  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px;">
    <div class="section-title">Today — Midnight to Now</div>
    <div style="display:flex;align-items:center;gap:16px;padding:12px 0;">
      <canvas id="todayPie" width="140" height="140" style="flex-shrink:0;"></canvas>
      <div id="todayPieLegend" style="font-size:10px;line-height:1.9;color:#8b949e;"></div>
    </div>
  </div>

  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px;">
    <div class="section-title">This Week — Mon to Now</div>
    <div style="display:flex;align-items:center;gap:16px;padding:12px 0;">
      <canvas id="weekPie" width="140" height="140" style="flex-shrink:0;"></canvas>
      <div id="weekPieLegend" style="font-size:10px;line-height:1.9;color:#8b949e;"></div>
    </div>
  </div>

  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px;">
    <div class="section-title">Last Week — Mon to Sun</div>
    <div style="display:flex;align-items:center;gap:16px;padding:12px 0;">
      <canvas id="lastWeekPie" width="140" height="140" style="flex-shrink:0;"></canvas>
      <div id="lastWeekPieLegend" style="font-size:10px;line-height:1.9;color:#8b949e;"></div>
    </div>
  </div>

</div>

<!-- ── State Timeline Charts ── -->
<div class="section">
  <div class="section-title">Today — Machine State Timeline</div>
  <div id="todayChart" style="width:100%;height:220px;background:#0d1117;"></div>
</div>

<div class="section">
  <div class="section-title">This Week — Machine State Timeline</div>
  <div id="weekChart" style="width:100%;height:220px;background:#0d1117;"></div>
</div>

<div class="section">
  <div class="section-title">Last Week — Machine State Timeline</div>
  <div id="lastWeekChart" style="width:100%;height:220px;background:#0d1117;"></div>
</div>

<!-- ── Changeovers Table ── -->
<div class="section">
  <div class="section-title">Bobbin Changeovers Today ({{ d.changeovers|length }})</div>
  {% if d.changeovers %}
  <table>
    <thead><tr><th>#</th><th>Start</th><th>End</th><th>Duration</th><th>Rating</th></tr></thead>
    <tbody>
    {% for c in d.changeovers %}
    <tr>
      <td>{{ loop.index }}</td>
      <td>{{ c.start[11:19] }}</td>
      <td>{{ c.end[11:19] }}</td>
      <td><strong>{{ c.min }} min</strong></td>
      <td>{% if c.min <= 45 %}<span style="color:#3fb950">● Fast</span>
          {% elif c.min <= 55 %}<span style="color:#d29922">● On time</span>
          {% else %}<span style="color:#f85149">● Slow</span>{% endif %}</td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p style="color:#8b949e;padding:12px 0">No changeovers recorded yet today.</p>
  {% endif %}
</div>

<!-- ── Event Timeline ── -->
{% if d.timeline %}
<div class="section">
  <div class="section-title">Today's Event Timeline</div>
  <table>
    <thead><tr><th>Time</th><th>From</th><th>To</th><th>Puller Feet</th></tr></thead>
    <tbody>
    {% for ev in d.timeline %}
    <tr>
      <td style="font-family:monospace;font-size:.85rem;">{{ ev.time[11:] }}</td>
      <td>{{ ev.from_state }}</td>
      <td>{{ ev.to_state }}</td>
      <td>{{ ev.feet }}</td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

<div class="footer">Braider 2 · :5000/floor · Refreshes every 60s · Noble Gas Systems</div>

<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<script>
const STATE_COLORS = {
    'RUNNING':'#66bb6a','STOPPED':'#ef5350','OFF':'#455a64',
    'READY':'#4fc3f7','STARTING':'#26c6da','STOPPING':'#ff7043',
    'PAUSING':'#ffb74d','PAUSED':'#ffa726','ABORTING':'#ab47bc','ABORTED':'#b71c1c'
};

function getChartLayout() {
    return {
        paper_bgcolor:'#0d1117', plot_bgcolor:'#0d1117',
        font:{color:'#8b949e', size:11},
        margin:{t:10, r:20, b:40, l:80},
        height:220,
        showlegend:true,
        legend:{orientation:'h', y:-0.25, font:{size:10}},
        xaxis:{gridcolor:'#21262d', tickfont:{size:10}},
        yaxis:{gridcolor:'#21262d', tickfont:{size:11}, categoryorder:'array',
               categoryarray:['ABORTED','ABORTING','STOPPING','PAUSED','PAUSING','STOPPED','OFF','READY','STARTING','RUNNING']},
    };
}

function buildStateChart(divId, data) {
    if (!data.timestamps || !data.timestamps.length) {
        document.getElementById(divId).innerHTML =
            '<p style="color:#8b949e;padding:20px">No data yet.</p>';
        return;
    }
    const byState = {};
    for (let i = 0; i < data.timestamps.length; i++) {
        const s = data.states[i];
        if (!s) continue;
        if (!byState[s]) byState[s] = [];
        byState[s].push(data.timestamps[i]);
    }
    const plotTraces = Object.entries(byState).map(([state, times]) => ({
        x: times,
        y: Array(times.length).fill(state),
        mode: 'markers',
        name: state,
        marker: { color: STATE_COLORS[state] || '#999', size: 5, symbol: 'square' },
        type: 'scatter'
    }));
    Plotly.newPlot(divId, plotTraces, getChartLayout(), {responsive:true, displayModeBar:false});
}

function drawPie(canvasId, legendId, data) {
    const canvas = document.getElementById(canvasId);
    const legend = document.getElementById(legendId);
    if (!canvas || !data.states || !data.states.length) return;
    const counts = {};
    for (const s of data.states) { if (s) counts[s] = (counts[s]||0) + 1; }
    const total = Object.values(counts).reduce((a,b)=>a+b, 0);
    if (!total) return;
    const ORDER = ['RUNNING','OFF','STOPPED','READY','STARTING','STOPPING','PAUSING','PAUSED','ABORTING','ABORTED'];
    const entries = ORDER.filter(s=>counts[s]).map(s=>[s,counts[s]])
                         .concat(Object.entries(counts).filter(([s])=>!ORDER.includes(s)));
    const ctx = canvas.getContext('2d');
    const W=canvas.width, H=canvas.height, cx=W/2, cy=H/2, r=Math.min(W,H)/2-4;
    ctx.clearRect(0,0,W,H);
    let angle=-Math.PI/2, legendHTML='';
    for (const [state,count] of entries) {
        const sweep=(count/total)*Math.PI*2;
        const color=STATE_COLORS[state]||'#999';
        ctx.beginPath(); ctx.moveTo(cx,cy); ctx.arc(cx,cy,r,angle,angle+sweep);
        ctx.closePath(); ctx.fillStyle=color; ctx.fill();
        ctx.strokeStyle='#0d1117'; ctx.lineWidth=1.5; ctx.stroke();
        const pct=(count/total*100).toFixed(1), hrs=(count*2/3600).toFixed(1);
        legendHTML+=`<div><span style="color:${color};font-weight:700;">■</span> ${state}: ${pct}% (${hrs}h)</div>`;
        angle+=sweep;
    }
    ctx.beginPath(); ctx.arc(cx,cy,r*0.52,0,Math.PI*2); ctx.fillStyle='#161b22'; ctx.fill();
    const runPct=counts['RUNNING']?(counts['RUNNING']/total*100).toFixed(0):'0';
    ctx.fillStyle='#3fb950'; ctx.font='bold 22px Arial'; ctx.textAlign='center'; ctx.textBaseline='middle';
    ctx.fillText(runPct+'%',cx,cy-8);
    ctx.fillStyle='#8b949e'; ctx.font='10px Arial'; ctx.fillText('running',cx,cy+12);
    if (legend) legend.innerHTML=legendHTML;
}

// Track whether charts have been initialized
let chartsInitialized = false;

async function updateFloor() {
    try {
        const [todayRes, weekRes, lastWeekRes] = await Promise.all([
            fetch('/api/floor_data?range=today'),
            fetch('/api/floor_data?range=week'),
            fetch('/api/floor_data?range=lastweek'),
        ]);
        const todayData    = await todayRes.json();
        const weekData     = await weekRes.json();
        const lastWeekData = await lastWeekRes.json();

        // Pies always redraw fast (canvas, not Plotly)
        drawPie('todayPie',    'todayPieLegend',    todayData);
        drawPie('weekPie',     'weekPieLegend',     weekData);
        drawPie('lastWeekPie', 'lastWeekPieLegend', lastWeekData);

        // Charts: newPlot on first load, react (in-place update) on subsequent
        if (!chartsInitialized) {
            buildStateChart('todayChart',    todayData);
            buildStateChart('weekChart',     weekData);
            buildStateChart('lastWeekChart', lastWeekData);
            chartsInitialized = true;
        } else {
            updateStateChart('todayChart',    todayData);
            updateStateChart('weekChart',     weekData);
            updateStateChart('lastWeekChart', lastWeekData);
        }

        // Update last-refreshed timestamp
        const tsEl = document.getElementById('last-refresh');
        if (tsEl) tsEl.textContent = 'Updated ' + new Date().toLocaleTimeString();

    } catch(e) {
        console.warn('Floor update error:', e);
    }
}

function updateStateChart(divId, data) {
    // Plotly.react updates data in-place without blanking the chart.
    // Layout must be passed explicitly every call — omitting it (undefined)
    // makes Plotly fall back to its default white theme during the repaint.
    const byState = {};
    for (let i = 0; i < data.timestamps.length; i++) {
        const s = data.states[i]; if (!s) continue;
        if (!byState[s]) byState[s] = [];
        byState[s].push(data.timestamps[i]);
    }
    const plotTraces = Object.entries(byState).map(([state, times]) => ({
        x: times, y: Array(times.length).fill(state),
        mode: 'markers', name: state,
        marker: { color: STATE_COLORS[state] || '#999', size: 5, symbol: 'square' },
        type: 'scatter'
    }));
    Plotly.react(divId, plotTraces, getChartLayout(), {responsive:true, displayModeBar:false});
}

// Initial load then refresh every 60s without page blank
updateFloor();
setInterval(updateFloor, 60000);
</script>
</body>
</html>"""


@app.route('/floor')
def floor_report():
    d = get_floor_report_data()
    return render_template_string(FLOOR_HTML, d=type('D', (), d)())


@app.route('/api/floor_data')
def api_floor_data():
    import glob
    from datetime import date, timedelta
    from flask import request as freq
    range_param = freq.args.get('range', 'today')
    today = date.today()

    if range_param == 'week':
        monday = today - timedelta(days=today.weekday())
        cutoff = monday.isoformat()
        def row_matches(ts_): return ts_ >= cutoff
    elif range_param == 'lastweek':
        this_monday = today - timedelta(days=today.weekday())
        last_monday = this_monday - timedelta(days=7)
        last_friday = last_monday + timedelta(days=4)
        cutoff_start = last_monday.isoformat()
        cutoff_end   = last_friday.isoformat() + 'T23:59:59'
        def row_matches(ts_): return cutoff_start <= ts_ <= cutoff_end
    else:
        today_str = today.isoformat()
        def row_matches(ts_): return ts_.startswith(today_str)

    all_rows = []
    base = PROCESS_LOG.replace('.csv', '')

    if range_param in ('week', 'today'):
        days_back = today.weekday() if range_param == 'week' else 0
        for i in range(days_back + 1):
            d = (today - timedelta(days=i)).strftime('%Y%m%d')
            for archive in glob.glob(base + '_archived_' + d + '*.csv'):
                try:
                    with open(archive, newline='', encoding='utf-8', errors='replace') as f:
                        all_rows.extend(r for r in csv.DictReader(f) if row_matches(r.get('Timestamp','')))
                except Exception: pass
    elif range_param == 'lastweek':
        for archive in sorted(glob.glob(base + '_archived_*.csv')):
            try:
                with open(archive, newline='', encoding='utf-8', errors='replace') as f:
                    all_rows.extend(r for r in csv.DictReader(f) if row_matches(r.get('Timestamp','')))
            except Exception: pass

    if os.path.exists(PROCESS_LOG):
        try:
            with open(PROCESS_LOG, newline='', encoding='utf-8', errors='replace') as f:
                all_rows.extend(r for r in csv.DictReader(f) if row_matches(r.get('Timestamp','')))
        except Exception: pass

    seen_ts = set(); deduped = []
    for r in sorted(all_rows, key=lambda x: x.get('Timestamp','')):
        t = r.get('Timestamp','')
        if t not in seen_ts: seen_ts.add(t); deduped.append(r)

    step = 10 if range_param in ('week', 'lastweek') else 1
    matching = deduped[::step]
    timestamps_, table_speed_, puller_speed_, speed_ratio_, states_ = [], [], [], [], []
    for row in matching:
        timestamps_.append(row.get('Timestamp',''))
        # Support both old column names (pre-rewrite CSVs) and new names
        ts_val = row.get('Table_Speed') or row.get('realTableSpeed') or 0
        ps_val = row.get('Puller_Speed') or row.get('Puller_Actual_Speed') or 0
        try: table_speed_.append(float(ts_val or 0))
        except: table_speed_.append(0)
        try: puller_speed_.append(float(ps_val or 0))
        except: puller_speed_.append(0)
        try: speed_ratio_.append(float(row.get('Speed_Ratio') or 0) or None)
        except: speed_ratio_.append(None)
        states_.append(row.get('State_Name','') or '')
    return jsonify({'timestamps':timestamps_,'table_speed':table_speed_,
                    'puller_speed':puller_speed_,'speed_ratio':speed_ratio_,'states':states_})

@app.route('/home')
def home_hub():
    """Hub page linking to both braider dashboards."""
    try:
        # Braider 2 — local data, same source the main dashboard's pie uses
        with _lock:
            braider2_online = bool(_latest.get('connected'))
            braider2_state  = _latest.get('state_name') or 'Unknown'
        braider2_pcts = calculate_daily_state_percentages()
        braider2_util = round(braider2_pcts.get('RUNNING', 0), 1)

        # Braider 1 — read over SSH, same calculation logic applied remotely
        braider1_data = get_remote_braider_stats('braider1.local', 'Braider_1')
        braider1_online = braider1_data.get('online', False)
        braider1_state  = braider1_data.get('state', 'Offline')
        braider1_pcts   = braider1_data.get('pcts', {})
        braider1_util   = round(braider1_pcts.get('RUNNING', 0), 1)

        # Combined average utilization (only across machines currently online;
        # if both are offline, show 0 rather than dividing by zero)
        online_utils = []
        if braider1_online:
            online_utils.append(braider1_util)
        if braider2_online:
            online_utils.append(braider2_util)
        avg_util = round(sum(online_utils) / len(online_utils), 1) if online_utils else 0.0

        return render_template_string(HOME_HUB_HTML,
            braider1_online=braider1_online,
            braider1_state=braider1_state,
            braider1_pcts=braider1_pcts,

            braider2_online=braider2_online,
            braider2_state=braider2_state,
            braider2_pcts=braider2_pcts,

            avg_util=avg_util,
            current_time=datetime.now().strftime('%I:%M %p')
        )
    except Exception as e:
        log.error(f'Home hub error: {e}')
        return f'<h1>Error loading home page</h1><p>{e}</p>', 500


def get_remote_braider_stats(host, braider_id):
    """
    SSH into another braider's Pi and compute the same daily RUNNING/STOPPED/etc.
    percentage breakdown that calculate_daily_state_percentages() produces locally,
    using only today's rows from its process_log (read remotely, no files written).
    Falls back to {'online': False} on any connection or parsing failure.

    Requires passwordless (key-based) SSH from this Pi to `host` as user `pi`.
    BatchMode=yes ensures we fail fast instead of hanging on a password prompt
    if the key isn't set up yet.
    """
    import subprocess
    try:
        today_str = datetime.now().strftime('%Y-%m-%d')
        remote_path = f'~/braider_logs/{braider_id}_process_log.csv'
        ssh_opts = '-o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=accept-new'

        cmd = f"ssh {ssh_opts} pi@{host} \"grep '^{today_str}' {remote_path}\""
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=6)

        if result.returncode != 0:
            log.warning(f'[{braider_id}] SSH to {host} failed (code {result.returncode}): {result.stderr.strip()}')
            return {'online': False, 'state': 'Offline', 'pcts': {}}

        if not result.stdout.strip():
            log.warning(f'[{braider_id}] SSH to {host} succeeded but no rows matched today ({today_str})')
            return {'online': False, 'state': 'No Data Today', 'pcts': {}}

        # Need the header separately since grep on today's rows won't include it
        header_cmd = f"ssh {ssh_opts} pi@{host} \"head -1 {remote_path}\""
        header_result = subprocess.run(header_cmd, shell=True, capture_output=True, text=True, timeout=6)
        if header_result.returncode != 0:
            log.warning(f'[{braider_id}] SSH header read failed: {header_result.stderr.strip()}')
            return {'online': False, 'state': 'Offline', 'pcts': {}}

        headers = header_result.stdout.strip().split(',')
        state_col = headers.index('State_Name') if 'State_Name' in headers else 3

        state_counts = {}
        total_rows = 0
        last_state = 'Unknown'
        for line in result.stdout.strip().split('\n'):
            parts = line.split(',')
            if len(parts) > state_col:
                sname = parts[state_col].strip() or 'UNKNOWN'
                state_counts[sname] = state_counts.get(sname, 0) + 1
                total_rows += 1
                last_state = sname

        if total_rows == 0:
            return {'online': False, 'state': 'No Data', 'pcts': {}}

        pcts = {state: round((count / total_rows) * 100, 1) for state, count in state_counts.items()}
        return {'online': True, 'state': last_state, 'pcts': pcts}

    except subprocess.TimeoutExpired:
        log.warning(f'[{braider_id}] SSH to {host} timed out')
        return {'online': False, 'state': 'Offline', 'pcts': {}}
    except Exception as e:
        log.error(f'[{braider_id}] Error reading via SSH from {host}: {e}')
        return {'online': False, 'state': 'Offline', 'pcts': {}}
 
 
# ============================================================================
# HTML template with same styling as main dashboard
# ============================================================================
 
HOME_HUB_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Braider Hub — Noble Gas Systems</title>
    <style>
        body  { font-family: monospace; background:#1a1a1a; color:#e0e0e0; padding:20px; margin:0; }
        h1    { color:#4fc3f7; margin-bottom:4px; font-size:28px; }
        .sub  { color:#888; font-size:13px; margin-bottom:20px; }

        .container { max-width:1200px; margin:0 auto; }

        .section { font-size:11px; color:#555; text-transform:uppercase; letter-spacing:2px; margin:20px 0 8px; }

        .hub-grid {
            display:grid;
            grid-template-columns:repeat(auto-fit, minmax(320px, 1fr));
            gap:12px;
            margin-bottom:20px;
        }

        .braider-card {
            background:#2a2a2a;
            border-radius:8px;
            padding:14px;
            border-left:3px solid #4fc3f7;
        }

        .card-header {
            display:flex;
            justify-content:space-between;
            align-items:center;
            margin-bottom:12px;
            padding-bottom:8px;
            border-bottom:1px solid #444;
        }

        .card-title { font-size: 18px; font-weight: bold; color: #4fc3f7; }

        .status-badge {
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: bold;
            text-transform:uppercase;
        }

        .status-online  { background: #1b5e20; color: #66bb6a; }
        .status-offline { background: #b71c1c; color: #ef9a9a; }

        .stat-row {
            display: flex;
            justify-content: space-between;
            padding: 6px 0;
            border-bottom: 1px solid #333;
            font-size: 12px;
        }

        .stat-label { color: #888; text-transform:uppercase; font-size:9px; letter-spacing:1px; }
        .stat-value { color: #e0e0e0; font-weight: bold; }

        .pie-row {
            display:flex; align-items:center; gap:14px; margin-top:10px;
        }

        .pie-value {
            font-size:22px; font-weight:bold; margin-bottom:4px;
        }

        .pie-legend {
            font-size:10px; line-height:1.6; color:#aaa;
        }

        .button-group { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 14px; }
        .btn {
            padding: 6px 10px; border: none; border-radius: 4px; font-size: 11px;
            font-weight: bold; cursor: pointer; text-decoration: none; display: inline-block;
            text-align: center; transition: all 0.2s; font-family: monospace;
            text-transform:uppercase; letter-spacing:1px;
        }
        .btn-primary { background: #4fc3f7; color: #1a1a1a; }
        .btn-primary:hover { background: #81d4fa; box-shadow: 0 0 8px rgba(79, 195, 247, 0.5); }
        .btn-secondary { background: #333; color: #4fc3f7; border: 1px solid #4fc3f7; }
        .btn-secondary:hover { background: #4fc3f7; color: #1a1a1a; }

        .avg-card {
            background:#2a2a2a; border-radius:8px; padding:14px;
            margin-bottom:20px; border-left:3px solid #4fc3f7;
            display:flex; align-items:center; gap:18px;
        }
        .avg-label {
            font-size: 11px; font-weight: bold; color: #888;
            text-transform:uppercase; letter-spacing:2px;
        }
        .avg-value {
            font-size: 32px; font-weight: bold; color: #4fc3f7;
        }

        .footer {
            text-align: center; color: #888; font-size: 10px;
            margin-top: 20px; padding-top: 12px; border-top: 1px solid #333;
        }

        @media (max-width: 768px) {
            .hub-grid { grid-template-columns: 1fr; }
            .pie-row { flex-direction:column; align-items:flex-start; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Braider Hub — Noble Gas Systems</h1>
        <div class="sub">
            Production monitoring hub &nbsp;|&nbsp;
            Updated: {{ current_time }}
        </div>

        <div class="section">Combined</div>
        <div class="avg-card">
            <div class="avg-label">Average Utilization<br>(Today, both braiders)</div>
            <div class="avg-value">{{ avg_util }}%</div>
        </div>

        <div class="section">Active Braiders</div>
        <div class="hub-grid">
            <!-- Braider 1 Card -->
            <div class="braider-card">
                <div class="card-header">
                    <div class="card-title">Braider 1</div>
                    <div class="status-badge {% if braider1_online %}status-online{% else %}status-offline{% endif %}">
                        {% if braider1_online %}Online{% else %}Offline{% endif %}
                    </div>
                </div>

                <div class="stat-row">
                    <span class="stat-label">State</span>
                    <span class="stat-value">{{ braider1_state }}</span>
                </div>

                <div class="pie-row">
                    <canvas id="pie1" width="90" height="90"></canvas>
                    <div>
                        <div class="pie-value" id="pie1-value">—</div>
                        <div class="pie-legend" id="pie1-legend"></div>
                    </div>
                </div>

                <div class="button-group">
                    <a href="http://braider1.local:5000" class="btn btn-primary" target="_blank">Dashboard</a>
                    <a href="http://braider1.local:5000/floor" class="btn btn-secondary" target="_blank">Report</a>
                </div>
            </div>

            <!-- Braider 2 Card -->
            <div class="braider-card">
                <div class="card-header">
                    <div class="card-title">Braider 2</div>
                    <div class="status-badge {% if braider2_online %}status-online{% else %}status-offline{% endif %}">
                        {% if braider2_online %}Online{% else %}Offline{% endif %}
                    </div>
                </div>

                <div class="stat-row">
                    <span class="stat-label">State</span>
                    <span class="stat-value">{{ braider2_state }}</span>
                </div>

                <div class="pie-row">
                    <canvas id="pie2" width="90" height="90"></canvas>
                    <div>
                        <div class="pie-value" id="pie2-value">—</div>
                        <div class="pie-legend" id="pie2-legend"></div>
                    </div>
                </div>

                <div class="button-group">
                    <a href="http://braider2.local:5000" class="btn btn-primary" target="_blank">Dashboard</a>
                    <a href="http://braider2.local:5000/floor" class="btn btn-secondary" target="_blank">Report</a>
                </div>
            </div>
        </div>

        <div class="footer">
            Bookmark <strong>braider2.local:5000/home</strong> for quick access
        </div>
    </div>

    <script>
        // Same color map and drawing logic as the main dashboard's OEE pie
        const stateColors = {
            'RUNNING':'#66bb6a','READY':'#4fc3f7','STOPPED':'#ef5350',
            'PAUSED':'#ffa726','OFF':'#78909c','ABORTED':'#b71c1c','UNKNOWN':'#555'
        };

        function drawHubPie(canvasId, valueId, legendId, pcts) {
            const valueEl  = document.getElementById(valueId);
            const legendEl = document.getElementById(legendId);
            const canvas   = document.getElementById(canvasId);
            if (!pcts || !Object.keys(pcts).length) {
                if (valueEl)  valueEl.textContent = '—';
                if (legendEl) legendEl.textContent = 'No data';
                return;
            }
            const runningPct = pcts['RUNNING'] || 0;
            if (valueEl) {
                valueEl.textContent = runningPct.toFixed(1) + '%';
                valueEl.style.color = runningPct>=50 ? '#66bb6a' : runningPct>=25 ? '#ffa726' : '#ef5350';
            }
            if (legendEl) {
                legendEl.innerHTML = Object.entries(pcts).map(([s,p]) =>
                    `<div><span style="display:inline-block;width:8px;height:8px;background:${stateColors[s]||'#999'};margin-right:4px;border-radius:2px;"></span>${s}: ${p}%</div>`
                ).join('');
            }
            if (canvas) {
                const ctx = canvas.getContext('2d');
                ctx.clearRect(0,0,90,90);
                const cx=45, cy=45, r=40;
                let angle = -Math.PI/2;
                for (const [s,p] of Object.entries(pcts)) {
                    if (p<=0) continue;
                    const sweep = (p/100)*(2*Math.PI);
                    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.arc(cx,cy,r,angle,angle+sweep);
                    ctx.closePath(); ctx.fillStyle = stateColors[s]||'#999'; ctx.fill();
                    ctx.strokeStyle='#1a1a1a'; ctx.lineWidth=1.5; ctx.stroke();
                    angle += sweep;
                }
                ctx.beginPath(); ctx.arc(cx,cy,r*0.52,0,Math.PI*2);
                ctx.fillStyle='#2a2a2a'; ctx.fill();
            }
        }

        drawHubPie('pie1', 'pie1-value', 'pie1-legend', {{ braider1_pcts | tojson }});
        drawHubPie('pie2', 'pie2-value', 'pie2-legend', {{ braider2_pcts | tojson }});

        // Auto-refresh every 30 seconds so SSH-fetched Braider 1 data stays current
        setTimeout(() => location.reload(), 30000);
    </script>
</body>
</html>
'''


# ── Sleep prevention ──────────────────────────────────────────────────────────

def prevent_sleep():
    import platform
    if platform.system() == 'Windows':
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001 | 0x00000002)
        log.info('Sleep prevention active (Windows).')
    else:
        log.info('Linux detected — sleep prevention handled by systemd.')


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    prevent_sleep()

    # Independent archiver — never touches the PLC loop
    archiver_thread = threading.Thread(target=independent_archiver_loop, daemon=True, name='archiver')
    archiver_thread.start()

    # Hi-res ring buffer thread — 0.5s polling, writes only on events
    hires_thread = threading.Thread(target=hires_loop, daemon=True, name='hires')
    hires_thread.start()

    # OEE thread — 60s polling, own connection, feeds recipe/PPI/cum-hours to monitor_loop
    oee_thread = threading.Thread(target=oee_loop, daemon=True, name='oee')
    oee_thread.start()

    monitor_thread = threading.Thread(target=monitor_loop, daemon=True, name='monitor')
    monitor_thread.start()

    log.info('Dashboard: http://0.0.0.0:5000  |  Floor: http://0.0.0.0:5000/floor')
    import logging as _logging
    _logging.getLogger('werkzeug').setLevel(_logging.ERROR)
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
