"""
braider_monitor.py
Noble Gas Systems — Steeger HS120/48 IMC-7K Braider Monitor
Raspberry Pi production logger — runs as a systemd service

Logs:
  - process_log.csv      : 2s poll, fast telemetry only (speeds, position, servos)
  - oee_log.csv          : 60s poll, cumulative state time + recipe context
  - event_log.csv        : RBE — discrete transitions (state change, faults 0→1,
                           wire break detected, recipe load, safety inputs, etc.)
  - wire_break_log.csv   : one row per discrete wire break event with bobbin/carrier
                           identification parsed from the changed bit mask

Archiving:
  - process_log.csv      : weekly split every Sunday midnight (Pi clock, independent
                           of PLC connection) via background archiver thread
  - event_log / oee_log / wire_break_log : monthly split on 1st of month

Flask dashboard at http://<pi-ip>:5000
Floor report   at http://<pi-ip>:5000/floor

Author: Joseph J Patronite, Noble Gas Systems
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
BRAIDER_ID = 'Braider_2'   # Change to 'Braider_1' on the other machine

LOG_DIR = os.path.expanduser('~/braider_logs')

PROCESS_LOG    = os.path.join(LOG_DIR, f'{BRAIDER_ID}_process_log.csv')
EVENT_LOG      = os.path.join(LOG_DIR, f'{BRAIDER_ID}_event_log.csv')
WIRE_BREAK_LOG = os.path.join(LOG_DIR, f'{BRAIDER_ID}_wire_break_log.csv')
OEE_LOG        = os.path.join(LOG_DIR, f'{BRAIDER_ID}_oee_log.csv')

FAST_POLL_INTERVAL       = 2    # seconds
OEE_POLL_INTERVAL        = 60   # seconds
PRE_BREAK_BUFFER_SECONDS = 5
POST_BREAK_CAPTURE_SECONDS = 5

# Startup grace period — ignore wire bit changes while servo axes are settling
STARTUP_GRACE_SECONDS = 5

STATE_CODES = {
    1:   'OFF',
    2:   'READY',
    4:   'STOPPED',
    8:   'STARTING',
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
    'realTableSpeed',                           # filtered table rev/s (YES / process_log)
    'Table_Position',
    'Active_Segment',
    # Wire break detection
    'Local:1:I.Data',                           # 16-bit wire break bitmask
    'Local:1:I.Fault',                          # I/O module fault flag
    'WIre_Break_Detected',                      # PLC-level break flag (typo is intentional)
    'Core_Break',
    # Safety inputs
    'I_Door_Interlock_Ok',
    'I_Emergency_Stop_Ok',
    'I_Table_Motor_OL',
    'I_CoreBreak_Sensor',
    'I_Triaxial_WB',
    'Machine.Estops_Ok',
    'Machine.Guards_Ok',
    'Machine.All_Safties_Ok',
    'Machine.All_Axes_Ok',
    'Machine.All_Axes_Running',
    # Servo sync
    'AxisSynced_OS1',
    'AxisSynced_OS2',
    'AxisSynced_OS3',
    'AxisSynced_OS4',
    'AxisSynced_OS5',
    # State elapsed time components
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
    'No_Machine_Faults',
    'No_Machine_Msgs',
    'Machine_Faults',
    # Wire break recovery
    'WireBreak_Move',
    'EStop_Recover',
    # Program-scoped — segment progress & recovery (always polled, log in process_log)
    'Program:MainProgram.Fault_WireBreak',
    'Program:MainProgram.Fault_EStop',
    'Program:MainProgram.Fault_GuardDoor',
    'Program:MainProgram.Fault_PullerServo',
    'Program:MainProgram.Fault_TableServo',
    'Program:MainProgram.Recover_Step',
    'Program:MainProgram.Puller_Current_Dist',
    'Program:MainProgram.Table_Current_Dist',
    'Program:P01_TableDrive.Servo_Axis_Faults',
    # Servo axis sub-tags for process log & wire break pre-detection research
    'servoPuller_Axis.ActualPosition',
    'servoPuller_Axis.CommandPosition',
    'servoPuller_Axis.ActualVelocity',
    'servoPuller_Axis.CommandVelocity',
    'servoPuller_Axis.MotionStatus',
    'servoTable_Axis.VelocityFeedback',         # raw encoder — wire break pre-detection
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
    # Recipe params (slow-changing)
    'Discrete_Distance',
    'Discrete_Loops',
    'Loop_Length_Feet',
    'Carrier_Mode',
    'Current_Ratio',
    'Low_PPI',
    'Hi_PPI',
    'Hi_PPI_Running',
    'Base_Ratio',
]

# CHANGE_TAGS — RBE tags written to event_log only on 0→1 or 1→0 transition.
# These are the 11 tags previously being polled every 2s into event_log.
# All are BOOLs unless otherwise noted.
CHANGE_TAGS = [
    'Fault_9',
    'Fault_13',
    'Fault_Cam',
    'Fault_Calc',
    'Core_Break',
    'EStop_Recover',
    'I_Table_Motor_OL',
    'I_Door_Interlock_Ok',
    'I_Emergency_Stop_Ok',
    'I_CoreBreak_Sensor',
    'I_Triaxial_WB',
    'Table_Drive:I.Faulted',
    'AxisSynced_OS1',
    'AxisSynced_OS2',
    'AxisSynced_OS3',
    'AxisSynced_OS4',
    'AxisSynced_OS5',
    'WIre_Break_Detected',
    'Puller_Position_Error',
    'New_Part_Latch',
    'Run_Complete',
    'Transition_Active',
    'PPI_Change_ONS',
    'Sensor_Mode_Enable',
    # Machine_Faults is a DINT — compare on any non-zero value change
    'Machine_Faults',
]

# ── Setup ─────────────────────────────────────────────────────────────────────

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
    """Background thread: checks wall-clock every 30 s and archives when needed."""
    global _last_archive_check_minute
    log.info('Archiver thread started.')
    while True:
        try:
            now = datetime.now()
            minute_key = (now.year, now.month, now.day, now.hour, now.minute)

            if minute_key != _last_archive_check_minute:
                _last_archive_check_minute = minute_key
                _run_archive_checks(now)
        except Exception as e:
            log.error(f'Archiver thread error: {e}')
        time.sleep(30)


def _run_archive_checks(now: datetime):
    """Execute file rotations if calendar conditions are met."""
    is_sunday_midnight     = (now.weekday() == 6 and now.hour == 0 and now.minute == 0)
    is_monthstart_midnight = (now.day == 1 and now.hour == 0 and now.minute == 0)

    archived_any = False

    if is_sunday_midnight:
        week_label = now.strftime('%Y_%m_%d')
        if os.path.exists(PROCESS_LOG) and os.path.getsize(PROCESS_LOG) > 0:
            archive_name = PROCESS_LOG.replace('.csv', f'_week_ending_{week_label}.csv')
            try:
                os.rename(PROCESS_LOG, archive_name)
                log.info(f'Weekly archive: {os.path.basename(PROCESS_LOG)} → {os.path.basename(archive_name)}')
                archived_any = True
            except OSError as e:
                log.error(f'Weekly archive failed: {e}')

    if is_monthstart_midnight:
        month_label = now.strftime('%Y_%m')
        for filepath in [EVENT_LOG, OEE_LOG, WIRE_BREAK_LOG]:
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                archive_name = filepath.replace('.csv', f'_{month_label}.csv')
                try:
                    os.rename(filepath, archive_name)
                    log.info(f'Monthly archive: {os.path.basename(filepath)} → {os.path.basename(archive_name)}')
                    archived_any = True
                except OSError as e:
                    log.error(f'Monthly archive failed for {filepath}: {e}')

    if archived_any:
        log.info('Archive pass complete.')


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
                        if len(parts) > 3:
                            row_timestamp = parts[0]
                            if row_timestamp.startswith(today_str):
                                sname = parts[3]
                                if sname:
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
    'fault_puller_servo':     None,
    'fault_table_servo':      None,
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

_rolling_buffer = deque(maxlen=int(PRE_BREAK_BUFFER_SECONDS / FAST_POLL_INTERVAL) + 5)

# Wire break capture state
_wb_capturing        = False
_wb_capture_until    = 0.0
_wb_capture_rows     = []


# ── Monitor loop ──────────────────────────────────────────────────────────────

def monitor_loop():
    global _wb_capturing, _wb_capture_until, _wb_capture_rows

    prev_state     = None
    prev_wire_bits = None
    prev_change    = {}   # {tag_name: last_logged_value} for RBE change tracking
    last_oee_poll  = 0
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
                    no_faults     = d.get('No_Machine_Faults')
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

                    # ── OEE poll (first loop + every 60s) ───────────────────
                    cum_running = cum_stopped = cum_ready = None
                    recipe_modified = mandrel_mode = None

                    if last_oee_poll == 0 or (now - last_oee_poll >= OEE_POLL_INTERVAL):
                        oee_results = plc.read(*OEE_TAGS)
                        od = {r.tag: r.value for r in oee_results if r.error is None}

                        stats = od.get('Machine_Statistics', {})
                        if isinstance(stats, dict):
                            cum = stats.get('Cum_State_Time', {})
                            if isinstance(cum, dict):
                                cum_running = cum.get('Running', {}).get('Hours')
                                cum_stopped = cum.get('Stopped', {}).get('Hours')
                                cum_ready   = cum.get('Ready',   {}).get('Hours')

                        recipe_raw = od.get('CurrentRecipe', {})
                        if isinstance(recipe_raw, dict):
                            recipe_name = recipe_raw.get('Name', 'Unknown')

                            hi_ppi     = od.get('Hi_PPI')
                            low_ppi    = od.get('Low_PPI')
                            hi_running = od.get('Hi_PPI_Running')
                            try:
                                if hi_running == 1 and hi_ppi is not None:
                                    recipe_ppi = hi_ppi
                                else:
                                    segments   = recipe_raw.get('Segments', [])
                                    seg_idx    = int(od.get('Active_Segment') or 1)
                                    seg_data   = segments[seg_idx] if segments else None
                                    seg_picks  = seg_data.get('Picks') if seg_data else None
                                    recipe_ppi = seg_picks if (seg_picks and seg_picks > 0) else recipe_raw.get('Connector_PPI')
                            except Exception:
                                recipe_ppi = recipe_raw.get('Connector_PPI')

                            recipe_modified = od.get('Recipe_Modified')
                            mandrel_mode    = od.get('HMI_Mandrel_Mode')

                        oee_row = {
                            'Timestamp':          timestamp,
                            'Braider_ID':         BRAIDER_ID,
                            'Machine_State':      machine_state,
                            'State_Name':         state_name(machine_state) if machine_state else '',
                            'Recipe_Name':        recipe_name,
                            'Recipe_Number':      od.get('HMI_Recipe_Number'),
                            'Recipe_PPI':         recipe_ppi,
                            'Recipe_Modified':    recipe_modified,
                            'Mandrel_Mode':       mandrel_mode,
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
                        write_csv_row(OEE_LOG, oee_row)
                        last_oee_poll = now

                    # ── Process log row ──────────────────────────────────────
                    process_row = {
                        'Timestamp':             timestamp,
                        'Braider_ID':            BRAIDER_ID,
                        'Machine_State':         machine_state,
                        'State_Name':            state_name(machine_state) if machine_state else '',
                        'Table_Speed':           round(table_speed, 6)  if table_speed  else None,
                        'Puller_Speed':          round(puller_speed, 6) if puller_speed else None,
                        'Speed_Ratio':           speed_ratio,
                        'Puller_Pos_Feet':       round(puller_feet, 4)  if puller_feet  else None,
                        'Table_Position':        round(table_pos, 4)    if table_pos    else None,
                        'Active_Segment':        active_seg,
                        'State_Elapsed_Secs':    state_elapsed_s,
                        'No_Faults':             no_faults,
                        'No_Msgs':               d.get('No_Machine_Msgs'),
                        'Machine_Faults':        d.get('Machine_Faults'),
                        'Wire_Break_Bits':       wire_bits,
                        'IO_Decoded':            decode_io_str(wire_bits),
                        'Wire_Input_Fault':      d.get('Local:1:I.Fault'),
                        'Wire_Break_Detected':   d.get('WIre_Break_Detected'),
                        'Core_Break':            d.get('Core_Break'),
                        'Door_Ok':               d.get('I_Door_Interlock_Ok'),
                        'Estop_Ok':              d.get('I_Emergency_Stop_Ok'),
                        'Guards_Ok':             d.get('Machine.Guards_Ok'),
                        'All_Safties_Ok':        d.get('Machine.All_Safties_Ok'),
                        'All_Axes_Ok':           d.get('Machine.All_Axes_Ok'),
                        'All_Axes_Running':      d.get('Machine.All_Axes_Running'),
                        'AxisSynced_1':          d.get('AxisSynced_OS1'),
                        'AxisSynced_2':          d.get('AxisSynced_OS2'),
                        'AxisSynced_3':          d.get('AxisSynced_OS3'),
                        'AxisSynced_4':          d.get('AxisSynced_OS4'),
                        'AxisSynced_5':          d.get('AxisSynced_OS5'),
                        'Recipe_Name':           recipe_name,
                        'Recipe_PPI':            recipe_ppi,
                        'VFD_Freq_Actual':       d.get('Table_Drive:I.OutputFreq'),
                        'VFD_Freq_Command':      d.get('Table_Drive:O.FreqCommand'),
                        'VFD_Freq_Delta':        (
                            (d.get('Table_Drive:O.FreqCommand') or 0) -
                            (d.get('Table_Drive:I.OutputFreq')  or 0)
                        ),
                        'VFD_Faulted':           d.get('Table_Drive:I.Faulted'),
                        'VFD_Active':            d.get('Table_Drive:I.Active'),
                        'VFD_AtReference':       d.get('Table_Drive:I.AtReference'),
                        'Transition_Active':     d.get('Transition_Active'),
                        'Taper_Sensor':          d.get('Taper_Sensor_Input'),
                        'Sensor_Mode':           d.get('Sensor_Mode_Enable'),
                        'Length_To_Run':         d.get('Length_To_Run'),
                        'Run_Complete':          d.get('Run_Complete'),
                        'WireBreak_Move':        d.get('WireBreak_Move'),
                        'EStop_Recover':         d.get('EStop_Recover'),
                        'I_Table_Motor_OL':      d.get('I_Table_Motor_OL'),
                        'I_CoreBreak_Sensor':    d.get('I_CoreBreak_Sensor'),
                        'I_Triaxial_WB':         d.get('I_Triaxial_WB'),
                        # Program-scoped
                        'Fault_WireBreak':       d.get('Program:MainProgram.Fault_WireBreak'),
                        'Fault_EStop':           d.get('Program:MainProgram.Fault_EStop'),
                        'Fault_GuardDoor':       d.get('Program:MainProgram.Fault_GuardDoor'),
                        'Fault_PullerServo':     d.get('Program:MainProgram.Fault_PullerServo'),
                        'Fault_TableServo':      d.get('Program:MainProgram.Fault_TableServo'),
                        'Recover_Step':          d.get('Program:MainProgram.Recover_Step'),
                        'Puller_Current_Dist':   d.get('Program:MainProgram.Puller_Current_Dist'),
                        'Table_Current_Dist':    d.get('Program:MainProgram.Table_Current_Dist'),
                        'Servo_Axis_Faults':     d.get('Program:P01_TableDrive.Servo_Axis_Faults'),
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

                    # ── Wire break post-capture ───────────────────────────────
                    if _wb_capturing:
                        _wb_capture_rows.append(process_row.copy())
                        if now >= _wb_capture_until:
                            log.info(f'Wire break capture complete — {len(_wb_capture_rows)} rows in buffer.')
                            # The wire_break_log gets one summary row per break event (written at
                            # break detection below).  The raw high-res window is available in the
                            # rolling process_log for later ML training data extraction.
                            _wb_capturing    = False
                            _wb_capture_rows = []

                    # ── State change → event_log ─────────────────────────────
                    if machine_state != prev_state and prev_state is not None:
                        _write_event(timestamp, 'STATE_CHANGE',
                                     from_val=state_name(prev_state),
                                     to_val=state_name(machine_state) if machine_state else '',
                                     from_code=prev_state,
                                     to_code=machine_state,
                                     puller_feet=puller_feet,
                                     recipe_name=recipe_name,
                                     d=d)
                        log.info(f'State: {state_name(prev_state)} → {state_name(machine_state)}')

                    if machine_state == 16 and prev_state != 16:
                        running_started_at = now
                    prev_state = machine_state

                    # ── RBE CHANGE_TAGS → event_log ──────────────────────────
                    # One row written only when a tag transitions (0→1 or 1→0 for BOOLs;
                    # any value change for DINT/REAL).
                    for tag in CHANGE_TAGS:
                        current_val = d.get(tag)
                        if current_val is None:
                            continue
                        last_val = prev_change.get(tag)
                        if last_val is None:
                            # First poll after (re)connect — seed without logging
                            prev_change[tag] = current_val
                            continue
                        if current_val != last_val:
                            # Determine transition label for BOOLs
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

                    # ── Wire break detection → event_log + wire_break_log ────
                    # Primary signal: WIre_Break_Detected (PLC-managed BOOL, typo intentional)
                    # Local:1:I.Data is operator input context only — not used for detection.
                    # Startup grace period prevents false triggers during servo settling.
                    in_startup = (
                        running_started_at is not None and
                        (now - running_started_at) < STARTUP_GRACE_SECONDS
                    )

                    wire_break_detected = d.get('WIre_Break_Detected')
                    prev_wb_detected    = prev_change.get('WIre_Break_Detected')

                    if (machine_state == 16 and not in_startup and
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

                        _wb_capture_rows  = list(_rolling_buffer)
                        _wb_capturing     = True
                        _wb_capture_until = now + POST_BREAK_CAPTURE_SECONDS

                    prev_wire_bits = wire_bits

                    # ── Update Flask shared state ────────────────────────────
                    with _lock:
                        _latest.update({
                            'timestamp':           timestamp,
                            'machine_state':       machine_state,
                            'state_name':          state_name(machine_state) if machine_state else 'Unknown',
                            'table_speed':         round(table_speed, 4)  if table_speed  else None,
                            'puller_speed':        round(puller_speed, 4) if puller_speed else None,
                            'speed_ratio':         speed_ratio,
                            'puller_pos_feet':     round(puller_feet, 2)  if puller_feet  else None,
                            'table_position':      round(table_pos, 2)    if table_pos    else None,
                            'active_segment':      active_seg,
                            'no_faults':           no_faults,
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
                            'cum_running_hrs':     cum_running if cum_running is not None else _latest.get('cum_running_hrs'),
                            'cum_stopped_hrs':     cum_stopped if cum_stopped is not None else _latest.get('cum_stopped_hrs'),
                            'cum_ready_hrs':       cum_ready   if cum_ready   is not None else _latest.get('cum_ready_hrs'),
                            'recipe_modified':     recipe_modified,
                            'mandrel_mode':        mandrel_mode,
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
                            'machine_faults':      d.get('Machine_Faults'),
                            'no_msgs':             d.get('No_Machine_Msgs'),
                            'wire_break_detected': d.get('WIre_Break_Detected'),
                            'core_break':          d.get('Core_Break'),
                            'i_table_motor_ol':    d.get('I_Table_Motor_OL'),
                            'i_triaxial_wb':       d.get('I_Triaxial_WB'),
                            'length_to_run':       d.get('Length_To_Run'),
                            'run_complete':        d.get('Run_Complete'),
                            'transition_active':   d.get('Transition_Active'),
                            'estop_recover':       d.get('EStop_Recover'),
                            'fault_wire_break':    d.get('Program:MainProgram.Fault_WireBreak'),
                            'fault_estop':         d.get('Program:MainProgram.Fault_EStop'),
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
        'No_Faults':   d.get('No_Machine_Faults'),
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
            <div class="label">Table Speed</div>
            <div class="value" id="table-value">{{ d.table_speed or '—' }}</div>
            <div class="unit">rev/s (filtered) &nbsp;|&nbsp; <span id="table-rpm-value" style="color:#4fc3f7">—</span> rpm</div>
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
            <div id="wb-div" class="value ok" style="font-size:18px;">
                <span id="wb-value">{{ d.wire_break_bits if d.wire_break_bits is not none else '—' }}</span>
            </div>
            <div id="io-decoded" style="margin-top:8px; line-height:2; font-size:11px;">—</div>
            <div class="unit" style="margin-top:4px;">normal running = 3 &nbsp;(EStop_OK + Door_Closed)</div>
        </div>

        <div class="card">
            <div class="label">Faults</div>
            <div id="fault-div" class="value {% if d.no_faults %}ok{% else %}fault blink{% endif %}">
                {% if d.no_faults %}NONE{% else %}FAULT{% endif %}
            </div>
            <div id="fault-unit" class="unit">
                {% if d.machine_faults and d.machine_faults != 4 %}code: {{ d.machine_faults }}{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Safety</div>
            <div class="checks">
                <span id="safety-estop" class="{{ 'ok' if d.estop_ok else 'fault blink' }}">
                    {{ '✓' if d.estop_ok else '✗' }} E-Stop
                </span><br>
                <span id="safety-door" class="{{ 'ok' if d.door_ok else 'warn' }}">
                    {{ '✓' if d.door_ok else '✗' }} Door
                </span><br>
                <span id="safety-guards" class="{{ 'ok' if d.guards_ok else 'fault' }}">
                    {{ '✓' if d.guards_ok else '✗' }} Guards
                </span><br>
                <span id="safety-motor" class="{{ 'fault blink' if d.i_table_motor_ol else 'ok' }}">
                    {{ '✗ MOTOR OL' if d.i_table_motor_ol else '✓ Motor OK' }}
                </span><br>
                <span id="safety-triaxial" class="{{ 'fault blink' if d.i_triaxial_wb else 'ok' }}">
                    {{ '✗ TRIAXIAL WB' if d.i_triaxial_wb else '✓ Triaxial OK' }}
                </span><br>
                <span id="safety-core" class="{{ 'fault blink' if d.core_break else 'ok' }}">
                    {{ '✗ CORE BREAK' if d.core_break else '✓ Core OK' }}
                </span>
            </div>
        </div>

        <div class="card">
            <div class="label">Servo Axis Sync</div>
            <div class="axes">
                {% for i, synced in [
                    (1, d.axis1_synced), (2, d.axis2_synced), (3, d.axis3_synced),
                    (4, d.axis4_synced), (5, d.axis5_synced)
                ] %}
                <span class="axis {{ 'axis-ok' if synced else 'axis-warn' }}">OS{{ i }}</span>
                {% endfor %}
            </div>
            <div class="unit" style="margin-top:8px">all green = normal</div>
        </div>

        <div class="card">
            <div class="label">Individual Faults</div>
            <div class="checks" style="font-size:11px;">
                <span id="fault-wb"     class="{{ 'fault blink' if d.fault_wire_break else 'ok' }}">{{ '✗ Wire Break' if d.fault_wire_break else '✓ Wire Break' }}</span><br>
                <span id="fault-es"     class="{{ 'fault blink' if d.fault_estop else 'ok' }}">{{ '✗ E-Stop' if d.fault_estop else '✓ E-Stop' }}</span><br>
                <span id="fault-puller" class="{{ 'fault blink' if d.fault_puller_servo else 'ok' }}">{{ '✗ Puller Servo' if d.fault_puller_servo else '✓ Puller Servo' }}</span><br>
                <span id="fault-table"  class="{{ 'fault blink' if d.fault_table_servo else 'ok' }}">{{ '✗ Table Servo' if d.fault_table_servo else '✓ Table Servo' }}</span>
            </div>
        </div>

        <div class="card">
            <div class="label">Recovery Step</div>
            <div class="value" style="font-size:26px">
                <span id="recover-step">{{ d.recover_step or '—' }}</span>
            </div>
            <div class="unit">wire break recovery progress</div>
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
    </div>

<script>
const MAX_POINTS = 75;
const tableSpeed  = Array(MAX_POINTS).fill(null);
const pullerSpeed = Array(MAX_POINTS).fill(null);
const speedRatio  = Array(MAX_POINTS).fill(null);
const timestamps  = Array(MAX_POINTS).fill('');
const machineStates = Array(MAX_POINTS).fill(0);
const canvas = document.getElementById('liveChart');
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
            const x0=PAD.left+(i/(MAX_POINTS-1))*plotW, x1=PAD.left+((i+1)/(MAX_POINTS-1))*plotW;
            canvasCtx.fillStyle=machineStates[i]===16?'rgba(102,187,106,0.12)':'rgba(144,164,174,0.08)';
            canvasCtx.fillRect(x0,panelTop(p),x1-x0,panelH);
        }
        canvasCtx.strokeStyle='#333'; canvasCtx.lineWidth=0.5;
        for (let i=0; i<=3; i++) {
            const y=panelTop(p)+(i/3)*panelH;
            canvasCtx.beginPath(); canvasCtx.moveTo(PAD.left,y); canvasCtx.lineTo(PAD.left+plotW,y); canvasCtx.stroke();
        }
    }
    function range(arr) {
        const vals=arr.filter(v=>v!==null&&isFinite(v));
        if (!vals.length) return [0,1];
        const mn=Math.min(...vals),mx=Math.max(...vals);
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
            if (!started) { canvasCtx.moveTo(x,y); started=true; } else canvasCtx.lineTo(x,y);
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
    const [tMn,tMx]=range(tableSpeed);
    drawBackground(0); drawLine(0,tableSpeed,'#4fc3f7',tMn,tMx); labelY(0,tMn,tMx,'#4fc3f7'); labelPanel(0,'Table Speed (rev/s)','#4fc3f7');
    const [pMn,pMx]=range(pullerSpeed);
    drawBackground(1); drawLine(1,pullerSpeed,'#81c784',pMn,pMx); labelY(1,pMn,pMx,'#81c784'); labelPanel(1,'Puller Speed (in/s)','#81c784');
    const [rMn,rMx]=range(speedRatio);
    drawBackground(2); drawLine(2,speedRatio,'#ffb74d',rMn,rMx); labelY(2,rMn,rMx,'#ffb74d'); labelPanel(2,'Speed Ratio','#ffb74d');
    canvasCtx.fillStyle='#555'; canvasCtx.font='9px monospace'; canvasCtx.textAlign='center';
    const xBottom=panelTop(2)+panelH+14;
    if (timestamps[0]) canvasCtx.fillText(timestamps[0],PAD.left,xBottom);
    if (timestamps[MAX_POINTS-1]) canvasCtx.fillText(timestamps[MAX_POINTS-1],PAD.left+plotW,xBottom);
    const mid=Math.floor(MAX_POINTS/2);
    if (timestamps[mid]) canvasCtx.fillText(timestamps[mid],PAD.left+plotW/2,xBottom);
}

let lastState=null, lastWBBits=null, lastSeenTimestamp='', timestampAgeTicks=0;

async function fetchAndUpdate() {
    try {
        const res=await fetch('/api/latest'); const data=await res.json();
        const now=new Date().toLocaleTimeString('en-US',{hour12:false});
        const ts=data.table_speed||0, ps=data.puller_speed||0, sr=data.speed_ratio||null;
        const wb=data.wire_break_bits, st=data.machine_state;
        let isStale=false;
        if (data.timestamp) {
            if (data.timestamp===lastSeenTimestamp) timestampAgeTicks++;
            else { lastSeenTimestamp=data.timestamp; timestampAgeTicks=0; }
            if (timestampAgeTicks>=5||!data.connected) isStale=true;
        } else { isStale=true; }
        tableSpeed.shift();  tableSpeed.push(isStale?null:ts);
        pullerSpeed.shift(); pullerSpeed.push(isStale?null:ps);
        speedRatio.shift();  speedRatio.push(isStale?null:sr);
        timestamps.shift();  timestamps.push(now);
        machineStates.shift();machineStates.push(isStale?0:(st||0));
        drawChart();
        const statusEl=document.getElementById('conn-status');
        if (isStale||!data.connected) { statusEl.textContent='STALE DATA — PLC UNREACHABLE'; statusEl.className='fault'; }
        else { statusEl.textContent='CONNECTED'; statusEl.className='ok'; }
        document.getElementById('last-update').textContent='updated '+now;
        const hts=document.getElementById('header-timestamp');
        if (hts) hts.textContent=isStale?'—':(data.timestamp||'—');
        const upd=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=isStale?'—':v;};
        upd('feet-value',   data.puller_pos_feet ? data.puller_pos_feet.toFixed(2) : '—');
        upd('table-value',  ts ? ts.toFixed(4) : '—');
        upd('table-rpm-value', ts ? (ts*60).toFixed(1) : '—');
        upd('puller-value', ps ? ps.toFixed(4) : '—');
        upd('ratio-value',  sr ? sr.toFixed(5) : '—');
        const taperEl=document.getElementById('taper-value');
        if (taperEl) taperEl.textContent=(data.taper_sensor!==null&&data.taper_sensor>0)?data.taper_sensor.toFixed(2):'—';
        upd('puller-vel-err', data.puller_vel_cmd_err!==null&&data.puller_vel_cmd_err!==undefined?data.puller_vel_cmd_err.toFixed(5):'—');
        upd('table-vel-fb',   data.table_vel_feedback!==null&&data.table_vel_feedback!==undefined?data.table_vel_feedback.toFixed(4):'—');
        upd('recover-step', data.recover_step!==null?data.recover_step:'—');
        upd('vfd-actual',   data.vfd_freq_actual!==null?data.vfd_freq_actual:'—');
        upd('vfd-command',  data.vfd_freq_command!==null?data.vfd_freq_command:'—');
        upd('vfd-delta',    data.vfd_freq_delta!==null?data.vfd_freq_delta:'0');
        const vfdRef=document.getElementById('vfd-at-ref');
        if (vfdRef) vfdRef.innerHTML=data.vfd_at_ref?'&nbsp;<span class="ok">AT REF</span>':'';
        upd('wb-value', wb!==null?wb:'—');

        // Decode Local:1:I.Data bits dynamically
        const IO_BIT_LABELS = [
            'EStop_OK','Door_Closed','Puller_SSW_Close','Puller_SSW_Open',
            'Start_PB','Stop_PB','Jog_Fwd','TakeUp_OL',
            'Upper_Prox','Lower_Prox','WireBreak_SW','Triaxial_SW'
        ];
        const ioEl = document.getElementById('io-decoded');
        if (ioEl) {
            if (wb !== null && wb >= 0 && !isStale) {
                let html = '';
                IO_BIT_LABELS.forEach((label, bit) => {
                    const active = (wb & (1 << bit)) !== 0;
                    const color  = active ? '#66bb6a' : '#555';
                    const dot    = active ? '●' : '○';
                    html += `<span style="color:${color};display:inline-block;margin-right:8px;font-size:11px;">${dot} ${label}</span>`;
                });
                ioEl.innerHTML = html;
            } else {
                ioEl.textContent = isStale ? '—' : (wb !== null && wb < 0 ? 'INVALID' : '—');
            }
        }
        const wbDiv = document.getElementById('wb-div');
        if (wbDiv) {
            const abnormal = wb !== null && wb !== 3 && wb !== 1 && wb >= 0 && !isStale;
            wbDiv.className = 'value ' + (abnormal ? 'warn' : 'ok');
        }(id,ok,okText,faultText,faultClass) {
            const el=document.getElementById(id); if (!el) return;
            el.textContent=(ok&&!isStale)?'✓ '+okText:'✗ '+(isStale?'DATA STALE':faultText);
            el.className=(ok&&!isStale)?'ok':faultClass;
        }
        setSafety('safety-estop',   data.estop_ok,          'E-Stop',   'E-STOP PRESSED','fault blink');
        setSafety('safety-door',    data.door_ok,           'Door',     'Door Open',     'warn');
        setSafety('safety-guards',  data.guards_ok,         'Guards',   'Guards Open',   'fault');
        setSafety('safety-motor',   !data.i_table_motor_ol, 'Motor OK', 'MOTOR OL',      'fault blink');
        setSafety('safety-triaxial',!data.i_triaxial_wb,    'Triaxial OK','TRIAXIAL WB', 'fault blink');
        setSafety('safety-core',    !data.core_break,       'Core OK',  'CORE BREAK',    'fault blink');
        function setFault(id,active,okText,faultText) {
            const el=document.getElementById(id); if(!el)return;
            el.textContent=active?'✗ '+faultText:'✓ '+okText;
            el.className=active?'fault blink':'ok';
        }
        setFault('fault-wb',     data.fault_wire_break,   'Wire Break',  'Wire Break');
        setFault('fault-es',     data.fault_estop,        'E-Stop',      'E-Stop');
        setFault('fault-puller', data.fault_puller_servo, 'Puller Servo','Puller Servo');
        setFault('fault-table',  data.fault_table_servo,  'Table Servo', 'Table Servo');
        const elapsed=data.state_elapsed_s;
        const elapsedEl=document.getElementById('elapsed-value');
        if (elapsedEl) {
            if (elapsed&&!isStale) {
                const h=Math.floor(elapsed/3600), m=Math.floor((elapsed%3600)/60);
                elapsedEl.textContent=h+'h '+m+'m';
            } else { elapsedEl.textContent='—'; }
        }
        const pcts=data.daily_state_pcts||{};
        const oeeEl=document.getElementById('oee-value'), legendEl=document.getElementById('oee-legend');
        const pieCanvas=document.getElementById('oeePieCanvas');
        const stateColors={'RUNNING':'#66bb6a','READY':'#4fc3f7','STOPPED':'#ef5350','PAUSED':'#ffa726','OFF':'#78909c','ABORTED':'#b71c1c','UNKNOWN':'#555555'};
        if (oeeEl&&Object.keys(pcts).length>0&&!isStale) {
            const runningPct=pcts['RUNNING']||0;
            oeeEl.textContent=runningPct.toFixed(1)+'%';
            oeeEl.style.color=runningPct>=50?'#66bb6a':runningPct>=25?'#ffa726':'#ef5350';
            let legendHTML='';
            for (const [state,pct] of Object.entries(pcts)) {
                const color=stateColors[state]||'#999';
                legendHTML+=`<div><span style="display:inline-block;width:8px;height:8px;background:${color};margin-right:4px;border-radius:2px;"></span>${state}: ${pct}%</div>`;
            }
            if (legendEl) legendEl.innerHTML=legendHTML;
            if (pieCanvas) {
                const ctx=pieCanvas.getContext('2d'); ctx.clearRect(0,0,110,110);
                const cx=55,cy=55,r=50; let startAngle=-Math.PI/2;
                for (const [state,pct] of Object.entries(pcts)) {
                    if (pct<=0) continue;
                    const sliceAngle=(pct/100)*(2*Math.PI);
                    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.arc(cx,cy,r,startAngle,startAngle+sliceAngle);
                    ctx.closePath(); ctx.fillStyle=stateColors[state]||'#999'; ctx.fill();
                    ctx.strokeStyle='#0d1117'; ctx.lineWidth=1.5; ctx.stroke();
                    startAngle+=sliceAngle;
                }
                ctx.beginPath(); ctx.arc(cx,cy,r*0.52,0,Math.PI*2); ctx.fillStyle='#161b22'; ctx.fill();
            }
        } else if (oeeEl) {
            oeeEl.textContent='—';
            if (legendEl) legendEl.textContent=isStale?'Data stale':'Waiting...';
        }
        const stateEl=document.getElementById('state-value');
        if (stateEl) {
            stateEl.textContent=isStale?'UNKNOWN (DISCONNECTED)':(data.state_name||'—');
            const stateDiv=document.getElementById('state-div');
            if (stateDiv) stateDiv.className='value '+(isStale?'stopped':st===16?'running':st===256||st===512?'fault blink':st===64||st===128?'paused':'stopped');
        }
        const wbDiv=document.getElementById('wb-div');
        if (wbDiv) wbDiv.className='value '+(wb!==null&&wb!==3&&!isStale?'fault blink':'ok');
        const faultDiv=document.getElementById('fault-div'), faultUnit=document.getElementById('fault-unit');
        if (faultDiv) { const hf=!data.no_faults; faultDiv.className='value '+((hf&&!isStale)?'fault blink':'ok'); faultDiv.textContent=isStale?'UNKNOWN':(hf?'FAULT':'NONE'); }
        if (faultUnit&&data.machine_faults&&data.machine_faults!==4&&!isStale) faultUnit.textContent='code: '+data.machine_faults;
        else if (faultUnit) faultUnit.textContent='';
        lastState=st; lastWBBits=wb;
    } catch(e) {
        const cs=document.getElementById('conn-status');
        if (cs) { cs.textContent='DISCONNECTED'; cs.className='fault'; }
    }
}

window.addEventListener('resize', drawChart);
setInterval(fetchAndUpdate, 2000);
fetchAndUpdate();
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
        normal_wire_bits=NORMAL_WIRE_BITS,
    )


@app.route('/api/latest')
def api_latest():
    with _lock:
        return jsonify(_latest)


@app.route('/favicon.ico')
def favicon():
    return '', 204


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
        seen = set(); deduped = []
        for row in rows:
            t = row.get('Timestamp','')
            if t not in seen: seen.add(t); deduped.append(row)
        return deduped

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
<meta http-equiv="refresh" content="60">
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
<div class="subtitle">{{ d.date }} &nbsp;·&nbsp; Refreshes every 60s &nbsp;·&nbsp; {{ d.total_hrs }}h logged today</div>

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
  <div id="todayChart" style="width:100%;height:220px;"></div>
</div>

<div class="section">
  <div class="section-title">This Week — Machine State Timeline</div>
  <div id="weekChart" style="width:100%;height:220px;"></div>
</div>

<div class="section">
  <div class="section-title">Last Week — Machine State Timeline</div>
  <div id="lastWeekChart" style="width:100%;height:220px;"></div>
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
    const layout = {
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
    Plotly.newPlot(divId, plotTraces, layout, {responsive:true, displayModeBar:false});
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

(async function() {
    try {
        const [todayRes, weekRes, lastWeekRes] = await Promise.all([
            fetch('/api/floor_data?range=today'),
            fetch('/api/floor_data?range=week'),
            fetch('/api/floor_data?range=lastweek'),
        ]);
        const todayData    = await todayRes.json();
        const weekData     = await weekRes.json();
        const lastWeekData = await lastWeekRes.json();
        drawPie('todayPie',    'todayPieLegend',    todayData);
        drawPie('weekPie',     'weekPieLegend',     weekData);
        drawPie('lastWeekPie', 'lastWeekPieLegend', lastWeekData);
        buildStateChart('todayChart',    todayData);
        buildStateChart('weekChart',     weekData);
        buildStateChart('lastWeekChart', lastWeekData);
    } catch(e) {
        ['todayChart','weekChart','lastWeekChart'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerHTML = '<p style="color:#8b949e;padding:20px">Chart unavailable.</p>';
        });
    }
})();
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
        last_sunday = this_monday - timedelta(days=1)
        cutoff_start = last_monday.isoformat()
        cutoff_end   = last_sunday.isoformat() + 'T23:59:59'
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

    step = 10 if range_param == 'week' else 1
    matching = deduped[::step]
    timestamps_, table_speed_, puller_speed_, speed_ratio_, states_ = [], [], [], [], []
    for row in matching:
        timestamps_.append(row.get('Timestamp',''))
        try: table_speed_.append(float(row.get('Table_Speed',0) or 0))
        except: table_speed_.append(0)
        try: puller_speed_.append(float(row.get('Puller_Speed',0) or 0))
        except: puller_speed_.append(0)
        try: speed_ratio_.append(float(row.get('Speed_Ratio') or 0) or None)
        except: speed_ratio_.append(None)
        states_.append(row.get('State_Name','') or '')
    return jsonify({'timestamps':timestamps_,'table_speed':table_speed_,
                    'puller_speed':puller_speed_,'speed_ratio':speed_ratio_,'states':states_})


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

    # PLC monitor loop
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True, name='monitor')
    monitor_thread.start()

    log.info('Dashboard: http://0.0.0.0:5000  |  Floor: http://0.0.0.0:5000/floor')
    import logging as _logging
    _logging.getLogger('werkzeug').setLevel(_logging.ERROR)
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)