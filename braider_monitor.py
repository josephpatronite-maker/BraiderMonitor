"""
braider_monitor.py
Noble Gas Systems — Steeger HS120/48 IMC-7K Braider Monitor
Raspberry Pi production logger — runs as a systemd service

Logs:
  - process_log.csv      : 2s poll, speeds + position + state + safety
  - event_log.csv        : on state change or wire break
  - wire_break_log.csv   : high-res 10s window around every wire break
  - oee_log.csv          : 60s poll, cumulative state time for OEE

Flask dashboard available at http://<pi-ip>:5000

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
BRAIDER_ID = 'Braider_2'   # Change to 'Braider_1' on the other machine

LOG_DIR = os.path.expanduser('~/braider_logs')

PROCESS_LOG    = os.path.join(LOG_DIR, f'{BRAIDER_ID}_process_log.csv')
EVENT_LOG      = os.path.join(LOG_DIR, f'{BRAIDER_ID}_event_log.csv')
WIRE_BREAK_LOG = os.path.join(LOG_DIR, f'{BRAIDER_ID}_wire_break_log.csv')
OEE_LOG        = os.path.join(LOG_DIR, f'{BRAIDER_ID}_oee_log.csv')

FAST_POLL_INTERVAL       = 2   # seconds
OEE_POLL_INTERVAL        = 60  # seconds
PRE_BREAK_BUFFER_SECONDS = 5
POST_BREAK_CAPTURE_SECONDS = 5

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

# ── Tag lists ─────────────────────────────────────────────────────────────────

# 2s poll — process data + safety flags
FAST_TAGS = [
    # Core process
    'Machine_State',
    'Table_Actual_Speed',
    'Puller_Actual_Speed',
    'Puller_Pos_Feet',
    'Table_Position',
    'Active_Segment',
    'Current_Segment',
    'realTableSpeed',
    # Speeds — additional context
    'Transition_Active',        # True during segment-to-segment speed transitions
    # Faults and wire breaks
    'No_Machine_Faults',
    'No_Machine_Msgs',
    'Machine_Faults',           # Fault bitmask — specific fault codes
    'Local:1:I.Data',
    'Local:1:I.Fault',
    'WIre_Break_Detected',      # Cleaner wire break flag (note: typo in PLC tag name is intentional)
    'Core_Break',               # Core/mandrel break detected
    'Cam_Error',                # Cam profile calculation error
    'Calc_Error',               # General calculation error
    'Start_Warning.DN',          # Warning condition at run start — DN bit of timer
    # Safety inputs — flip before state change, useful fault context
    'I_Door_Interlock_Ok',
    'I_Emergency_Stop_Ok',
    'I_Table_Motor_OL',         # Table motor overload relay — mechanical overload precursor
    'I_CoreBreak_Sensor',       # Core break sensor input
    'I_Triaxial_WB',            # Triaxial wire break input
    'Machine.Estops_Ok',
    'Machine.Guards_Ok',
    'Machine.All_Safties_Ok',
    'Machine.All_Axes_Ok',
    'Machine.All_Axes_Running',
    # Servo sync flags — pre-fault signal candidates
    'AxisSynced_OS1',
    'AxisSynced_OS2',
    'AxisSynced_OS3',
    'AxisSynced_OS4',
    'AxisSynced_OS5',
    # Servo/motion health
    'Puller_Position_Error',    # Puller following error — mechanical load indicator
    'Table_Drive:I.AtReference', # True when VFD reaches commanded speed
    # Current state elapsed time — real-time OEE
    'Current_Hours.ACC',
    'Current_Minutes.ACC',
    'Current_Seconds.ACC',
    # Job definition
    'Length_To_Run',            # Remaining length to complete job — production progress
    'Run_Complete',             # True when a job run completes — production event trigger
    # Taper sensor (Keyence IX-H2000)
    'Taper_Sensor_Input',       # Raw sensor input — braid diameter measurement
    'Sensor_Mode_Enable',       # Taper sensor mode active
    'New_Part_ONS',             # New vessel start one-shot pulse
    'New_Part_Latch',           # New vessel detection latched
    'PPI_Change_ONS',           # PPI changed mid-run
    # Inactivity
    'Inactivity_Timer.ACC',     # Time machine has been idle — alert trigger
    # VFD feedback — actual vs commanded frequency (load indicator)
    'Table_Drive:I.OutputFreq',
    'Table_Drive:O.FreqCommand',
    'Table_Drive:I.Faulted',
    'Table_Drive:I.Active',
    # Wire break recovery
    'WireBreak_Move',           # Distance machine backed up after wire break
    'EStop_Recover',            # E-stop recovery sequence active
]

# 60s poll — OEE accumulators + recipe
OEE_TAGS = [
    'Machine_Statistics',
    'CurrentRecipe',
    'HMI_NumberCarriers',
    'HMI_Recipe_Number',
    'Recipe_Modified',
    'HMI_Mandrel_Mode',
    'PowerOn_Days.ACC',
    'PowerOn_Hours.ACC',
    'Triaxial_Enable',          # Whether triaxial detection is active this recipe
    'ABORTED_Hours.ACC',        # Time spent in fault state — downtime analysis
    'ABORTING_Hours.ACC',       # Time spent in fault recovery
    'TOTAL_RUNNING_Hours.ACC',  # Alternate running hours counter
    # Job definition — slow changing, set at recipe load
    'Discrete_Distance',        # Target vessel length in feet
    'Discrete_Loops',           # Number of braid passes
    'Loop_Length_Feet',         # Length per loop
    'Carrier_Mode',             # Carrier configuration mode
    'Current_Ratio',            # Current gear ratio
]

# Tags to watch for fault events — logged to event_log on change
FAULT_TAGS = [
    'Fault_9',
    'Fault_13',
    'Fault_14',
    'Fault_16',
    'Fault_Cam',
    'Fault_Calc',
]

# ── Setup ─────────────────────────────────────────────────────────────────────

from logging.handlers import RotatingFileHandler

LOG_FILE = os.path.join(os.path.expanduser('~'), 'braider_monitor.log')

# Caps log at 5MB, keeps 3 old files (braider_monitor.log.1, .2, .3)
# Total max log footprint: 20MB regardless of how long the Pi runs
_rotating_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5MB per file
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


def archive_logs():
    """
    Archive log files on a schedule:
    - process_log: weekly on Sunday midnight (gets large fast at 2s polling)
    - event_log, oee_log, wire_break_log: monthly on 1st of month midnight
    """
    from datetime import datetime
    now = datetime.now()

    is_sunday_midnight   = (now.weekday() == 6 and now.hour == 0 and now.minute == 0)
    is_monthstart_midnight = (now.day == 1 and now.hour == 0 and now.minute == 0)

    archived_any = False

    # Weekly — process_log only
    if is_sunday_midnight:
        week_label = now.strftime('%Y_%m_%d')
        if os.path.exists(PROCESS_LOG) and os.path.getsize(PROCESS_LOG) > 0:
            archive_name = PROCESS_LOG.replace('.csv', f'_week_ending_{week_label}.csv')
            os.rename(PROCESS_LOG, archive_name)
            log.info(f'Weekly archive: {os.path.basename(PROCESS_LOG)} -> {os.path.basename(archive_name)}')
            archived_any = True

    # Monthly — event, oee, wire_break logs
    if is_monthstart_midnight:
        month_label = now.strftime('%Y_%m')
        for filepath in [EVENT_LOG, OEE_LOG, WIRE_BREAK_LOG]:
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                archive_name = filepath.replace('.csv', f'_{month_label}.csv')
                os.rename(filepath, archive_name)
                log.info(f'Monthly archive: {os.path.basename(filepath)} -> {os.path.basename(archive_name)}')
                archived_any = True

    if archived_any:
        log.info('Archive complete')


def write_csv_row(filepath, row: dict):
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0

    if file_exists:
        # Check if the columns match — if not, archive the old file and start fresh
        with open(filepath, 'r', newline='') as f:
            existing_headers = f.readline().strip().split(',')
        new_headers = list(row.keys())
        if existing_headers != new_headers:
            # Archive old file with timestamp so data is never lost
            from datetime import datetime
            archive_name = filepath.replace('.csv', f'_archived_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
            os.rename(filepath, archive_name)
            log.warning(f'Column mismatch detected — archived old file to {os.path.basename(archive_name)}')
            file_exists = False

    with open(filepath, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ── Shared state ──────────────────────────────────────────────────────────────

_lock = threading.Lock()
_latest = {
    'timestamp':         None,
    'machine_state':     None,
    'state_name':        None,
    'table_speed':       None,
    'puller_speed':      None,
    'puller_pos_feet':   None,
    'table_position':    None,
    'active_segment':    None,
    'no_faults':         None,
    'wire_break_bits':   None,
    'recipe_name':       None,
    'recipe_ppi':        None,
    # Safety
    'door_ok':           None,
    'estop_ok':          None,
    'guards_ok':         None,
    'all_axes_running':  None,
    # Servo sync
    'axis1_synced':      None,
    'axis2_synced':      None,
    'axis3_synced':      None,
    'axis4_synced':      None,
    'axis5_synced':      None,
    # Current state time
    'current_state_hrs':  None,
    'current_state_mins': None,
    'current_state_secs': None,
    # Job
    'discrete_distance': None,
    'discrete_loops':    None,
    # OEE
    'cum_running_hrs':   None,
    'cum_stopped_hrs':   None,
    'cum_ready_hrs':     None,
    'recipe_modified':   None,
    'mandrel_mode':      None,
    'connected':         False,
    'last_error':        None,
}

_rolling_buffer = deque(maxlen=int(PRE_BREAK_BUFFER_SECONDS / FAST_POLL_INTERVAL) + 5)
_wire_break_capturing = False
_wire_break_capture_until = 0
_wire_break_capture_rows = []


# ── Monitor loop ──────────────────────────────────────────────────────────────

def monitor_loop():
    global _wire_break_capturing, _wire_break_capture_until, _wire_break_capture_rows

    prev_state          = None
    prev_wire_bits      = None
    last_oee_poll       = 0
    recipe_name         = 'Unknown'
    running_started_at  = None   # timestamp when machine last entered RUNNING state
    recipe_ppi     = None

    log.info(f'Starting monitor loop -> PLC {PLC_IP}')

    while True:
        try:
            with LogixDriver(PLC_IP) as plc:
                if not plc.connected:
                    raise ConnectionError('LogixDriver connected=False')

                log.info('Connected to PLC')
                monitor_loop._retry_count = 0  # Reset backoff on successful connect
                with _lock:
                    _latest['connected'] = True
                    _latest['last_error'] = None

                while True:
                    now       = time.time()
                    timestamp = ts()

                    # ── Fast poll ────────────────────────────────────────────
                    results = plc.read(*FAST_TAGS)
                    d = {r.tag: r.value for r in results if r.error is None}

                    machine_state = d.get('Machine_State')
                    table_speed   = d.get('Table_Actual_Speed')
                    puller_speed  = d.get('Puller_Actual_Speed')
                    puller_feet   = d.get('Puller_Pos_Feet')
                    table_pos     = d.get('Table_Position')
                    active_seg    = d.get('Active_Segment')
                    no_faults     = d.get('No_Machine_Faults')
                    wire_bits     = d.get('Local:1:I.Data')

                    # Derived speed ratio (PPI proxy)
                    speed_ratio = None
                    if table_speed and puller_speed and table_speed > 0:
                        speed_ratio = round(puller_speed / table_speed, 6)

                    # Current state elapsed time in seconds total
                    state_elapsed_s = None
                    ch = d.get('Current_Hours.ACC')
                    cm = d.get('Current_Minutes.ACC')
                    cs = d.get('Current_Seconds.ACC')
                    if ch is not None and cm is not None and cs is not None:
                        state_elapsed_s = (ch * 3600) + (cm * 60) + round(cs / 1000, 1)

                    # ── OEE poll ─────────────────────────────────────────────
                    cum_running = cum_stopped = cum_ready = None
                    recipe_modified = mandrel_mode = None

                    if now - last_oee_poll >= OEE_POLL_INTERVAL:
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
                            recipe_name     = recipe_raw.get('Name', 'Unknown')
                            recipe_ppi      = recipe_raw.get('Body_PPI')
                            recipe_modified = od.get('Recipe_Modified')
                            mandrel_mode    = od.get('HMI_Mandrel_Mode')

                        oee_row = {
                            'Timestamp':          timestamp,
                            'Braider_ID':         BRAIDER_ID,
                            'Machine_State':      machine_state,
                            'Discrete_Distance':  od.get('Discrete_Distance'),
                            'Discrete_Loops':     od.get('Discrete_Loops'),
                            'Loop_Length_Feet':   od.get('Loop_Length_Feet'),
                            'Carrier_Mode':       od.get('Carrier_Mode'),
                            'Current_Ratio':      od.get('Current_Ratio'),
                            'State_Name':         state_name(machine_state) if machine_state else '',
                            'Recipe_Name':        recipe_name,
                            'Recipe_Number':      od.get('HMI_Recipe_Number'),
                            'Recipe_PPI':         recipe_ppi,
                            'Recipe_Modified':    recipe_modified,
                            'Mandrel_Mode':       mandrel_mode,
                            'Carriers':           od.get('HMI_NumberCarriers'),
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
                        'Timestamp':          timestamp,
                        'Braider_ID':         BRAIDER_ID,
                        'Machine_State':      machine_state,
                        'State_Name':         state_name(machine_state) if machine_state else '',
                        'Table_Speed':        round(table_speed,  6) if table_speed  else None,
                        'Puller_Speed':       round(puller_speed, 6) if puller_speed else None,
                        'Speed_Ratio':        speed_ratio,
                        'Puller_Pos_Feet':    round(puller_feet,  4) if puller_feet  else None,
                        'Table_Position':     round(table_pos,    4) if table_pos    else None,
                        'Active_Segment':     active_seg,
                        'Current_Segment':    d.get('Current_Segment'),
                        'State_Elapsed_Secs': state_elapsed_s,
                        'No_Faults':          no_faults,
                        'No_Msgs':            d.get('No_Machine_Msgs'),
                        'Wire_Break_Bits':    wire_bits,
                        'Wire_Input_Fault':   d.get('Local:1:I.Fault'),
                        # Safety
                        'Door_Ok':            d.get('I_Door_Interlock_Ok'),
                        'Estop_Ok':           d.get('I_Emergency_Stop_Ok'),
                        'Guards_Ok':          d.get('Machine.Guards_Ok'),
                        'All_Safties_Ok':     d.get('Machine.All_Safties_Ok'),
                        'All_Axes_Ok':        d.get('Machine.All_Axes_Ok'),
                        'All_Axes_Running':   d.get('Machine.All_Axes_Running'),
                        # Servo sync — pre-fault signal candidates
                        'AxisSynced_1':       d.get('AxisSynced_OS1'),
                        'AxisSynced_2':       d.get('AxisSynced_OS2'),
                        'AxisSynced_3':       d.get('AxisSynced_OS3'),
                        'AxisSynced_4':       d.get('AxisSynced_OS4'),
                        'AxisSynced_5':       d.get('AxisSynced_OS5'),
                        # Job context
                        'Recipe_Name':        recipe_name,
                        'Recipe_PPI':         recipe_ppi,
                        # VFD load indicators
                        'VFD_Freq_Actual':    d.get('Table_Drive:I.OutputFreq'),
                        'VFD_Freq_Command':   d.get('Table_Drive:O.FreqCommand'),
                        'VFD_Freq_Delta':     (d.get('Table_Drive:O.FreqCommand', 0) or 0) -
                                              (d.get('Table_Drive:I.OutputFreq', 0) or 0),
                        'VFD_Faulted':        d.get('Table_Drive:I.Faulted'),
                        'VFD_Active':         d.get('Table_Drive:I.Active'),
                        'VFD_AtReference':    d.get('Table_Drive:I.AtReference'),
                        # Speeds — additional
                        'Transition_Active':  d.get('Transition_Active'),
                        # Faults
                        'Machine_Faults':     d.get('Machine_Faults'),
                        'Wire_Break_Detected':d.get('WIre_Break_Detected'),
                        'Core_Break':         d.get('Core_Break'),
                        'Cam_Error':          d.get('Cam_Error'),
                        'Calc_Error':         d.get('Calc_Error'),
                        'Start_Warning':      d.get('Start_Warning.DN'),
                        'I_Table_Motor_OL':   d.get('I_Table_Motor_OL'),
                        'I_CoreBreak_Sensor': d.get('I_CoreBreak_Sensor'),
                        'I_Triaxial_WB':      d.get('I_Triaxial_WB'),
                        'Puller_Pos_Error':   d.get('Puller_Position_Error'),
                        # Job progress
                        'Length_To_Run':      d.get('Length_To_Run'),
                        'Run_Complete':       d.get('Run_Complete'),
                        # Taper sensor
                        'Taper_Sensor':       d.get('Taper_Sensor_Input'),
                        'Sensor_Mode':        d.get('Sensor_Mode_Enable'),
                        'New_Part':           d.get('New_Part_Latch'),
                        'PPI_Change':         d.get('PPI_Change_ONS'),
                        # Inactivity
                        'Inactivity_Secs':    d.get('Inactivity_Timer.ACC'),
                        # Wire break recovery
                        'WireBreak_Move':     d.get('WireBreak_Move'),
                        'EStop_Recover':      d.get('EStop_Recover'),
                    }
                    write_csv_row(PROCESS_LOG, process_row)
                    _rolling_buffer.append(process_row.copy())

                    # ── Wire break post-capture ──────────────────────────────
                    if _wire_break_capturing:
                        _wire_break_capture_rows.append(process_row.copy())
                        if now >= _wire_break_capture_until:
                            for r in _wire_break_capture_rows:
                                write_csv_row(WIRE_BREAK_LOG, r)
                            log.info(f'Wire break capture complete — {len(_wire_break_capture_rows)} rows saved')
                            _wire_break_capturing = False
                            _wire_break_capture_rows = []

                    # ── State change event ───────────────────────────────────
                    if machine_state != prev_state and prev_state is not None:
                        event_row = {
                            'Timestamp':   timestamp,
                            'Braider_ID':  BRAIDER_ID,
                            'Event':       'STATE_CHANGE',
                            'From_State':  state_name(prev_state),
                            'To_State':    state_name(machine_state) if machine_state else '',
                            'From_Code':   prev_state,
                            'To_Code':     machine_state,
                            'Puller_Feet': round(puller_feet, 4) if puller_feet else None,
                            'Recipe_Name': recipe_name,
                            'Estop_Ok':    d.get('I_Emergency_Stop_Ok'),
                            'Door_Ok':     d.get('I_Door_Interlock_Ok'),
                            'Detail':      '',
                        }
                        write_csv_row(EVENT_LOG, event_row)
                        log.info(f'State change: {state_name(prev_state)} -> {state_name(machine_state)}')
                    # Track when machine enters RUNNING state
                    if machine_state == 16 and prev_state != 16:
                        running_started_at = now
                    prev_state = machine_state

                    # ── Wire break detection (RUNNING only) ──────────────────
                    # Only fire during active production (state 16)
                    # Ignore first 5 seconds after startup — operator threading
                    # and manual tensioning triggers identical bit transitions
                    STARTUP_GRACE_SECONDS = 5
                    in_startup = (
                        running_started_at is not None and
                        (now - running_started_at) < STARTUP_GRACE_SECONDS
                    )

                    if wire_bits is not None and prev_wire_bits is not None:
                        if wire_bits != prev_wire_bits:
                            changed    = wire_bits ^ prev_wire_bits
                            new_breaks = wire_bits & changed
                            cleared    = prev_wire_bits & changed

                            if machine_state == 16 and not in_startup:
                                if new_breaks:
                                    log.warning(f'WIRE BREAK — bits:{bin(wire_bits)} at {puller_feet:.2f} ft')
                                    event_row = {
                                        'Timestamp':    timestamp,
                                        'Braider_ID':   BRAIDER_ID,
                                        'Event':        'WIRE_BREAK',
                                        'From_State':   state_name(prev_state) if prev_state else '',
                                        'To_State':     state_name(machine_state) if machine_state else '',
                                        'From_Code':    prev_wire_bits,
                                        'To_Code':      wire_bits,
                                        'Puller_Feet':  round(puller_feet, 4) if puller_feet else None,
                                        'Recipe_Name':  recipe_name,
                                        'Estop_Ok':     d.get('I_Emergency_Stop_Ok'),
                                        'Door_Ok':      d.get('I_Door_Interlock_Ok'),
                                        'Detail':       f'bits_changed={bin(new_breaks)}',
                                    }
                                    write_csv_row(EVENT_LOG, event_row)
                                    _wire_break_capture_rows = list(_rolling_buffer)
                                    _wire_break_capturing = True
                                    _wire_break_capture_until = now + POST_BREAK_CAPTURE_SECONDS

                                if cleared:
                                    event_row = {
                                        'Timestamp':   timestamp,
                                        'Braider_ID':  BRAIDER_ID,
                                        'Event':       'WIRE_BREAK_CLEARED',
                                        'From_State':  state_name(prev_state) if prev_state else '',
                                        'To_State':    state_name(machine_state) if machine_state else '',
                                        'From_Code':   prev_wire_bits,
                                        'To_Code':     wire_bits,
                                        'Puller_Feet': round(puller_feet, 4) if puller_feet else None,
                                        'Recipe_Name': recipe_name,
                                        'Estop_Ok':    d.get('I_Emergency_Stop_Ok'),
                                        'Door_Ok':     d.get('I_Door_Interlock_Ok'),
                                        'Detail':      f'bits_cleared={bin(cleared)}',
                                    }
                                    write_csv_row(EVENT_LOG, event_row)
                            elif machine_state == 16 and in_startup:
                                log.debug(f'Wire bits changed during startup grace period — ignored: ' 
                                          f'{prev_wire_bits} -> {wire_bits} ({now - running_started_at:.1f}s after start)')
                            else:
                                log.debug(f'Wire bits changed during {state_name(machine_state)} '
                                          f'(bobbin change?) — ignored: {prev_wire_bits} -> {wire_bits}')

                    prev_wire_bits = wire_bits

                    # ── Fault tag change detection ───────────────────────────
                    # Log to event_log when any specific fault tag becomes True
                    for ft in FAULT_TAGS:
                        val = d.get(ft)
                        if val:
                            event_row = {
                                'Timestamp':   timestamp,
                                'Braider_ID':  BRAIDER_ID,
                                'Event':       f'FAULT_{ft}',
                                'From_State':  state_name(machine_state) if machine_state else '',
                                'To_State':    '',
                                'From_Code':   machine_state,
                                'To_Code':     machine_state,
                                'Puller_Feet': round(puller_feet, 4) if puller_feet else None,
                                'Recipe_Name': recipe_name,
                                'Estop_Ok':    d.get('I_Emergency_Stop_Ok'),
                                'Door_Ok':     d.get('I_Door_Interlock_Ok'),
                                'Detail':      f'{ft}={val}',
                            }
                            write_csv_row(EVENT_LOG, event_row)
                            log.warning(f'FAULT TAG: {ft} = {val}')

                    # ── Update dashboard state ───────────────────────────────
                    with _lock:
                        _latest.update({
                            'timestamp':          timestamp,
                            'machine_state':      machine_state,
                            'state_name':         state_name(machine_state) if machine_state else 'Unknown',
                            'table_speed':        round(table_speed,  4) if table_speed  else None,
                            'puller_speed':       round(puller_speed, 4) if puller_speed else None,
                            'speed_ratio':        speed_ratio,
                            'puller_pos_feet':    round(puller_feet,  2) if puller_feet  else None,
                            'table_position':     round(table_pos,    2) if table_pos    else None,
                            'active_segment':     active_seg,
                            'no_faults':          no_faults,
                            'wire_break_bits':    wire_bits,
                            'recipe_name':        recipe_name,
                            'recipe_ppi':         recipe_ppi,
                            'door_ok':            d.get('I_Door_Interlock_Ok'),
                            'estop_ok':           d.get('I_Emergency_Stop_Ok'),
                            'guards_ok':          d.get('Machine.Guards_Ok'),
                            'all_axes_running':   d.get('Machine.All_Axes_Running'),
                            'axis1_synced':       d.get('AxisSynced_OS1'),
                            'axis2_synced':       d.get('AxisSynced_OS2'),
                            'axis3_synced':       d.get('AxisSynced_OS3'),
                            'axis4_synced':       d.get('AxisSynced_OS4'),
                            'axis5_synced':       d.get('AxisSynced_OS5'),
                            'current_state_hrs':  ch,
                            'current_state_mins': cm,
                            'current_state_secs': round(cs / 1000, 0) if cs else None,
                            'state_elapsed_s':    state_elapsed_s,
                            'discrete_distance':  d.get('Discrete_Distance'),
                            'discrete_loops':     d.get('Discrete_Loops'),
                            'cum_running_hrs':    cum_running,
                            'cum_stopped_hrs':    cum_stopped,
                            'cum_ready_hrs':      cum_ready,
                            'recipe_modified':    recipe_modified,
                            'mandrel_mode':       mandrel_mode,
                            'vfd_freq_actual':    d.get('Table_Drive:I.OutputFreq'),
                            'vfd_freq_command':   d.get('Table_Drive:O.FreqCommand'),
                            'vfd_freq_delta':     (d.get('Table_Drive:O.FreqCommand', 0) or 0) -
                                                  (d.get('Table_Drive:I.OutputFreq', 0) or 0),
                            'vfd_faulted':        d.get('Table_Drive:I.Faulted'),
                            'vfd_at_ref':         d.get('Table_Drive:I.AtReference'),
                            'horn_gear_rpm':      d.get('Horn_Gear_RPM'),
                            'active_seg_speed':   d.get('Active_Seg_Speed'),
                            'transition_active':  d.get('Transition_Active'),
                            'machine_faults':     d.get('Machine_Faults'),
                            'wire_break_detected':d.get('WIre_Break_Detected'),
                            'core_break':         d.get('Core_Break'),
                            'i_table_motor_ol':   d.get('I_Table_Motor_OL'),
                            'i_triaxial_wb':       d.get('I_Triaxial_WB'),
                            'loop_count':         d.get('Loop_Count'),
                            'length_to_run':      d.get('Length_To_Run'),
                            'run_complete':       d.get('Run_Complete'),
                            'taper_sensor':       d.get('Taper_Sensor_Input'),
                            'tube_dia_mm':        d.get('Tube_Dia_mm'),
                            'ppi_pos':            d.get('PPI_Pos'),
                            'new_part':           d.get('New_Part_Latch'),
                            'inactivity_secs':    d.get('Inactivity_Timer.ACC'),
                            'estop_recover':      d.get('EStop_Recover'),
                            'connected':          True,
                        })

                    # Check for scheduled archives
                    archive_logs()

                    time.sleep(FAST_POLL_INTERVAL)

        except KeyboardInterrupt:
            log.info('Shutdown requested.')
            break
        except Exception as e:
            with _lock:
                _latest['connected'] = False
                _latest['last_error'] = str(e)

            # Back off retry interval — short at first, longer after repeated failures
            if not hasattr(monitor_loop, '_retry_count'):
                monitor_loop._retry_count = 0
            monitor_loop._retry_count += 1

            if monitor_loop._retry_count <= 3:
                # First 3 failures — retry every 10s (fast reconnect after brief outage)
                wait = 10
                log.error(f'Connection lost: {e}')
                log.info(f'Retrying in {wait}s (attempt {monitor_loop._retry_count})...')
            elif monitor_loop._retry_count <= 10:
                # Next 7 failures — retry every 60s
                wait = 60
                if monitor_loop._retry_count == 4:
                    log.warning('PLC unreachable — switching to 60s retry interval')
            else:
                # After 10 failures — retry every 5 minutes, log once per hour
                wait = 300
                if monitor_loop._retry_count % 12 == 0:
                    log.warning(f'PLC still unreachable after {monitor_loop._retry_count} attempts — retrying every 5min')

            time.sleep(wait)


# ── Flask dashboard ───────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Braider Monitor — Noble Gas Systems</title>
    <!-- Live updates via JS fetch — no page reload needed -->
    <style>
        body  { font-family: monospace; background:#1a1a1a; color:#e0e0e0; padding:20px; margin:0; }
        h1    { color:#4fc3f7; margin-bottom:4px; }
        .sub  { color:#888; font-size:13px; margin-bottom:20px; }
        .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; }
        .card { background:#2a2a2a; border-radius:8px; padding:14px; }
        .label{ font-size:10px; color:#888; text-transform:uppercase; letter-spacing:1px; }
        .value{ font-size:26px; font-weight:bold; margin-top:4px; line-height:1.1; }
        .unit { font-size:12px; color:#888; margin-top:2px; }
        .section { font-size:11px; color:#555; text-transform:uppercase;
                   letter-spacing:2px; margin:20px 0 8px; }
        .running { color:#66bb6a; }
        .stopped { color:#ef5350; }
        .paused  { color:#ffa726; }
        .fault   { color:#ef5350; }
        .ok      { color:#66bb6a; }
        .warn    { color:#ffa726; }
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
        Updated: {{ d.timestamp }}
    </div>

    <!-- ── MACHINE ─────────────────────────────────────────── -->
    <div class="section">Machine</div>
    <div class="grid">

        <div class="card">
            <div class="label">State</div>
            <div class="value {% if d.machine_state == 16 %}running
                              {% elif d.machine_state in (256,512) %}fault blink
                              {% elif d.machine_state in (64,128) %}paused
                              {% else %}stopped{% endif %}">
                <span id="state-value">{{ d.state_name or '—' }}</span>
            </div>
            <div class="unit">
                code {{ d.machine_state }}
                {% if d.state_elapsed_s %}
                  &nbsp;|&nbsp;
                  {{ (d.state_elapsed_s // 3600)|int }}h
                  {{ ((d.state_elapsed_s % 3600) // 60)|int }}m
                {% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Recipe</div>
            <div class="value" style="font-size:22px">{{ d.recipe_name or '—' }}</div>
            <div class="unit">
                {{ d.recipe_ppi }} PPI
                {% if d.recipe_modified %}&nbsp;<span class="warn">modified</span>{% endif %}
                {% if d.mandrel_mode %}&nbsp;| mandrel{% endif %}
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

    </div>

    <!-- ── PROCESS ─────────────────────────────────────────── -->
    <div class="section">Process</div>
    <div class="grid">

        <div class="card">
            <div class="label">Table Speed</div>
            <div class="value" id="table-value">{{ d.table_speed or '—' }}</div>
            <div class="unit">rev/s</div>
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
            <div class="unit">raw units — units TBD</div>
        </div>

        <div class="card">
            <div class="label">VFD — Actual / Command</div>
            <div class="value" style="font-size:18px">
                {{ d.vfd_freq_actual or '—' }} / {{ d.vfd_freq_command or '—' }}
            </div>
            <div class="unit">
                Hz×10 &nbsp;|&nbsp; delta: {{ d.vfd_freq_delta or 0 }}
                {% if d.vfd_at_ref %}&nbsp;<span class="ok">AT REF</span>{% endif %}
                {% if d.vfd_faulted %}&nbsp;<span class="fault blink">VFD FAULT</span>{% endif %}
            </div>
        </div>

    </div>

    <!-- ── FAULTS & SAFETY ────────────────────────────────── -->
    <div class="section">Faults &amp; Safety</div>
    <div class="grid">

        <div class="card">
            <div class="label">Wire Break Inputs</div>
            <div class="value {% if d.wire_break_bits is not none and d.wire_break_bits != 3 %}fault blink{% else %}ok{% endif %}">
                <span id="wb-value">{{ d.wire_break_bits if d.wire_break_bits is not none else '—' }}</span>
            </div>
            <div class="unit">Local:1:I.Data &nbsp;|&nbsp; normal = 3</div>
        </div>

        <div class="card">
            <div class="label">Faults</div>
            <div class="value {% if d.no_faults %}ok{% else %}fault blink{% endif %}">
                {% if d.no_faults %}NONE{% else %}FAULT{% endif %}
            </div>
            <div class="unit">
                {% if d.machine_faults and d.machine_faults != 4 %}code: {{ d.machine_faults }}{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Safety</div>
            <div class="checks">
                <span class="{{ 'ok' if d.estop_ok else 'fault blink' }}">
                    {{ '✓' if d.estop_ok else '✗' }} E-Stop
                </span><br>
                <span class="{{ 'ok' if d.door_ok else 'warn' }}">
                    {{ '✓' if d.door_ok else '✗' }} Door
                </span><br>
                <span class="{{ 'ok' if d.guards_ok else 'fault' }}">
                    {{ '✓' if d.guards_ok else '✗' }} Guards
                </span><br>
                <span class="{{ 'fault blink' if d.i_table_motor_ol else 'ok' }}">
                    {{ '✗ MOTOR OL' if d.i_table_motor_ol else '✓ Motor OK' }}
                </span><br>
                <span class="{{ 'fault blink' if d.i_triaxial_wb else 'ok' }}">
                    {{ '✗ TRIAXIAL WB' if d.i_triaxial_wb else '✓ Triaxial OK' }}
                </span><br>
                <span class="{{ 'fault blink' if d.core_break else 'ok' }}">
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

    </div>

    <!-- ── LIVE CHART ──────────────────────────────────────── -->
    <div class="section">Live — Last 60 Seconds</div>
    <div style="background:#2a2a2a; border-radius:8px; padding:14px; margin-bottom:12px;">
        <canvas id="liveChart" height="120"></canvas>
    </div>

    <div class="conn" id="conn-bar">
        PLC: <span id="conn-status" class="ok">CONNECTED</span>
        &nbsp;|&nbsp; Logs: {{ log_dir }}
        &nbsp;|&nbsp; Braider_2
        &nbsp;|&nbsp; <span id="sound-toggle" onclick="toggleSound()" style="cursor:pointer">🔔 Sound ON</span>
        &nbsp;|&nbsp; <span id="last-update" style="color:#555"></span>
    </div>

<script>
// Sound alerts — plays on wire break or state change
// State is stored in sessionStorage so it persists across the 3s auto-refresh
let soundEnabled = sessionStorage.getItem('soundEnabled') !== 'false';

function toggleSound() {
    soundEnabled = !soundEnabled;
    sessionStorage.setItem('soundEnabled', soundEnabled);
    document.getElementById('sound-toggle').textContent = soundEnabled ? '🔔 Sound ON' : '🔕 Sound OFF';
}

document.getElementById('sound-toggle').textContent = soundEnabled ? '🔔 Sound ON' : '🔕 Sound OFF';

function beep(freq, duration, volume) {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        gain.gain.value = volume || 0.3;
        osc.frequency.value = freq;
        osc.type = 'sine';
        osc.start();
        osc.stop(ctx.currentTime + duration / 1000);
    } catch(e) {}
}

function alertWireBreak() {
    // Three urgent beeps for wire break
    beep(880, 200, 0.4);
    setTimeout(() => beep(880, 200, 0.4), 300);
    setTimeout(() => beep(880, 200, 0.4), 600);
}

function alertStateChange() {
    // Single soft tone for state change
    beep(440, 300, 0.2);
}

// Check current values against last known values
const currentState = {{ d.machine_state or 0 }};
const currentWBBits = {{ d.wire_break_bits if d.wire_break_bits is not none else 3 }};

const lastState   = parseInt(sessionStorage.getItem('lastState') || currentState);
const lastWBBits  = parseInt(sessionStorage.getItem('lastWBBits') || currentWBBits);

if (soundEnabled) {
    if (currentWBBits !== lastWBBits && currentWBBits !== 3 && currentState === 16) {
        alertWireBreak();
    } else if (currentState !== lastState) {
        alertStateChange();
    }
}

sessionStorage.setItem('lastState',  currentState);
sessionStorage.setItem('lastWBBits', currentWBBits);
</script>
<script>
// ── Live chart — plain canvas, no external libraries ─────────────────────
const MAX_POINTS = 30;
const tableSpeed  = Array(MAX_POINTS).fill(null);
const pullerSpeed = Array(MAX_POINTS).fill(null);
const speedRatio  = Array(MAX_POINTS).fill(null);
const timestamps  = Array(MAX_POINTS).fill('');
const machineStates = Array(MAX_POINTS).fill(0);

const canvas = document.getElementById('liveChart');
const ctx = canvas.getContext('2d');

function drawChart() {
    const W = canvas.width  = canvas.offsetWidth;
    const H = canvas.height = 120;
    const PAD = { top: 10, right: 60, bottom: 30, left: 50 };
    const plotW = W - PAD.left - PAD.right;
    const plotH = H - PAD.top - PAD.bottom;

    ctx.clearRect(0, 0, W, H);

    // Background by machine state
    for (let i = 0; i < MAX_POINTS - 1; i++) {
        const x0 = PAD.left + (i / (MAX_POINTS-1)) * plotW;
        const x1 = PAD.left + ((i+1) / (MAX_POINTS-1)) * plotW;
        ctx.fillStyle = machineStates[i] === 16 ? 'rgba(102,187,106,0.12)' : 'rgba(144,164,174,0.08)';
        ctx.fillRect(x0, PAD.top, x1-x0, plotH);
    }

    // Grid lines
    ctx.strokeStyle = '#333';
    ctx.lineWidth = 0.5;
    for (let i = 0; i <= 4; i++) {
        const y = PAD.top + (i/4) * plotH;
        ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + plotW, y); ctx.stroke();
    }

    // Helper to get non-null values range
    function range(arr) {
        const vals = arr.filter(v => v !== null);
        if (!vals.length) return [0, 1];
        const mn = Math.min(...vals), mx = Math.max(...vals);
        const pad = (mx - mn) * 0.1 || 0.1;
        return [mn - pad, mx + pad];
    }

    function toY(v, mn, mx) {
        return PAD.top + plotH - ((v - mn) / (mx - mn)) * plotH;
    }

    function drawLine(data, color, mn, mx) {
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.lineJoin = 'round';
        ctx.beginPath();
        let started = false;
        for (let i = 0; i < MAX_POINTS; i++) {
            if (data[i] === null) { started = false; continue; }
            const x = PAD.left + (i / (MAX_POINTS-1)) * plotW;
            const y = toY(data[i], mn, mx);
            if (!started) { ctx.moveTo(x, y); started = true; }
            else ctx.lineTo(x, y);
        }
        ctx.stroke();
    }

    // Speed axis (left) — table + puller
    const [spMn, spMx] = range([...tableSpeed, ...pullerSpeed]);
    drawLine(tableSpeed,  '#4fc3f7', spMn, spMx);
    drawLine(pullerSpeed, '#81c784', spMn, spMx);

    // Ratio axis (right)
    const [rMn, rMx] = range(speedRatio);
    drawLine(speedRatio, '#ffb74d', rMn, rMx);

    // Y axis labels — left
    ctx.fillStyle = '#888'; ctx.font = '10px monospace'; ctx.textAlign = 'right';
    ctx.fillText(spMx.toFixed(2), PAD.left - 4, PAD.top + 10);
    ctx.fillText(spMn.toFixed(2), PAD.left - 4, PAD.top + plotH);

    // Y axis labels — right (ratio)
    ctx.fillStyle = '#ffb74d'; ctx.textAlign = 'left';
    ctx.fillText(rMx.toFixed(4), PAD.left + plotW + 4, PAD.top + 10);
    ctx.fillText(rMn.toFixed(4), PAD.left + plotW + 4, PAD.top + plotH);

    // X axis timestamps — show first and last
    ctx.fillStyle = '#555'; ctx.textAlign = 'center'; ctx.font = '10px monospace';
    if (timestamps[0])                ctx.fillText(timestamps[0],            PAD.left,         PAD.top + plotH + 18);
    if (timestamps[MAX_POINTS-1])     ctx.fillText(timestamps[MAX_POINTS-1], PAD.left + plotW, PAD.top + plotH + 18);

    // Legend
    ctx.textAlign = 'left'; ctx.font = '10px monospace';
    const legY = PAD.top + 12;
    ctx.fillStyle = '#4fc3f7'; ctx.fillRect(PAD.left + 4, legY - 6, 12, 2);
    ctx.fillText('Table', PAD.left + 20, legY);
    ctx.fillStyle = '#81c784'; ctx.fillRect(PAD.left + 70, legY - 6, 12, 2);
    ctx.fillText('Puller', PAD.left + 86, legY);
    ctx.fillStyle = '#ffb74d'; ctx.fillRect(PAD.left + 144, legY - 6, 12, 2);
    ctx.fillText('Ratio →', PAD.left + 160, legY);
}

// ── Fetch loop ────────────────────────────────────────────────────────────
let lastState  = null;
let lastWBBits = null;
let soundEnabled = sessionStorage.getItem('soundEnabled') !== 'false';

async function fetchAndUpdate() {
    try {
        const res  = await fetch('/api/latest');
        const data = await res.json();

        const now = new Date().toLocaleTimeString('en-US', { hour12: false });
        const ts  = data.table_speed  || 0;
        const ps  = data.puller_speed || 0;
        const sr  = data.speed_ratio  || null;
        const wb  = data.wire_break_bits;
        const st  = data.machine_state;

        // Shift rolling buffers
        tableSpeed.shift();   tableSpeed.push(ts);
        pullerSpeed.shift();  pullerSpeed.push(ps);
        speedRatio.shift();   speedRatio.push(sr);
        timestamps.shift();   timestamps.push(now);
        machineStates.shift();machineStates.push(st || 0);

        drawChart();

        // Update status bar
        document.getElementById('conn-status').textContent = data.connected ? 'CONNECTED' : 'DISCONNECTED — ' + (data.last_error || '');
        document.getElementById('conn-status').className   = data.connected ? 'ok' : 'fault';
        document.getElementById('last-update').textContent = 'updated ' + now;

        // Update cards
        const upd = (id, v) => { const e = document.getElementById(id); if(e) e.textContent = v; };
        upd('state-value',  data.state_name || '—');
        upd('feet-value',   data.puller_pos_feet  ? data.puller_pos_feet.toFixed(2)  : '—');
        upd('table-value',  ts ? ts.toFixed(4) : '—');
        upd('puller-value', ps ? ps.toFixed(4) : '—');
        upd('ratio-value',  sr ? sr.toFixed(5) : '—');
        upd('taper-value',  data.taper_sensor ? data.taper_sensor.toFixed(2) : '—');
        upd('wb-value',     wb !== null ? wb : '—');

        // Sound
        if (soundEnabled) {
            if (wb !== lastWBBits && wb !== 3 && st === 16) alertWireBreak();
            else if (st !== lastState && lastState !== null) alertStateChange();
        }
        lastState  = st;
        lastWBBits = wb;

    } catch(e) {
        document.getElementById('conn-status').textContent = 'DISCONNECTED';
        document.getElementById('conn-status').className   = 'fault';
    }
}

function updateCard(id, val) { const e = document.getElementById(id); if(e) e.textContent = val; }

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
    )

@app.route('/api/latest')
def api_latest():
    with _lock:
        return jsonify(_latest)


# ── Sleep prevention ─────────────────────────────────────────────────────────

def prevent_sleep():
    """
    Tells Windows to keep the system awake while the script runs.
    Uses the native SetThreadExecutionState API — no admin rights needed.
    Automatically releases when the script exits.
    Does nothing on Linux (Pi stays awake via systemd anyway).
    """
    import platform
    if platform.system() == 'Windows':
        import ctypes
        ES_CONTINUOUS       = 0x80000000
        ES_SYSTEM_REQUIRED  = 0x00000001
        ES_DISPLAY_REQUIRED = 0x00000002
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
        )
        log.info('Sleep prevention active — Windows will not sleep while script is running')
    else:
        log.info('Sleep prevention: Linux detected, skipping (Pi stays awake via systemd)')


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    prevent_sleep()
    t = threading.Thread(target=monitor_loop, daemon=True)
    t.start()
    log.info('Dashboard starting at http://0.0.0.0:5000')
    # Silence Flask request logs — they flood the terminal
    import logging as _logging
    _logging.getLogger('werkzeug').setLevel(_logging.ERROR)
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
