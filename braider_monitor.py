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
    'Low_PPI',            # <-- Add this
    'Hi_PPI'
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
    from datetime import datetime
    now = datetime.now()

    is_sunday_midnight   = (now.weekday() == 6 and now.hour == 0 and now.minute == 0)
    is_monthstart_midnight = (now.day == 1 and now.hour == 0 and now.minute == 0)

    archived_any = False

    if is_sunday_midnight:
        week_label = now.strftime('%Y_%m_%d')
        if os.path.exists(PROCESS_LOG) and os.path.getsize(PROCESS_LOG) > 0:
            archive_name = PROCESS_LOG.replace('.csv', f'_week_ending_{week_label}.csv')
            os.rename(PROCESS_LOG, archive_name)
            log.info(f'Weekly archive: {os.path.basename(PROCESS_LOG)} -> {os.path.basename(archive_name)}')
            archived_any = True

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
        with open(filepath, 'r', newline='') as f:
            existing_headers = f.readline().strip().split(',')
        new_headers = list(row.keys())
        if existing_headers != new_headers:
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

def calculate_daily_state_percentages():
    """
    Reads the process_log.csv from the bottom up, filters for rows matching 
    the current calendar day, and calculates the percentage of time spent in each state.
    """
    import os
    if not os.path.exists(PROCESS_LOG) or os.path.getsize(PROCESS_LOG) == 0:
        return {}

    today_str = datetime.now().strftime('%Y-%m-%d')
    state_counts = {}
    total_rows = 0

    # Read backward in chunks to optimize Pi memory usage
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
                
                # Keep the incomplete first line for the next iteration
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
                    if len(parts) > 2:
                        row_timestamp = parts[0] # YYYY-MM-DD HH:MM:SS
                        if row_timestamp.startswith(today_str):
                            state_name = parts[3] # State_Name column
                            if state_name:
                                state_counts[state_name] = state_counts.get(state_name, 0) + 1
                                total_rows += 1
                        else:
                            # We found a row from yesterday! We can safely stop scanning.
                            done = True
                            break
        except Exception as e:
            log.error(f"Error calculating daily OEE percentages: {e}")
            return {}

    if total_rows == 0:
        return {}

    # Also check archived files from today (handles mid-shift restarts)
    import glob
    today_compact = datetime.now().strftime('%Y%m%d')
    base = PROCESS_LOG.replace('.csv', '')
    for archive in sorted(glob.glob(base + '_archived_' + today_compact + '*.csv')):
        try:
            with open(archive, newline='', encoding='utf-8', errors='replace') as af:
                import csv as _csv
                for row in _csv.DictReader(af):
                    ts = row.get('Timestamp', '')
                    if ts.startswith(today_str):
                        state = row.get('State_Name', 'UNKNOWN') or 'UNKNOWN'
                        state_counts[state] = state_counts.get(state, 0) + 1
                        total_rows += 1
        except Exception:
            pass

    if total_rows == 0:
        return {}

    # Convert counts to exact percentages
    return {state: round((count / total_rows) * 100, 1) for state, count in state_counts.items()}


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
    # Process
    'taper_sensor':      None,
    'speed_ratio':       None,
    'machine_faults':    None,
    'no_msgs':           None,
    'state_elapsed_s':   None,
    'vfd_freq_actual':   None,
    'vfd_freq_command':  None,
    'vfd_freq_delta':    None,
    'vfd_at_ref':        None,
    'vfd_faulted':       None,
    'vfd_active':        None,
    'i_table_motor_ol':  None,
    'i_triaxial_wb':     None,
    'core_break':        None,
    'all_safties_ok':    None,
    'all_axes_ok':       None,
    'puller_pos_error':  None,
    'inactivity_secs':   None,
    'new_part':          None,
    'run_complete':      None,
    'length_to_run':     None,
    'wire_break_move':   None,
    'estop_recover':     None,
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
    running_started_at  = None   
    recipe_ppi          = None

    log.info(f'Starting monitor loop -> PLC {PLC_IP}')

    while True:
        try:
            with LogixDriver(PLC_IP) as plc:
                if not plc.connected:
                    raise ConnectionError('LogixDriver connected=False')

                log.info('Connected to PLC')
                monitor_loop._retry_count = 0  
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

                    speed_ratio = None
                    if table_speed and puller_speed and table_speed > 0:
                        speed_ratio = round(puller_speed / table_speed, 6)

                    state_elapsed_s = None
                    ch = d.get('Current_Hours.ACC')
                    cm = d.get('Current_Minutes.ACC')
                    cs = d.get('Current_Seconds.ACC')
                    if ch is not None and cm is not None and cs is not None:
                        state_elapsed_s = (ch * 3600) + (cm * 60) + round(cs / 1000, 1)

                    # ── OEE poll (Runs instantly on first loop connection) ────
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
                            recipe_name     = recipe_raw.get('Name', 'Unknown')

                            # PPI selection — Body_PPI from recipe struct is the correct value
                            # Hi/Low PPI tags are only used in mandrel mode
                            mandrel_mode_val = od.get('HMI_Mandrel_Mode')
                            body_ppi   = recipe_raw.get('Body_PPI')
                            hi_ppi     = od.get('Hi_PPI')
                            low_ppi    = od.get('Low_PPI')
                            hi_running = od.get('Hi_PPI_Running')

                            if mandrel_mode_val:
                                # Mandrel mode — Hi pass takes priority over Low
                                if hi_running == 1 and hi_ppi is not None:
                                    recipe_ppi = hi_ppi
                                elif low_ppi is not None:
                                    recipe_ppi = low_ppi
                                else:
                                    recipe_ppi = body_ppi
                            else:
                                # Standard mode — Body_PPI from recipe struct is correct
                                recipe_ppi = body_ppi
                            
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
                        'Door_Ok':            d.get('I_Door_Interlock_Ok'),
                        'Estop_Ok':           d.get('I_Emergency_Stop_Ok'),
                        'Guards_Ok':          d.get('Machine.Guards_Ok'),
                        'All_Safties_Ok':     d.get('Machine.All_Safties_Ok'),
                        'All_Axes_Ok':        d.get('Machine.All_Axes_Ok'),
                        'All_Axes_Running':   d.get('Machine.All_Axes_Running'),
                        'AxisSynced_1':       d.get('AxisSynced_OS1'),
                        'AxisSynced_2':       d.get('AxisSynced_OS2'),
                        'AxisSynced_3':       d.get('AxisSynced_OS3'),
                        'AxisSynced_4':       d.get('AxisSynced_OS4'),
                        'AxisSynced_5':       d.get('AxisSynced_OS5'),
                        'Recipe_Name':        recipe_name,
                        'Recipe_PPI':         recipe_ppi,
                        'VFD_Freq_Actual':    d.get('Table_Drive:I.OutputFreq'),
                        'VFD_Freq_Command':   d.get('Table_Drive:O.FreqCommand'),
                        'VFD_Freq_Delta':     (d.get('Table_Drive:O.FreqCommand', 0) or 0) - (d.get('Table_Drive:I.OutputFreq', 0) or 0),
                        'VFD_Faulted':        d.get('Table_Drive:I.Faulted'),
                        'VFD_Active':         d.get('Table_Drive:I.Active'),
                        'VFD_AtReference':    d.get('Table_Drive:I.AtReference'),
                        'Transition_Active':  d.get('Transition_Active'),
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
                        'Length_To_Run':      d.get('Length_To_Run'),
                        'Run_Complete':       d.get('Run_Complete'),
                        'Taper_Sensor':       d.get('Taper_Sensor_Input'),
                        'Sensor_Mode':        d.get('Sensor_Mode_Enable'),
                        'New_Part':           d.get('New_Part_Latch'),
                        'PPI_Change':         d.get('PPI_Change_ONS'),
                        'Inactivity_Secs':    d.get('Inactivity_Timer.ACC'),
                        'WireBreak_Move':     d.get('WireBreak_Move'),
                        'EStop_Recover':      d.get('EStop_Recover'),
                    }
                    write_csv_row(PROCESS_LOG, process_row)
                    _rolling_buffer.append(process_row.copy())

                    if _wire_break_capturing:
                        _wire_break_capture_rows.append(process_row.copy())
                        if now >= _wire_break_capture_until:
                            for r in _wire_break_capture_rows:
                                write_csv_row(WIRE_BREAK_LOG, r)
                            log.info(f'Wire break capture complete — {len(_wire_break_capture_rows)} rows saved')
                            _wire_break_capturing = False
                            _wire_break_capture_rows = []

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
                    
                    if machine_state == 16 and prev_state != 16:
                        running_started_at = now
                    prev_state = machine_state

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
                                log.debug(f'Wire bits changed during startup grace period — ignored')
                            else:
                                log.debug(f'Wire bits changed during structural setup')

                    prev_wire_bits = wire_bits

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

                    # ── Update dashboard state dict ───────────────────────────
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
                            'cum_running_hrs':    cum_running if cum_running is not None else _latest.get('cum_running_hrs'),
                            'cum_stopped_hrs':    cum_stopped if cum_stopped is not None else _latest.get('cum_stopped_hrs'),
                            'cum_ready_hrs':      cum_ready if cum_ready is not None else _latest.get('cum_ready_hrs'),
                            'recipe_modified':    recipe_modified,
                            'mandrel_mode':       mandrel_mode,
                            'vfd_freq_actual':    d.get('Table_Drive:I.OutputFreq'),
                            'vfd_freq_command':   d.get('Table_Drive:O.FreqCommand'),
                            'vfd_freq_delta':     (d.get('Table_Drive:O.FreqCommand', 0) or 0) - (d.get('Table_Drive:I.OutputFreq', 0) or 0),
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
                            'daily_state_pcts':   calculate_daily_state_percentages(),
                        })

                    archive_logs()
                    time.sleep(FAST_POLL_INTERVAL)

        except KeyboardInterrupt:
            log.info('Shutdown requested.')
            break
        except Exception as e:
            with _lock:
                _latest['connected'] = False
                _latest['last_error'] = str(e)

            if not hasattr(monitor_loop, '_retry_count'):
                monitor_loop._retry_count = 0
            monitor_loop._retry_count += 1

            if monitor_loop._retry_count <= 3:
                wait = 10
                log.error(f'Connection lost: {e}')
            elif monitor_loop._retry_count <= 10:
                wait = 60
                if monitor_loop._retry_count == 4:
                    log.warning('PLC unreachable — switching to 60s retry interval')
            else:
                wait = 300
                if monitor_loop._retry_count % 12 == 0:
                    log.warning(f'PLC still unreachable — retrying every 5min')

            time.sleep(wait)


# ── Flask dashboard template string ───────────────────────────────────────────

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
                code {{ d.machine_state or 0 }} &nbsp;|&nbsp; <span id="elapsed-value">{% if d.state_elapsed_s %}{{ (d.state_elapsed_s // 3600)|int }}h {{ ((d.state_elapsed_s % 3600) // 60)|int }}m{% endif %}</span>
            </div>
        </div>

        <div class="card">
            <div class="label">Recipe</div>
            <div class="value" style="font-size:22px">{{ d.recipe_name or '—' }}</div>
            <div class="unit">
                Body: {{ d.recipe_ppi }} PPI
                {% if d.connector_ppi %}&nbsp;| Conn: {{ d.connector_ppi }} PPI{% endif %}
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

        <div class="card" style="min-width: 280px;">
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
            <div class="unit">rev/s &nbsp;|&nbsp; <span id="table-rpm-value" style="color:#4fc3f7">—</span> rpm</div>
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

        <div class="card">
            <div class="label">Wire Break Inputs</div>
            <div id="wb-div" class="value {% if d.wire_break_bits is not none and d.wire_break_bits != 3 %}fault blink{% else %}ok{% endif %}">
                <span id="wb-value">{{ d.wire_break_bits if d.wire_break_bits is not none else '—' }}</span>
            </div>
            <div class="unit">Local:1:I.Data &nbsp;|&nbsp; normal = 3</div>
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

    </div>

    <div class="section">Live — Last 2.5 Minutes</div>
    <div style="background:#2a2a2a; border-radius:8px; padding:14px; margin-bottom:12px;">
        <canvas id="liveChart" style="width:100%; height:300px; display:block;"></canvas>
    </div>

    <div class="conn" id="conn-bar">
        PLC: <span id="conn-status" class="ok">CONNECTED</span>
        &nbsp;|&nbsp; Logs: {{ log_dir }}
        &nbsp;|&nbsp; Braider_2
        &nbsp;|&nbsp; <span id="sound-toggle" onclick="toggleSound()" style="cursor:pointer">🔔 Sound ON</span>
        &nbsp;|&nbsp; <span id="last-update" style="color:#555"></span>
    </div>

<script>
let soundEnabled = localStorage.getItem('soundEnabled') === 'true';
function toggleSound() {
    soundEnabled = !soundEnabled;
    localStorage.setItem('soundEnabled', soundEnabled);
    document.getElementById('sound-toggle').textContent = soundEnabled ? '🔔 Sound ON' : '🔕 Sound OFF';
}
document.getElementById('sound-toggle').textContent = soundEnabled ? '🔔 Sound ON' : '🔕 Sound OFF';

let audioContext = null;
let audioUnlocked = false;
const silentAudio = new Audio("data:audio/wav;base64,UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA=");

function unlockAudio() {
    if (audioUnlocked) return;
    silentAudio.play().catch(() => {});
    if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
    if (audioContext.state === 'suspended') audioContext.resume();
    audioUnlocked = true;
}
document.addEventListener('click', unlockAudio, { once: false });

function beep(freq, duration, volume) {
    try {
        unlockAudio();
        const ctx = audioContext || new (window.AudioContext || window.webkitAudioContext)();
        audioContext = ctx;
        if (ctx.state === 'suspended') ctx.resume();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain); gain.connect(ctx.destination);
        gain.gain.value = volume || 0.3;
        osc.frequency.value = freq; osc.type = 'sine';
        osc.start(); osc.stop(ctx.currentTime + duration / 1000);
    } catch(e) {}
}
function alertWireBreak() {
    beep(880, 200, 0.4); setTimeout(() => beep(880, 200, 0.4), 300); setTimeout(() => beep(880, 200, 0.4), 600);
}
function alertStateChange() { beep(440, 300, 0.2); }

// ── Live chart plain canvas ───────────────────────────────────────────────
const MAX_POINTS = 75;  
const tableSpeed    = Array(MAX_POINTS).fill(null);
const pullerSpeed   = Array(MAX_POINTS).fill(null);
const speedRatio    = Array(MAX_POINTS).fill(null);
const timestamps    = Array(MAX_POINTS).fill('');
const machineStates = Array(MAX_POINTS).fill(0);

const canvas = document.getElementById('liveChart');
const canvasCtx = canvas.getContext('2d');

function drawChart() {
    const W = canvas.width  = canvas.parentElement.clientWidth - 28;
    const H = canvas.height = 300;
    const PAD   = { top: 8, right: 10, bottom: 22, left: 52 };
    const GAP   = 8;
    const PANELS = 3;
    const plotW  = W - PAD.left - PAD.right;
    const panelH = (H - PAD.top - PAD.bottom - GAP * (PANELS-1)) / PANELS;

    canvasCtx.clearRect(0, 0, W, H);
    const panelTop = p => PAD.top + p * (panelH + GAP);

    function drawBackground(p) {
        for (let i = 0; i < MAX_POINTS - 1; i++) {
            const x0 = PAD.left + (i / (MAX_POINTS-1)) * plotW;
            const x1 = PAD.left + ((i+1) / (MAX_POINTS-1)) * plotW;
            canvasCtx.fillStyle = machineStates[i] === 16 ? 'rgba(102,187,106,0.12)' : 'rgba(144,164,174,0.08)';
            canvasCtx.fillRect(x0, panelTop(p), x1-x0, panelH);
        }
        canvasCtx.strokeStyle = '#333'; canvasCtx.lineWidth = 0.5;
        for (let i = 0; i <= 3; i++) {
            const y = panelTop(p) + (i/3) * panelH;
            canvasCtx.beginPath(); canvasCtx.moveTo(PAD.left, y); canvasCtx.lineTo(PAD.left + plotW, y); canvasCtx.stroke();
        }
    }

    function range(arr) {
        const vals = arr.filter(v => v !== null && isFinite(v));
        if (!vals.length) return [0, 1];
        const mn = Math.min(...vals), mx = Math.max(...vals);
        const pad = (mx - mn) * 0.15 || 0.05;
        return [mn - pad, mx + pad];
    }

    function drawLine(p, data, color, mn, mx) {
        canvasCtx.strokeStyle = color; canvasCtx.lineWidth = 1.5; canvasCtx.lineJoin = 'round';
        canvasCtx.beginPath();
        let started = false;
        const top = panelTop(p);
        for (let i = 0; i < MAX_POINTS; i++) {
            if (data[i] === null || !isFinite(data[i])) { started = false; continue; }
            const x = PAD.left + (i / (MAX_POINTS-1)) * plotW;
            const y = top + panelH - ((data[i] - mn) / (mx - mn)) * panelH;
            if (!started) { canvasCtx.moveTo(x, y); started = true; }
            else canvasCtx.lineTo(x, y);
        }
        canvasCtx.stroke();
    }

    function labelY(p, mn, mx, color) {
        canvasCtx.fillStyle = color; canvasCtx.font = '9px monospace'; canvasCtx.textAlign = 'right';
        canvasCtx.fillText(mx.toFixed(3), PAD.left - 3, panelTop(p) + 9);
        canvasCtx.fillText(mn.toFixed(3), PAD.left - 3, panelTop(p) + panelH - 2);
    }

    function labelPanel(p, text, color) {
        canvasCtx.fillStyle = color; canvasCtx.font = 'bold 10px monospace'; canvasCtx.textAlign = 'left';
        canvasCtx.fillText(text, PAD.left + 4, panelTop(p) + 11);
    }

    const [tMn, tMx] = range(tableSpeed);
    drawBackground(0); drawLine(0, tableSpeed, '#4fc3f7', tMn, tMx); labelY(0, tMn, tMx, '#4fc3f7'); labelPanel(0, 'Table Speed (rev/s)', '#4fc3f7');

    const [pMn, pMx] = range(pullerSpeed);
    drawBackground(1); drawLine(1, pullerSpeed, '#81c784', pMn, pMx); labelY(1, pMn, pMx, '#81c784'); labelPanel(1, 'Puller Speed (in/s)', '#81c784');

    const [rMn, rMx] = range(speedRatio);
    drawBackground(2); drawLine(2, speedRatio, '#ffb74d', rMn, rMx); labelY(2, rMn, rMx, '#ffb74d'); labelPanel(2, 'Speed Ratio', '#ffb74d');

    canvasCtx.fillStyle = '#555'; canvasCtx.font = '9px monospace'; canvasCtx.textAlign = 'center';
    const xBottom = panelTop(2) + panelH + 14;
    if (timestamps[0])            canvasCtx.fillText(timestamps[0],            PAD.left,         xBottom);
    if (timestamps[MAX_POINTS-1]) canvasCtx.fillText(timestamps[MAX_POINTS-1], PAD.left + plotW, xBottom);
    const mid = Math.floor(MAX_POINTS/2);
    if (timestamps[mid]) canvasCtx.fillText(timestamps[mid], PAD.left + plotW/2, xBottom);
}

// ── Fetch loop tracking variables ─────────────────────────────────────────────
let lastState  = null;
let lastWBBits = null;
let lastSeenTimestamp = "";
let timestampAgeTicks = 0;

async function fetchAndUpdate() {
    try {
        const res  = await fetch('/api/latest');
        const data = await res.json();
        const now  = new Date().toLocaleTimeString('en-US', { hour12: false });
        const ts   = data.table_speed  || 0;
        const ps   = data.puller_speed || 0;
        const sr   = data.speed_ratio  || null;
        const wb   = data.wire_break_bits;
        const st   = data.machine_state;

        // ── BULLETPROOF STALE CHECK (Clock-Sync Independent) ──
        let isStale = false;
        if (data.timestamp) {
            if (data.timestamp === lastSeenTimestamp) {
                timestampAgeTicks++;
            } else {
                lastSeenTimestamp = data.timestamp;
                timestampAgeTicks = 0;
            }
            if (timestampAgeTicks >= 5 || !data.connected) {
                isStale = true;
            }
        } else {
            isStale = true;
        }

        tableSpeed.shift();    tableSpeed.push(isStale ? null : ts);
        pullerSpeed.shift();   pullerSpeed.push(isStale ? null : ps);
        speedRatio.shift();    speedRatio.push(isStale ? null : sr);
        timestamps.shift();    timestamps.push(now);
        machineStates.shift(); machineStates.push(isStale ? 0 : (st || 0));

        drawChart();

        const statusEl = document.getElementById('conn-status');
        if (isStale || !data.connected) {
            statusEl.textContent = 'STALE DATA — PLC UNREACHABLE';
            statusEl.className = 'fault';
        } else {
            statusEl.textContent = 'CONNECTED';
            statusEl.className = 'ok';
        }
        
        document.getElementById('last-update').textContent = 'updated ' + now;
        const hts = document.getElementById('header-timestamp');
        if (hts) hts.textContent = isStale ? '—' : (data.timestamp || '—');

        const upd = (id, v) => { const e = document.getElementById(id); if(e) e.textContent = isStale ? '—' : v; };
        upd('feet-value',   data.puller_pos_feet  ? data.puller_pos_feet.toFixed(2)  : '—');
        upd('table-value',  ts ? ts.toFixed(4) : '—');
        upd('table-rpm-value', ts ? (ts * 60).toFixed(1) : '—');
        upd('puller-value', ps ? ps.toFixed(4) : '—');
        upd('ratio-value',  sr ? sr.toFixed(5) : '—');
        upd('taper-value',  data.taper_sensor !== null && data.taper_sensor !== undefined ? data.taper_sensor.toFixed(2) : '—');
        upd('vfd-actual',   data.vfd_freq_actual   !== null ? data.vfd_freq_actual   : '—');
        upd('vfd-command',  data.vfd_freq_command  !== null ? data.vfd_freq_command  : '—');
        upd('vfd-delta',    data.vfd_freq_delta    !== null ? data.vfd_freq_delta    : '0');
        const vfdRef = document.getElementById('vfd-at-ref');
        if (vfdRef) vfdRef.innerHTML = data.vfd_at_ref ? '&nbsp;<span class="ok">AT REF</span>' : '';
        upd('wb-value',     wb !== null ? wb : '—');

        function setSafety(id, ok, okText, faultText, faultClass) {
            const el = document.getElementById(id);
            if (!el) return;
            el.textContent = (ok && !isStale) ? '✓ ' + okText : '✗ ' + (isStale ? 'DATA STALE' : faultText);
            el.className = (ok && !isStale) ? 'ok' : faultClass;
        }
        setSafety('safety-estop',    data.estop_ok,        'E-Stop',    'E-STOP PRESSED', 'fault blink');
        setSafety('safety-door',     data.door_ok,         'Door',      'Door Open',      'warn');
        setSafety('safety-guards',   data.guards_ok,       'Guards',    'Guards Open',    'fault');
        setSafety('safety-motor',    !data.i_table_motor_ol,'Motor OK', 'MOTOR OL',       'fault blink');
        setSafety('safety-triaxial', !data.i_triaxial_wb,  'Triaxial OK','TRIAXIAL WB',  'fault blink');
        setSafety('safety-core',     !data.core_break,     'Core OK',   'CORE BREAK',     'fault blink');

        const elapsed = data.state_elapsed_s;
        const elapsedEl = document.getElementById('elapsed-value');
        if (elapsedEl) {
            if (elapsed && !isStale) {
                const h = Math.floor(elapsed / 3600);
                const m = Math.floor((elapsed % 3600) / 60);
                elapsedEl.textContent = h + 'h ' + m + 'm';
            } else {
                elapsedEl.textContent = '—';
            }
        }

       // ── Daily Equipment Utilization Pie Chart calculation ────────────────
        const pcts = data.daily_state_pcts || {};
        const oeeEl = document.getElementById('oee-value');
        const legendEl = document.getElementById('oee-legend');
        const pieCanvas = document.getElementById('oeePieCanvas');

        // Color definitions matching the style scheme in braider_analysis.py
        const stateColors = {
            'RUNNING':  '#66bb6a', // Green
            'READY':    '#4fc3f7', // Light Blue
            'STOPPED':  '#ef5350', // Red
            'PAUSED':   '#ffa726', // Orange
            'OFF':      '#78909c', // Gray
            'FAULT':    '#d32f2f', // Dark Red
            'ABORTED':  '#b71c1c', 
            'UNKNOWN':  '#555555'
        };

        if (oeeEl && Object.keys(pcts).length > 0 && !isStale) {
            // Main Utilization Percentage is time spent in RUNNING state
            const runningPct = pcts['RUNNING'] || 0;
            oeeEl.textContent = runningPct.toFixed(1) + '%';

            // Force the inline CSS style color directly to ensure class defaults are ignored
            if (runningPct >= 50) {
                oeeEl.style.color = '#66bb6a'; // Green (ok)
            } else if (runningPct >= 25) {
                oeeEl.style.color = '#ffa726'; // Yellow/Orange (warn)
            } else {
                oeeEl.style.color = '#ef5350'; // Red (fault)
            }

            // Generate textual legend details
            let legendHTML = '';
            for (const [state, pct] of Object.entries(pcts)) {
                const color = stateColors[state] || '#999';
                legendHTML += `<div><span style="display:inline-block; width:8px; height:8px; background:${color}; margin-right:4px; border-radius:2px;"></span>${state}: ${pct}%</div>`;
            }
            if (legendEl) legendEl.innerHTML = legendHTML;

            // Render the Canvas Pie Chart
            if (pieCanvas) {
                const ctx = pieCanvas.getContext('2d');
                ctx.clearRect(0, 0, 110, 110);
                const cx = 55, cy = 55, r = 50;
                let startAngle = -Math.PI / 2;
                for (const [state, pct] of Object.entries(pcts)) {
                    if (pct <= 0) continue;
                    const sliceAngle = (pct / 100) * (2 * Math.PI);
                    ctx.beginPath();
                    ctx.moveTo(cx, cy);
                    ctx.arc(cx, cy, r, startAngle, startAngle + sliceAngle);
                    ctx.closePath();
                    ctx.fillStyle = stateColors[state] || '#999';
                    ctx.fill();
                    ctx.strokeStyle = '#0d1117';
                    ctx.lineWidth = 1.5;
                    ctx.stroke();
                    startAngle += sliceAngle;
                }
                // Donut hole
                ctx.beginPath();
                ctx.arc(cx, cy, r * 0.52, 0, Math.PI * 2);
                ctx.fillStyle = '#161b22';
                ctx.fill();
            }
        } else if (oeeEl) {
            oeeEl.textContent = '—';
            oeeEl.className = 'value';
            if (legendEl) legendEl.textContent = isStale ? 'Data stale' : 'Waiting for metrics...';
            if (pieCanvas) {
                const ctx = pieCanvas.getContext('2d');
                ctx.clearRect(0, 0, 110, 110);
            }
        }

        const stateEl = document.getElementById('state-value');
        if (stateEl) {
            stateEl.textContent = isStale ? 'UNKNOWN (DISCONNECTED)' : (data.state_name || '—');
            const stateDiv = document.getElementById('state-div');
            if (stateDiv) {
                stateDiv.className = 'value ' + (
                    isStale                  ? 'stopped' :
                    st === 16                ? 'running' :
                    st === 256 || st === 512 ? 'fault blink' :
                    st === 64  || st === 128 ? 'paused' : 'stopped'
                );
            }
        }

        const wbDiv = document.getElementById('wb-div');
        if (wbDiv) {
            wbDiv.className = 'value ' + (wb !== null && wb !== 3 && !isStale ? 'fault blink' : 'ok');
        }

        const faultDiv  = document.getElementById('fault-div');
        const faultUnit = document.getElementById('fault-unit');
        if (faultDiv) {
            const hasFault = !data.no_faults;
            faultDiv.className = 'value ' + ((hasFault && !isStale) ? 'fault blink' : 'ok');
            faultDiv.textContent = isStale ? 'UNKNOWN' : (hasFault ? 'FAULT' : 'NONE');
        }
        if (faultUnit && data.machine_faults && data.machine_faults !== 4 && !isStale) {
            faultUnit.textContent = 'code: ' + data.machine_faults;
        } else if (faultUnit) {
            faultUnit.textContent = '';
        }

        if (soundEnabled && !isStale) {
            if (wb !== lastWBBits && wb !== 3 && st === 16) alertWireBreak();
            else if (st !== lastState && lastState !== null) alertStateChange();
        }
        if (!isStale) {
            lastState = st; 
            lastWBBits = wb;
        }

    } catch(e) {
        const cs = document.getElementById('conn-status');
        if (cs) { cs.textContent = 'DISCONNECTED'; cs.className = 'fault'; }
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
    """Builds today's floor report from process_log and event_log."""
    import csv as csv_mod
    from datetime import date, timedelta

    today_str     = date.today().isoformat()
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()

    def read_today_rows(filepath, today):
        rows = []
        # Read from live file
        if os.path.exists(filepath):
            try:
                with open(filepath, newline='', encoding='utf-8', errors='replace') as f:
                    all_rows = list(csv_mod.DictReader(f))
                for row in reversed(all_rows):
                    ts = row.get('Timestamp', '')
                    if ts.startswith(today):
                        rows.append(row)
                    elif rows:
                        break
                rows.reverse()
            except Exception:
                pass

        # Also check archived files from today — covers restarts mid-shift
        import glob, re
        base = filepath.replace('.csv', '')
        pattern = base + '_archived_' + today.replace('-', '') + '*.csv'
        for archive in sorted(glob.glob(pattern)):
            try:
                with open(archive, newline='', encoding='utf-8', errors='replace') as f:
                    archive_rows = [r for r in csv_mod.DictReader(f)
                                    if r.get('Timestamp', '').startswith(today)]
                # Prepend archive rows — they come before live rows
                rows = archive_rows + rows
            except Exception:
                pass

        # Deduplicate by timestamp, keep order
        seen = set()
        deduped = []
        for row in rows:
            ts = row.get('Timestamp', '')
            if ts not in seen:
                seen.add(ts)
                deduped.append(row)
        return deduped

    def read_day_rows(filepath, day):
        rows = []
        if not os.path.exists(filepath):
            return rows
        try:
            with open(filepath, newline='', encoding='utf-8', errors='replace') as f:
                for row in csv_mod.DictReader(f):
                    if row.get('Timestamp', '').startswith(day):
                        rows.append(row)
        except Exception:
            pass
        return rows

    proc_rows  = read_today_rows(PROCESS_LOG, today_str)
    event_rows = read_today_rows(EVENT_LOG,   today_str)

    # State breakdown
    state_counts = {}
    for row in proc_rows:
        s = row.get('State_Name', 'UNKNOWN') or 'UNKNOWN'
        state_counts[s] = state_counts.get(s, 0) + 1

    total_rows   = len(proc_rows)
    def pct(n):  return round(100 * n / total_rows, 1) if total_rows else 0
    def hrs(n):  return round(n * 2 / 3600, 2) if n else 0

    running_rows = state_counts.get('RUNNING', 0)
    off_rows     = state_counts.get('OFF', 0)
    stopped_rows = state_counts.get('STOPPED', 0)

    # Vessels completed — RUNNING → OFF or STOPPED transition
    vessels = 0
    prev_state = None
    for row in proc_rows:
        s = row.get('State_Name', '')
        if prev_state == 'RUNNING' and s in ('OFF', 'STOPPED', 'STOPPING'):
            vessels += 1
        prev_state = s

    # Bobbin changeovers — OFF state durations > 5 min
    changeovers  = []
    in_off       = False
    off_start_ts = None
    for row in proc_rows:
        s  = row.get('State_Name', '')
        ts = row.get('Timestamp', '')
        if s == 'OFF' and not in_off:
            in_off = True; off_start_ts = ts
        elif s != 'OFF' and in_off:
            in_off = False
            if off_start_ts:
                try:
                    from datetime import datetime as dt
                    dur = (dt.fromisoformat(ts) - dt.fromisoformat(off_start_ts)).total_seconds() / 60
                    if dur > 5:
                        changeovers.append({'start': off_start_ts, 'end': ts, 'min': round(dur, 1)})
                except Exception:
                    pass

    avg_co = round(sum(c['min'] for c in changeovers) / len(changeovers), 1) if changeovers else None
    min_co = min((c['min'] for c in changeovers), default=None)
    max_co = max((c['min'] for c in changeovers), default=None)

    # Timeline from event_log
    timeline = []
    wire_breaks = 0
    for row in event_rows:
        etype = row.get('Event_Type', '')
        if etype == 'STATE_CHANGE':
            timeline.append({'time': row.get('Timestamp', '')[:19],
                             'from_state': row.get('From_State', ''),
                             'to_state':   row.get('To_State', ''),
                             'feet':       row.get('Puller_Pos_Feet', '')})
        elif etype == 'WIRE_BREAK':
            wire_breaks += 1
            timeline.append({'time': row.get('Timestamp', '')[:19],
                             'from_state': 'WIRE BREAK', 'to_state': '',
                             'feet':       row.get('Puller_Pos_Feet', '')})

    # Yesterday comparison
    yest_rows    = read_day_rows(PROCESS_LOG, yesterday_str)
    yest_total   = len(yest_rows)
    yest_running = sum(1 for r in yest_rows if r.get('State_Name') == 'RUNNING')
    yest_run_pct = round(100 * yest_running / yest_total, 1) if yest_total else None
    yest_vessels = 0
    prev = None
    for row in yest_rows:
        s = row.get('State_Name', '')
        if prev == 'RUNNING' and s in ('OFF', 'STOPPED', 'STOPPING'):
            yest_vessels += 1
        prev = s

    VESSEL_TARGET      = 4
    RUNNING_TARGET_PCT = 55.0

    return {
        'date':               today_str,
        'total_hrs':          hrs(total_rows),
        'running_hrs':        hrs(running_rows),
        'running_pct':        pct(running_rows),
        'off_hrs':            hrs(off_rows),
        'off_pct':            pct(off_rows),
        'stopped_pct':        pct(stopped_rows),
        'vessels':            vessels,
        'vessel_target':      VESSEL_TARGET,
        'running_target_pct': RUNNING_TARGET_PCT,
        'changeovers':        changeovers,
        'avg_changeover':     avg_co,
        'min_changeover':     min_co,
        'max_changeover':     max_co,
        'wire_breaks':        wire_breaks,
        'timeline':           timeline,
        'yest_running_pct':   yest_run_pct,
        'yest_vessels':       yest_vessels,
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
.bar-fill  { height:100%; border-radius:4px; }
.bar-green  { background:#238636; } .bar-yellow { background:#9e6a03; } .bar-red { background:#da3633; }
.section-title { font-size:1.1rem; font-weight:600; color:#58a6ff; margin-bottom:12px;
                 border-bottom:1px solid #21262d; padding-bottom:6px; }
.section { margin-bottom:28px; }
table { width:100%; border-collapse:collapse; font-size:.9rem; }
th { background:#161b22; color:#8b949e; text-align:left; padding:8px 12px; font-weight:600;
     font-size:.78rem; text-transform:uppercase; letter-spacing:.05em; }
td { padding:8px 12px; border-top:1px solid #21262d; }
tr:hover td { background:#161b22; }
.badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:.78rem; font-weight:600; }
.badge-running  { background:#1a4a1a; color:#3fb950; }
.badge-off      { background:#1f2a3c; color:#58a6ff; }
.badge-stopped  { background:#3c1a1a; color:#f85149; }
.badge-starting { background:#1a2a3c; color:#79c0ff; }
.badge-stopping { background:#3c2a1a; color:#d29922; }
.badge-break    { background:#4a3000; color:#e3b341; }
.badge-other    { background:#21262d; color:#8b949e; }
.timeline { list-style:none; }
.timeline li { display:flex; align-items:flex-start; gap:14px; padding:10px 0;
               border-top:1px solid #21262d; font-size:.9rem; }
.tl-time { color:#8b949e; min-width:85px; font-family:monospace; font-size:.85rem; }
.tl-feet { color:#8b949e; font-size:.8rem; margin-left:auto; }
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
@media(max-width:700px){ .two-col{ grid-template-columns:1fr; } }
.footer { color:#444; font-size:.78rem; margin-top:20px; text-align:center; }
</style>
</head>
<body>

<h1>Braider 2 — Daily Performance</h1>
<div class="subtitle">{{ d.date }} &nbsp;·&nbsp; Refreshes every 60s
  &nbsp;·&nbsp; {{ d.total_hrs }}h logged today</div>

<div class="scorecard">

  {% set v_color = 'green' if d.vessels >= d.vessel_target else ('yellow' if d.vessels >= d.vessel_target - 1 else 'red') %}
  <div class="card {{ v_color }}">
    <div class="card-label">Vessels Today</div>
    <div class="card-value" style="color:{% if v_color=='green' %}#3fb950{% elif v_color=='yellow' %}#d29922{% else %}#f85149{% endif %}">{{ d.vessels }}</div>
    <div class="card-sub">Target: {{ d.vessel_target }}</div>
    {% if d.yest_vessels is not none %}
    <div class="card-vs">
      {% if d.vessels > d.yest_vessels %}<span class="up">▲ {{ d.vessels - d.yest_vessels }} vs yesterday</span>
      {% elif d.vessels < d.yest_vessels %}<span class="down">▼ {{ d.yest_vessels - d.vessels }} vs yesterday</span>
      {% else %}<span class="same">= same as yesterday</span>{% endif %}
    </div>{% endif %}
    <div class="bar-wrap"><div class="bar-fill bar-{{ v_color }}" style="width:{{ [100,(d.vessels/d.vessel_target*100)|int]|min }}%"></div></div>
  </div>

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

  {% set s_color = 'green' if d.stopped_pct <= 10 else ('yellow' if d.stopped_pct <= 20 else 'red') %}
  <div class="card {{ s_color }}">
    <div class="card-label">Unplanned Stops</div>
    <div class="card-value" style="color:{% if s_color=='green' %}#3fb950{% elif s_color=='yellow' %}#d29922{% else %}#f85149{% endif %}">{{ d.stopped_pct }}%</div>
    <div class="card-sub">of shift in STOPPED state</div>
  </div>

</div>

<div class="two-col">
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

  <div class="section">
    <div class="section-title">State Timeline ({{ d.timeline|length }} events)</div>
    {% if d.timeline %}
    <ul class="timeline">
    {% for t in d.timeline[-30:] | reverse %}
      <li>
        <span class="tl-time">{{ t.time[11:] }}</span>
        {% if t.from_state == 'WIRE BREAK' %}
          <span class="badge badge-break">⚡ WIRE BREAK</span>
        {% else %}
          {% set fs = t.from_state|upper %}
          {% set ts2 = t.to_state|upper %}
          <span class="badge {% if fs=='RUNNING' %}badge-running{% elif fs=='OFF' %}badge-off{% elif fs in ('STOPPED','ABORTED','ABORTING') %}badge-stopped{% elif fs in ('STARTING','STOPPING') %}badge-starting{% else %}badge-other{% endif %}">{{ fs }}</span>
          <span style="color:#30363d">→</span>
          <span class="badge {% if ts2=='RUNNING' %}badge-running{% elif ts2=='OFF' %}badge-off{% elif ts2 in ('STOPPED','ABORTED','ABORTING') %}badge-stopped{% elif ts2 in ('STARTING','STOPPING') %}badge-starting{% else %}badge-other{% endif %}">{{ ts2 }}</span>
        {% endif %}
        {% if t.feet %}<span class="tl-feet">{{ t.feet|float|round(1) }} ft</span>{% endif %}
      </li>
    {% endfor %}
    </ul>
    {% if d.timeline|length > 30 %}<p style="color:#8b949e;font-size:.8rem;margin-top:8px">Showing last 30 of {{ d.timeline|length }} events</p>{% endif %}
    {% else %}
    <p style="color:#8b949e;padding:12px 0">No state changes recorded yet today.</p>
    {% endif %}
  </div>
</div>

<!-- ── Utilization Pie Charts ── -->
<div class="two-col" style="margin-bottom:28px;">
  <div class="section">
    <div class="section-title">Today's Utilization — Midnight to Now</div>
    <div style="display:flex;align-items:center;gap:20px;padding:12px 0;">
      <canvas id="todayPie" width="160" height="160" style="flex-shrink:0;"></canvas>
      <div id="todayPieLegend" style="font-size:11px;line-height:2;color:#8b949e;"></div>
    </div>
  </div>
  <div class="section">
    <div class="section-title">This Week's Utilization — Mon to Now</div>
    <div style="display:flex;align-items:center;gap:20px;padding:12px 0;">
      <canvas id="weekPie" width="160" height="160" style="flex-shrink:0;"></canvas>
      <div id="weekPieLegend" style="font-size:11px;line-height:2;color:#8b949e;"></div>
    </div>
  </div>
</div>

<!-- ── State Timeline Charts ── -->
<div class="section" style="margin-top:8px;">
  <div class="section-title">Today — Machine State Timeline</div>
  <div id="todayChart" style="width:100%;height:220px;"></div>
</div>

<div class="section" style="margin-top:8px;">
  <div class="section-title">This Week — Machine State Timeline</div>
  <div id="weekChart" style="width:100%;height:220px;"></div>
</div>

<div class="footer">Braider 2 · braider2.local:5000/floor · Refreshes every 60s · Noble Gas Systems</div>

<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<script>
const STATE_COLORS = {
    'RUNNING':'#66bb6a','STOPPED':'#ef5350','OFF':'#455a64',
    'READY':'#4fc3f7','STARTING':'#26c6da','STOPPING':'#ff7043',
    'PAUSING':'#ffb74d','PAUSED':'#ffa726','ABORTING':'#ab47bc','ABORTED':'#b71c1c'
};

function buildStateChart(divId, data, title) {
    if (!data.timestamps || !data.timestamps.length) {
        document.getElementById(divId).innerHTML =
            '<p style="color:#8b949e;padding:20px">No data yet — chart populates during production.</p>';
        return;
    }
    const ts     = data.timestamps;
    const states = data.states;
    const traces = [];
    const seen   = {};

    for (let i = 0; i < ts.length; i++) {
        const s = states[i];
        if (!s) continue;
        if (!seen[s]) seen[s] = true;
        traces.push({s, t: ts[i]});
    }

    // One trace per state — scatter markers
    const byState = {};
    for (const pt of traces) {
        if (!byState[pt.s]) byState[pt.s] = [];
        byState[pt.s].push(pt.t);
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
        legend:{orientation:'h', y:-0.2, font:{size:10}},
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

    // Count rows per state
    const counts = {};
    for (const s of data.states) {
        if (s) counts[s] = (counts[s] || 0) + 1;
    }
    const total = Object.values(counts).reduce((a,b) => a+b, 0);
    if (!total) return;

    const ORDER = ['RUNNING','OFF','STOPPED','READY','STARTING','STOPPING','PAUSING','PAUSED','ABORTING','ABORTED'];
    const entries = ORDER.filter(s => counts[s])
                         .map(s => [s, counts[s]])
                         .concat(Object.entries(counts).filter(([s]) => !ORDER.includes(s)));

    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    const cx = W/2, cy = H/2, r = Math.min(W,H)/2 - 4;
    ctx.clearRect(0,0,W,H);

    let angle = -Math.PI/2;
    let legendHTML = '';
    for (const [state, count] of entries) {
        const sweep = (count/total) * Math.PI * 2;
        const color = STATE_COLORS[state] || '#999';
        ctx.beginPath();
        ctx.moveTo(cx,cy);
        ctx.arc(cx,cy,r,angle,angle+sweep);
        ctx.closePath();
        ctx.fillStyle = color;
        ctx.fill();
        ctx.strokeStyle = '#0d1117';
        ctx.lineWidth = 1.5;
        ctx.stroke();
        const pct = (count/total*100).toFixed(1);
        const hrs = (count*2/3600).toFixed(1);
        legendHTML += `<div><span style="color:${color};font-weight:700;">■</span> ${state}: ${pct}% (${hrs}h)</div>`;
        angle += sweep;
    }
    // Donut hole
    ctx.beginPath();
    ctx.arc(cx,cy,r*0.52,0,Math.PI*2);
    ctx.fillStyle = '#161b22';
    ctx.fill();
    // Center text — running %
    const runPct = counts['RUNNING'] ? (counts['RUNNING']/total*100).toFixed(0) : '0';
    ctx.fillStyle = '#3fb950';
    ctx.font = 'bold 22px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(runPct + '%', cx, cy-8);
    ctx.fillStyle = '#8b949e';
    ctx.font = '10px Arial';
    ctx.fillText('running', cx, cy+12);

    if (legend) legend.innerHTML = legendHTML;
}

(async function() {
    try {
        const [todayRes, weekRes] = await Promise.all([
            fetch('/api/floor_data?range=today'),
            fetch('/api/floor_data?range=week'),
        ]);
        const todayData = await todayRes.json();
        const weekData  = await weekRes.json();
        drawPie('todayPie', 'todayPieLegend', todayData);
        drawPie('weekPie',  'weekPieLegend',  weekData);
        buildStateChart('todayChart', todayData, 'Today');
        buildStateChart('weekChart',  weekData,  'This Week');
    } catch(e) {
        ['todayChart','weekChart'].forEach(id => {
            document.getElementById(id).innerHTML =
                '<p style="color:#8b949e;padding:20px">Chart unavailable.</p>';
        });
    }
})();
</script>
</body>
</html>"""


@app.route('/api/floor_data')
def api_floor_data():
    import csv as csv_mod
    from datetime import date, timedelta
    from flask import request as freq
    range_param = freq.args.get('range', 'today')
    today = date.today()
    if range_param == 'week':
        # Monday midnight of current week
        days_since_monday = today.weekday()  # 0=Mon, 6=Sun
        monday = today - timedelta(days=days_since_monday)
        cutoff = monday.isoformat()
        def row_matches(ts): return ts >= cutoff
    else:
        # Today midnight to now
        today_str = today.isoformat()
        def row_matches(ts): return ts.startswith(today_str)

    timestamps, table_speed, puller_speed, speed_ratio, states = [], [], [], [], []
    import glob
    all_rows = []
    # Include archived files that match the date range
    base = PROCESS_LOG.replace('.csv', '')
    if range_param == 'week':
        # Grab up to 7 days of archives
        from datetime import date as _date, timedelta as _td
        for i in range(7):
            d = (_date.today() - _td(days=i)).strftime('%Y%m%d')
            for archive in glob.glob(base + '_archived_' + d + '*.csv'):
                try:
                    with open(archive, newline='', encoding='utf-8', errors='replace') as f:
                        all_rows.extend(r for r in csv_mod.DictReader(f) if row_matches(r.get('Timestamp','')))
                except Exception:
                    pass
    else:
        # Today — check archives from today
        today_compact = date.today().strftime('%Y%m%d')
        for archive in glob.glob(base + '_archived_' + today_compact + '*.csv'):
            try:
                with open(archive, newline='', encoding='utf-8', errors='replace') as f:
                    all_rows.extend(r for r in csv_mod.DictReader(f) if row_matches(r.get('Timestamp','')))
            except Exception:
                pass

    if os.path.exists(PROCESS_LOG):
        try:
            with open(PROCESS_LOG, newline='', encoding='utf-8', errors='replace') as f:
                all_rows.extend(r for r in csv_mod.DictReader(f) if row_matches(r.get('Timestamp','')))
        except Exception:
            pass

    # Sort by timestamp and deduplicate
    seen_ts = set()
    deduped = []
    for r in sorted(all_rows, key=lambda x: x.get('Timestamp','')):
        ts = r.get('Timestamp','')
        if ts not in seen_ts:
            seen_ts.add(ts)
            deduped.append(r)
    all_rows = deduped

    # For week view subsample to every 10th row to keep response small
    step = 10 if range_param == 'week' else 1
    matching = all_rows[::step]
    for row in matching:
        timestamps.append(row.get('Timestamp', ''))
        try: table_speed.append(float(row.get('Table_Speed', 0) or 0))
        except: table_speed.append(0)
        try: puller_speed.append(float(row.get('Puller_Speed', 0) or 0))
        except: puller_speed.append(0)
        try: speed_ratio.append(float(row.get('Speed_Ratio') or 0) or None)
        except: speed_ratio.append(None)
        states.append(row.get('State_Name', '') or '')
    return jsonify({'timestamps': timestamps, 'table_speed': table_speed,
                    'puller_speed': puller_speed, 'speed_ratio': speed_ratio,
                    'states': states})


@app.route('/floor')
def floor_report():
    d = get_floor_report_data()
    from flask import render_template_string
    return render_template_string(FLOOR_HTML, d=d)

# ── Sleep prevention ─────────────────────────────────────────────────────────

def prevent_sleep():
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
    import logging as _logging
    _logging.getLogger('werkzeug').setLevel(_logging.ERROR)
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)