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

PLC_IP = '192.168.1.102'

LOG_DIR = os.path.expanduser('~/braider_logs')

PROCESS_LOG    = os.path.join(LOG_DIR, 'process_log.csv')
EVENT_LOG      = os.path.join(LOG_DIR, 'event_log.csv')
WIRE_BREAK_LOG = os.path.join(LOG_DIR, 'wire_break_log.csv')
OEE_LOG        = os.path.join(LOG_DIR, 'oee_log.csv')

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
    # Faults and wire breaks
    'No_Machine_Faults',
    'No_Machine_Msgs',
    'Local:1:I.Data',
    'Local:1:I.Fault',
    # Safety inputs — flip before state change, useful fault context
    'I_Door_Interlock_Ok',
    'I_Emergency_Stop_Ok',
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
    # Current state elapsed time — real-time OEE
    'Current_Hours.ACC',
    'Current_Minutes.ACC',
    'Current_Seconds.ACC',
    # Job definition
    'Discrete_Distance',
    'Discrete_Loops',
    'Loop_Length_Feet',
    'Carrier_Mode',
    'Current_Ratio',
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


def write_csv_row(filepath, row: dict):
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0
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

    prev_state     = None
    prev_wire_bits = None
    last_oee_poll  = 0
    recipe_name    = 'Unknown'
    recipe_ppi     = None

    log.info(f'Starting monitor loop -> PLC {PLC_IP}')

    while True:
        try:
            with LogixDriver(PLC_IP) as plc:
                if not plc.connected:
                    raise ConnectionError('LogixDriver connected=False')

                log.info('Connected to PLC')
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
                            'Machine_State':      machine_state,
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
                        'Discrete_Distance':  d.get('Discrete_Distance'),
                        'Discrete_Loops':     d.get('Discrete_Loops'),
                        'Loop_Length_Feet':   d.get('Loop_Length_Feet'),
                        'Carrier_Mode':       d.get('Carrier_Mode'),
                        'Current_Ratio':      d.get('Current_Ratio'),
                        'Recipe_Name':        recipe_name,
                        'Recipe_PPI':         recipe_ppi,
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
                    prev_state = machine_state

                    # ── Wire break detection (RUNNING only) ──────────────────
                    # Only fire during active production (state 16)
                    # Bobbin changes and maintenance while stopped generate
                    # identical bit transitions — filter them out here
                    if wire_bits is not None and prev_wire_bits is not None:
                        if wire_bits != prev_wire_bits:
                            changed    = wire_bits ^ prev_wire_bits
                            new_breaks = wire_bits & changed
                            cleared    = prev_wire_bits & changed

                            if machine_state == 16:
                                if new_breaks:
                                    log.warning(f'WIRE BREAK — bits:{bin(wire_bits)} at {puller_feet:.2f} ft')
                                    event_row = {
                                        'Timestamp':    timestamp,
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
                            else:
                                log.debug(f'Wire bits changed during {state_name(machine_state)} ' 
                                          f'(bobbin change?) — ignored: {prev_wire_bits} -> {wire_bits}')

                    prev_wire_bits = wire_bits

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
                            'connected':          True,
                        })

                    time.sleep(FAST_POLL_INTERVAL)

        except KeyboardInterrupt:
            log.info('Shutdown requested.')
            break
        except Exception as e:
            log.error(f'Connection lost: {e}')
            with _lock:
                _latest['connected'] = False
                _latest['last_error'] = str(e)
            log.info('Retrying in 10 seconds...')
            time.sleep(10)


# ── Flask dashboard ───────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Braider Monitor — Noble Gas Systems</title>
    <meta http-equiv="refresh" content="3">
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
        .axes { display:flex; gap:8px; margin-top:6px; }
        .axis { padding:3px 8px; border-radius:4px; font-size:12px; font-weight:bold; }
        .axis-ok   { background:#1b5e20; color:#66bb6a; }
        .axis-warn { background:#b71c1c; color:#ef9a9a; }
    </style>
</head>
<body>
    <h1>Braider Monitor</h1>
    <div class="sub">
        Noble Gas Systems — Steeger HS120/48 &nbsp;|&nbsp; 
        PLC {{ plc_ip }} &nbsp;|&nbsp; 
        Updated: {{ d.timestamp }}
    </div>

    <div class="section">Machine</div>
    <div class="grid">

        <div class="card">
            <div class="label">State</div>
            <div class="value {% if d.machine_state == 16 %}running
                              {% elif d.machine_state in (256,512) %}fault blink
                              {% elif d.machine_state in (64,128) %}paused
                              {% else %}stopped{% endif %}">
                {{ d.state_name or '—' }}
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
            <div class="value" style="font-size:20px">{{ d.recipe_name or '—' }}</div>
            <div class="unit">
                {{ d.recipe_ppi }} PPI setpoint
                {% if d.recipe_modified %}&nbsp;<span class="warn">modified</span>{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Job</div>
            <div class="value" style="font-size:20px">
                {{ d.discrete_distance or '—' }} ft
            </div>
            <div class="unit">
                {{ d.discrete_loops or '—' }} loops
                {% if d.mandrel_mode %}&nbsp;| mandrel{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Production This Session</div>
            <div class="value">{{ d.puller_pos_feet or '—' }}</div>
            <div class="unit">feet &nbsp;|&nbsp; seg {{ d.active_segment }}</div>
        </div>

    </div>

    <div class="section">Process</div>
    <div class="grid">

        <div class="card">
            <div class="label">Table Speed</div>
            <div class="value">{{ d.table_speed or '—' }}</div>
            <div class="unit">rev/s</div>
        </div>

        <div class="card">
            <div class="label">Puller Speed</div>
            <div class="value">{{ d.puller_speed or '—' }}</div>
            <div class="unit">in/s</div>
        </div>

        <div class="card">
            <div class="label">Speed Ratio</div>
            <div class="value" style="font-size:20px">
                {% if d.speed_ratio %}{{ "%.5f"|format(d.speed_ratio) }}{% else %}—{% endif %}
            </div>
            <div class="unit">puller ÷ table (PPI proxy)</div>
        </div>

        <div class="card">
            <div class="label">Wire Break Inputs</div>
            <div class="value {% if d.wire_break_bits is not none and d.wire_break_bits != 3 %}fault blink{% else %}ok{% endif %}">
                {{ d.wire_break_bits if d.wire_break_bits is not none else '—' }}
            </div>
            <div class="unit">Local:1:I.Data &nbsp;|&nbsp; normal = 3</div>
        </div>

    </div>

    <div class="section">Safety &amp; Axes</div>
    <div class="grid">

        <div class="card">
            <div class="label">Faults</div>
            <div class="value {% if d.no_faults %}ok{% else %}fault blink{% endif %}">
                {% if d.no_faults %}NONE{% else %}FAULT{% endif %}
            </div>
        </div>

        <div class="card">
            <div class="label">Safety Inputs</div>
            <div class="value" style="font-size:14px; line-height:1.8">
                <span class="{{ 'ok' if d.estop_ok else 'fault' }}">
                    {{ '✓' if d.estop_ok else '✗' }} E-Stop
                </span><br>
                <span class="{{ 'ok' if d.door_ok else 'fault' }}">
                    {{ '✓' if d.door_ok else '✗' }} Door
                </span><br>
                <span class="{{ 'ok' if d.guards_ok else 'fault' }}">
                    {{ '✓' if d.guards_ok else '✗' }} Guards
                </span>
            </div>
        </div>

        <div class="card">
            <div class="label">Servo Axis Sync</div>
            <div class="axes" style="flex-wrap:wrap; margin-top:8px;">
                {% for i, synced in [
                    (1, d.axis1_synced), (2, d.axis2_synced), (3, d.axis3_synced),
                    (4, d.axis4_synced), (5, d.axis5_synced)
                ] %}
                <span class="axis {{ 'axis-ok' if synced else 'axis-warn' }}">
                    OS{{ i }}
                </span>
                {% endfor %}
            </div>
            <div class="unit" style="margin-top:6px">all green = normal</div>
        </div>

        <div class="card">
            <div class="label">Axes Running</div>
            <div class="value {% if d.all_axes_running %}ok{% else %}fault{% endif %}">
                {% if d.all_axes_running %}YES{% else %}NO{% endif %}
            </div>
        </div>

    </div>

    <div class="section">OEE — Lifetime</div>
    <div class="grid">

        <div class="card">
            <div class="label">Running</div>
            <div class="value running">{{ d.cum_running_hrs or '—' }}</div>
            <div class="unit">hours cumulative</div>
        </div>

        <div class="card">
            <div class="label">Stopped</div>
            <div class="value stopped">{{ d.cum_stopped_hrs or '—' }}</div>
            <div class="unit">hours cumulative</div>
        </div>

        <div class="card">
            <div class="label">Ready / Idle</div>
            <div class="value paused">{{ d.cum_ready_hrs or '—' }}</div>
            <div class="unit">hours cumulative</div>
        </div>

        <div class="card">
            <div class="label">Availability</div>
            <div class="value" style="font-size:22px">
                {% if d.cum_running_hrs and d.cum_stopped_hrs and d.cum_ready_hrs %}
                    {% set total = d.cum_running_hrs + d.cum_stopped_hrs + d.cum_ready_hrs %}
                    {% if total > 0 %}
                        {{ "%.1f"|format(100 * d.cum_running_hrs / total) }}%
                    {% else %}—{% endif %}
                {% else %}—{% endif %}
            </div>
            <div class="unit">running ÷ (running+stopped+ready)</div>
        </div>

    </div>

    <div class="conn">
        PLC: <span class="{{ 'ok' if d.connected else 'fault' }}">
            {% if d.connected %}CONNECTED{% else %}DISCONNECTED — {{ d.last_error }}{% endif %}
        </span>
        &nbsp;|&nbsp; Logs: {{ log_dir }}
    </div>
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
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
