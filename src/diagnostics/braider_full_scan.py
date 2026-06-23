"""
braider_full_scan.py
====================
Logs all 379 PLC tags every 2 seconds for one full production day.
Produces two output files:
  1. braider_scan_raw_TIMESTAMP.csv   — every row, every tag, full history
  2. braider_tag_activity_summary.csv — live-updating change count per tag

Run:  python3 braider_full_scan.py
Stop: Ctrl+C  (summary auto-saves on exit)
"""

import csv
import json
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime

from pycomm3 import LogixDriver

# ── Configuration ─────────────────────────────────────────────────────────────
PLC_IP          = '192.168.1.102'
POLL_INTERVAL   = 2.0          # seconds between polls
OUTPUT_DIR      = os.path.expanduser('~/braider_logs/full_scan')
SUMMARY_FLUSH   = 60           # write summary file every N seconds

# ── Helpers ───────────────────────────────────────────────────────────────────

def flatten_value(value, tag_name=''):
    """
    Convert any pycomm3 return value into a clean, CSV-safe string.
    - Atomic types (bool, int, float) → returned as-is
    - Dicts / nested structs       → JSON string (compact)
    - None                         → empty string
    """
    if value is None:
        return ''
    if isinstance(value, bool):
        return int(value)          # 1 / 0 instead of True / False
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, dict):
        # Flatten known simple structs to readable format
        name = value.get('name', '') or tag_name
        # TIMER / COUNTER: just grab ACC and DN
        if 'ACC' in value and 'PRE' in value:
            dn  = int(value.get('DN', 0))
            acc = value.get('ACC', 0)
            pre = value.get('PRE', 0)
            return f'ACC={acc} PRE={pre} DN={dn}'
        # Anything else: compact JSON, strip internal_tags bloat
        clean = {k: v for k, v in value.items()
                 if k not in ('internal_tags', '_struct_members',
                               'type_class', 'template', 'attributes')}
        try:
            return json.dumps(clean, default=str)
        except Exception:
            return str(value)[:200]
    if isinstance(value, list):
        return json.dumps(value, default=str)
    return str(value)


def get_all_tags(plc):
    """Return sorted list of all controller + program-scoped tag names."""
    tag_names = []

    # Controller-scope tags
    for t in plc.get_tag_list():
        if isinstance(t, dict):
            tag_names.append(t['tag_name'])

    # Program-scoped tags
    for program in ['MainProgram', 'P01_TableDrive']:
        try:
            prog_tags = plc.get_tag_list(program=program)
            for t in prog_tags:
                if not isinstance(t, dict):
                    continue
                raw_name = t.get('tag_name', '')
                # Fix doubled name if present
                short_name = raw_name.split('.')[-1] if '.' in raw_name else raw_name
                full_name = f'Program:{program}.{short_name}'
                tag_names.append(full_name)
        except Exception as e:
            print(f'  Warning: could not get tags for {program}: {e}')

    return sorted(set(tag_names))


def read_all_in_batches(plc, tag_names, batch_size=50):
    """
    Read all tags in batches to stay within EtherNet/IP packet limits.
    Returns dict: {tag_name: value}
    """
    results = {}
    for i in range(0, len(tag_names), batch_size):
        batch = tag_names[i:i + batch_size]
        try:
            reads = plc.read(*batch)
            if not isinstance(reads, (list, tuple)):
                reads = [reads]
            for r in reads:
                if r is not None:
                    results[r.tag] = r.value if r.error is None else None
        except Exception as e:
            # If a batch fails, mark all in batch as None
            for t in batch:
                results[t] = None
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp_str  = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_path       = os.path.join(OUTPUT_DIR, f'braider_scan_raw_{timestamp_str}.csv')
    summary_path   = os.path.join(OUTPUT_DIR, 'braider_tag_activity_summary.csv')

    print(f'\nBraider Full Tag Scanner')
    print(f'========================')
    print(f'PLC:        {PLC_IP}')
    print(f'Interval:   {POLL_INTERVAL}s')
    print(f'Raw log:    {raw_path}')
    print(f'Summary:    {summary_path}')
    print(f'\nConnecting...')

    # ── Activity tracking ─────────────────────────────────────────────────────
    change_count   = defaultdict(int)   # how many times each tag changed
    last_value     = {}                 # last seen value per tag
    first_value    = {}                 # first seen value per tag
    last_type      = {}                 # data type string per tag
    poll_count     = 0
    null_count     = defaultdict(int)   # how many nulls per tag
    last_summary   = time.time()
    start_time     = time.time()

    def write_summary():
        """Write the activity summary CSV."""
        with open(summary_path, 'w', newline='') as sf:
            writer = csv.writer(sf)
            writer.writerow([
                'Tag Name', 'Data Type',
                'First Value', 'Last Value',
                'Change Count', 'Null Count',
                'Verdict'
            ])
            for tag in sorted(change_count.keys() | last_value.keys()):
                changes = change_count.get(tag, 0)
                nulls   = null_count.get(tag, 0)
                fv      = str(first_value.get(tag, ''))[:80]
                lv      = str(last_value.get(tag, ''))[:80]
                dt      = last_type.get(tag, '')

                if nulls == poll_count:
                    verdict = 'ALWAYS NULL'
                elif changes == 0:
                    verdict = 'STATIC'
                elif changes <= 5:
                    verdict = 'RARE CHANGE'
                elif changes <= 50:
                    verdict = 'OCCASIONAL'
                else:
                    verdict = 'ACTIVE'

                writer.writerow([tag, dt, fv, lv, changes, nulls, verdict])
        print(f'  [summary saved — {len(last_value)} tags, {poll_count} polls]')

    def on_exit(sig, frame):
        print('\n\nShutdown requested. Writing final summary...')
        write_summary()
        elapsed = time.time() - start_time
        h, m = divmod(int(elapsed), 3600)
        m, s = divmod(m, 60)
        print(f'Run time: {h}h {m}m {s}s  |  {poll_count} polls  |  {poll_count * len(last_value):,} data points')
        print(f'Files saved to: {OUTPUT_DIR}')
        sys.exit(0)

    signal.signal(signal.SIGINT,  on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    # ── Connect and discover tags ─────────────────────────────────────────────
    try:
        with LogixDriver(PLC_IP) as plc:
            tag_names = get_all_tags(plc)

            # Add all readable struct sub-tags discovered via probe_struct_tags.py
            STRUCT_SUBTAGS = [
                'servoPuller_Axis.AccelStatus',
                'servoPuller_Axis.ActualAcceleration',
                'servoPuller_Axis.ActualPosition',
                'servoPuller_Axis.ActualVelocity',
                'servoPuller_Axis.AxisFault',
                'servoPuller_Axis.CommandPosition',
                'servoPuller_Axis.CommandTorque',
                'servoPuller_Axis.CommandVelocity',
                'servoPuller_Axis.ConfigFault',
                'servoPuller_Axis.ConverterCapacity',
                'servoPuller_Axis.DCBusVoltage',
                'servoPuller_Axis.DecelStatus',
                'servoPuller_Axis.DriveEnableStatus',
                'servoPuller_Axis.GroupFault',
                'servoPuller_Axis.MotionFault',
                'servoPuller_Axis.MotionStatus',
                'servoPuller_Axis.MotorCapacity',
                'servoPuller_Axis.MoveStatus',
                'servoPuller_Axis.OutputCurrent',
                'servoPuller_Axis.OutputFrequency',
                'servoPuller_Axis.OutputPower',
                'servoPuller_Axis.OutputVoltage',
                'servoPuller_Axis.PositionError',
                'servoPuller_Axis.ServoActionStatus',
                'servoPuller_Axis.TorqueReferenceLimited',
                'servoPuller_Axis.VelocityError',
                'servoPuller_Axis.VelocityFeedback',
                'servoTable_Axis.AccelStatus',
                'servoTable_Axis.ActualAcceleration',
                'servoTable_Axis.ActualPosition',
                'servoTable_Axis.ActualVelocity',
                'servoTable_Axis.AxisFault',
                'servoTable_Axis.CommandPosition',
                'servoTable_Axis.CommandTorque',
                'servoTable_Axis.CommandVelocity',
                'servoTable_Axis.ConfigFault',
                'servoTable_Axis.ConverterCapacity',
                'servoTable_Axis.DCBusVoltage',
                'servoTable_Axis.DecelStatus',
                'servoTable_Axis.DriveEnableStatus',
                'servoTable_Axis.GroupFault',
                'servoTable_Axis.MotionFault',
                'servoTable_Axis.MotionStatus',
                'servoTable_Axis.MotorCapacity',
                'servoTable_Axis.MoveStatus',
                'servoTable_Axis.OutputCurrent',
                'servoTable_Axis.OutputFrequency',
                'servoTable_Axis.OutputPower',
                'servoTable_Axis.OutputVoltage',
                'servoTable_Axis.PositionError',
                'servoTable_Axis.ServoActionStatus',
                'servoTable_Axis.TorqueReferenceLimited',
                'servoTable_Axis.VelocityError',
                'servoTable_Axis.VelocityFeedback',
                'servoBraider_Group.AxisFault',
                'servoBraider_Group.ConfigFault',
                'servoBraider_Group.GroupFault',
                'servoBraider_Group.GroupStatus',
                'Table_Drive:I.Active',
                'Table_Drive:I.AtReference',
                'Table_Drive:I.Faulted',
                'Table_Drive:I.OutputFreq',
                'Table_Drive:I.Ready',
                'Table_Drive:O.FreqCommand',
                'ServoDrive_01:S.CoarseUpdatePeriod',
                'Table_Encoder:S.CoarseUpdatePeriod',
                'DL_IX:C.Data',
                'DL_IX:I.Data',
                'DL_IX:O.Data',
                'Local:1:I.Data',
                'Local:1:O.Data',
                'Local:2:I.Data',
            ]
            for st in STRUCT_SUBTAGS:
                if st not in tag_names:
                    tag_names.append(st)
            tag_names = sorted(tag_names)
            print(f'Total tags including struct sub-tags: {len(tag_names)}')
            print(f'Starting scan. Press Ctrl+C to stop and save summary.\n')

            # Get data types once
            tag_type_map = {}
            raw_tags = plc.get_tag_list()
            for t in raw_tags:
                if isinstance(t, dict):
                    tn = t.get('tag_name', '')
                    dt = t.get('data_type_name', '')
                    if not dt:
                        dt_raw = t.get('data_type', {})
                        dt = dt_raw.get('name', 'UNKNOWN') if isinstance(dt_raw, dict) else str(dt_raw)
                    tag_type_map[tn] = dt

            # ── Write raw CSV header ──────────────────────────────────────────
            with open(raw_path, 'w', newline='') as rf:
                raw_writer = csv.writer(rf)
                header = ['Timestamp', 'Poll_ms'] + tag_names
                raw_writer.writerow(header)

                # ── Poll loop ─────────────────────────────────────────────────
                while True:
                    t0 = time.time()
                    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                    values = read_all_in_batches(plc, tag_names)
                    poll_ms = int((time.time() - t0) * 1000)
                    poll_count += 1

                    # Build row and track changes
                    row = [ts, poll_ms]
                    for tag in tag_names:
                        raw_val = values.get(tag)
                        flat    = flatten_value(raw_val, tag)
                        row.append(flat)

                        # Track activity
                        flat_str = str(flat)
                        if raw_val is None:
                            null_count[tag] += 1

                        if tag not in first_value:
                            first_value[tag] = flat_str
                            last_type[tag]   = tag_type_map.get(tag, '')

                        if tag in last_value and last_value[tag] != flat_str:
                            change_count[tag] += 1

                        last_value[tag] = flat_str

                    raw_writer.writerow(row)

                    # Progress print every 30 polls (~60s)
                    if poll_count % 30 == 0:
                        elapsed = time.time() - start_time
                        h, m = divmod(int(elapsed), 3600)
                        m2, s = divmod(m, 60)
                        active = sum(1 for v in change_count.values() if v > 0)
                        print(f'  {h:02d}:{m:02d}:{s:02d}  poll #{poll_count}  '
                              f'{poll_ms}ms/poll  {active} active tags')

                    # Flush summary periodically
                    if time.time() - last_summary >= SUMMARY_FLUSH:
                        write_summary()
                        last_summary = time.time()

                    # Sleep remainder of interval
                    elapsed_poll = time.time() - t0
                    sleep_time = max(0, POLL_INTERVAL - elapsed_poll)
                    time.sleep(sleep_time)

    except ConnectionError as e:
        print(f'ERROR: Could not connect to PLC at {PLC_IP}')
        print(f'  {e}')
        print(f'  Make sure you are on the braider network (192.168.1.x)')
        sys.exit(1)
    except Exception as e:
        print(f'Unexpected error: {e}')
        import traceback
        traceback.print_exc()
        write_summary()
        sys.exit(1)


if __name__ == '__main__':
    main()
