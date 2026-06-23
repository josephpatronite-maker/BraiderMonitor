"""
verify_potential_tags.py
Checks the 54 "POTENTIAL" candidate tags against the live PLC:
  1. Can pycomm3 actually read each one (valid tag path)?
  2. What's its current value?
  3. Over a sampling window, does it actually change, or sit static?

This is read-only, lightweight (54 tags, single connection) — safe to run
alongside the production monitor without contention, since it opens its own
independent LogixDriver connection just like hires_loop/oee_loop already do.

A 60s sample is too short to settle anything for load-dependent signals
(current, voltage, torque, RPM) — those only show real variation while the
machine is actively running a job, and short samples can land entirely in
an idle window. Default sample window is now 1 hour (3600s) to span at
least part of a real production run. Run this DURING active production,
not while the machine sits idle, for a meaningful result.

Usage:
    python3 verify_potential_tags.py                   # 1-hour sample (default)
    python3 verify_potential_tags.py --sample-sec 14400 # full 4-hour half-shift
    python3 verify_potential_tags.py --sample-sec 60    # quick check only
    python3 verify_potential_tags.py --sample-sec 0     # single read only, no sampling
"""

import argparse
import csv
import time
from datetime import datetime, timedelta

from pycomm3 import LogixDriver

PLC_IP = '192.168.1.102'

POTENTIAL_TAGS = [
    'Local:2:I.Data',
    'CurrentRecipe.EnableTakeUp',
    'CurrentRecipe.EnableHeater',
    'CurrentRecipe.SensorModeEnable',
    'Machine.Estops_Ok',
    'Machine.All_Axes_Homed',
    'Machine.Any_Start_Pressed',
    'Machine.Any_Stop_Pressed',
    'Machine.Any_Pause_Pressed',
    'Machine.Auto_Run_State_Active',
    'Machine_Statistics.Cur_State',
    'servoPuller_Axis.AxisFault',
    'servoPuller_Axis.MotionFaultStatus',
    'servoPuller_Axis.PositionError',
    'servoPuller_Axis.VelocityError',
    'servoPuller_Axis.TorqueReference',
    'servoPuller_Axis.TorqueReferenceLimited',
    'servoPuller_Axis.TorqueEstimate',
    'servoPuller_Axis.OutputCurrent',
    'servoPuller_Axis.OutputVoltage',
    'servoPuller_Axis.OutputPower',
    'servoPuller_Axis.DCBusVoltage',
    'servoPuller_Axis.ExcessivePositionErrorFault',
    'servoPuller_Axis.ExcessiveVelocityErrorFault',
    'servoPuller_Axis.OvertorqueLimitFault',
    'servoPuller_Axis.UndertorqueLimitFault',
    'servoTable_Axis.AxisFault',
    'servoTable_Axis.MotionFaultStatus',
    'servoTable_Axis.PositionError',
    'servoTable_Axis.VelocityError',
    'servoTable_Axis.TorqueReference',
    'servoTable_Axis.TorqueReferenceLimited',
    'servoTable_Axis.TorqueEstimate',
    'servoTable_Axis.OutputCurrent',
    'servoTable_Axis.OutputVoltage',
    'servoTable_Axis.OutputPower',
    'servoTable_Axis.DCBusVoltage',
    'servoTable_Axis.ExcessivePositionErrorFault',
    'servoTable_Axis.ExcessiveVelocityErrorFault',
    'servoTable_Axis.OvertorqueLimitFault',
    'servoTable_Axis.UndertorqueLimitFault',
    'DL_IX:I.Data',
    'Program:P02_PullerServo.ServoStatus.Motor_RPM',
    'Program:P02_PullerServo.ServoStatus.Peak_Torque',
    'Program:P02_PullerServo.ServoStatus.FilteredTorque',
    'Program:P02_PullerServo.Servo_Axis_Faults',
    'Program:MainProgram.Fault_SERCOS',
    'Program:MainProgram.Fault_StartingTimeout',
    'Program:MainProgram.Master_SegDist',
    'Program:MainProgram.Master_TransDist',
    'Program:MainProgram.Table_Drive_Fault',
    'Program:P01_TableDrive.ServoStatus.Motor_RPM',
    'Program:P01_TableDrive.ServoStatus.Peak_Torque',
    'Program:P01_TableDrive.ServoStatus.FilteredTorque',
    # Machine_State included as context — lets the long-run summary report
    # what fraction of the sample the machine was actually RUNNING (16),
    # so a "STATIC" verdict on a load-dependent tag can be read correctly
    # (genuinely static during a real run vs. machine was idle the whole time).
    'Machine_State',
]



def initial_read(plc):
    """Single read of all tags — confirms each is a valid, resolvable path."""
    print(f"\n{'Tag':<55} {'Status':<10} {'Value':<20} {'Type'}")
    print('-' * 100)

    results = plc.read(*POTENTIAL_TAGS)
    readable = {}
    errors = {}

    for r in results:
        if r.error:
            errors[r.tag] = r.error
            print(f"{r.tag:<55} {'ERROR':<10} {str(r.error)[:40]}")
        else:
            readable[r.tag] = r.value
            val_str = str(r.value)
            if len(val_str) > 18:
                val_str = val_str[:18] + '…'
            print(f"{r.tag:<55} {'OK':<10} {val_str:<20} {type(r.value).__name__}")

    print(f"\nReadable: {len(readable)}/{len(POTENTIAL_TAGS)}")
    if errors:
        print(f"Errors: {len(errors)}")
        for tag, err in errors.items():
            print(f"  {tag}: {err}")

    return readable, errors


def sample_for_changes(plc, readable_tags, duration_sec, poll_interval=2.0):
    """Poll the readable tags repeatedly and track which ones actually change."""
    if duration_sec <= 0 or not readable_tags:
        return {}

    duration_str = str(timedelta(seconds=int(duration_sec)))
    print(f"\nSampling {len(readable_tags)} readable tags for {duration_str} "
          f"at {poll_interval}s intervals...")
    print("Run this DURING active production for a meaningful result on load-dependent "
          "tags (current, voltage, torque, RPM) — those only vary while the machine is "
          "actually running a job under load.\n")

    last_values = {}
    change_counts = {tag: 0 for tag in readable_tags}
    min_values = {}
    max_values = {}
    n_polls = 0
    running_polls = 0  # polls where Machine_State == 16

    start_time = time.time()
    end_time = start_time + duration_sec
    last_minute_mark = -1

    while time.time() < end_time:
        try:
            results = plc.read(*readable_tags)
        except Exception as e:
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}] Read error, retrying: {e}")
            time.sleep(poll_interval)
            continue

        for r in results:
            if r.error:
                continue
            prev = last_values.get(r.tag)
            if prev is not None and prev != r.value:
                change_counts[r.tag] += 1
            last_values[r.tag] = r.value

            if isinstance(r.value, (int, float)) and not isinstance(r.value, bool):
                min_values[r.tag] = min(min_values.get(r.tag, r.value), r.value)
                max_values[r.tag] = max(max_values.get(r.tag, r.value), r.value)

        if last_values.get('Machine_State') == 16:
            running_polls += 1

        n_polls += 1
        elapsed = time.time() - start_time
        minute_mark = int(elapsed // 60)

        # Print a real log line once per minute instead of a single carriage-return
        # countdown — visible in nohup/detached SSH session logs for long runs.
        if minute_mark != last_minute_mark:
            last_minute_mark = minute_mark
            remaining = str(timedelta(seconds=max(0, int(end_time - time.time()))))
            running_pct = (100 * running_polls / n_polls) if n_polls else 0
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                  f"{minute_mark} min elapsed, {remaining} remaining — "
                  f"{n_polls} polls, {running_pct:.0f}% RUNNING so far")

        time.sleep(poll_interval)

    total_running_pct = (100 * running_polls / n_polls) if n_polls else 0
    print(f"\nSampling complete — {n_polls} polls over {duration_str}")
    print(f"Machine was in RUNNING state for {total_running_pct:.0f}% of the sample "
          f"({running_polls}/{n_polls} polls)")
    if total_running_pct < 20:
        print("WARNING: machine was running less than 20% of this sample. Any tag showing "
              "STATIC below may simply not have been under load long enough to tell — "
              "consider re-running during a longer continuous production stretch.\n")
    else:
        print()

    return {
        'change_counts': change_counts,
        'min_values': min_values,
        'max_values': max_values,
        'last_values': last_values,
        'n_polls': n_polls,
        'running_polls': running_polls,
        'running_pct': total_running_pct,
    }


def classify_result(tag, change_count, n_polls, min_v, max_v):
    if n_polls == 0:
        return 'NOT_SAMPLED'
    if change_count == 0:
        return 'STATIC — never changed during sample'
    rate = change_count / n_polls
    range_note = ''
    if min_v is not None and max_v is not None and min_v != max_v:
        range_note = f' (range {min_v} to {max_v})'
    if rate > 0.3:
        return f'ACTIVE — changed {change_count}/{n_polls} polls{range_note}'
    return f'OCCASIONAL — changed {change_count}/{n_polls} polls{range_note}'


def export_results(readable, errors, sample_data, out_path='potential_tags_verified.csv'):
    running_pct = sample_data.get('running_pct') if sample_data else None

    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Tag', 'Readable', 'Current Value', 'Data Type',
                          'Sample Verdict', 'Min', 'Max', 'Running % During Sample'])

        for tag in POTENTIAL_TAGS:
            if tag == 'Machine_State':
                continue  # context tag only, not a candidate to report on

            if tag in errors:
                writer.writerow([tag, 'NO', '', '', f'ERROR: {errors[tag]}', '', '', ''])
                continue

            val = readable.get(tag)
            dtype = type(val).__name__ if tag in readable else ''

            if sample_data:
                cc = sample_data['change_counts'].get(tag, 0)
                np_ = sample_data['n_polls']
                mn = sample_data['min_values'].get(tag)
                mx = sample_data['max_values'].get(tag)
                verdict = classify_result(tag, cc, np_, mn, mx)
                last_val = sample_data['last_values'].get(tag, val)
                writer.writerow([tag, 'YES', last_val, dtype, verdict,
                                  mn if mn is not None else '', mx if mx is not None else '',
                                  f'{running_pct:.0f}%' if running_pct is not None else ''])
            else:
                writer.writerow([tag, 'YES', val, dtype, 'NOT SAMPLED (single read only)', '', '', ''])

    print(f"\nResults exported to {out_path}")


def main():
    ap = argparse.ArgumentParser(description="Verify the 50 POTENTIAL candidate tags against the live PLC")
    ap.add_argument('--plc-ip', default=PLC_IP)
    ap.add_argument('--sample-sec', type=float, default=3600,
                     help='Seconds to sample for changes after the initial read. '
                          'Default is 1 hour (3600s) — long enough to span real '
                          'production activity for load-dependent tags. Set to 0 '
                          'to skip sampling and only do the single validity read.')
    ap.add_argument('--poll-interval', type=float, default=2.0)
    args = ap.parse_args()

    print(f"Connecting to {args.plc_ip}...")
    with LogixDriver(args.plc_ip) as plc:
        if not plc.connected:
            print("ERROR: could not connect to PLC.")
            return

        print(f"Connected. Controller: {plc.info.get('product_name', 'unknown')}")

        readable, errors = initial_read(plc)

        sample_data = None
        if args.sample_sec > 0 and readable:
            sample_data = sample_for_changes(plc, list(readable.keys()),
                                              args.sample_sec, args.poll_interval)

            print(f"{'Tag':<55} {'Verdict'}")
            print('-' * 100)
            for tag in readable:
                if tag == 'Machine_State':
                    continue
                cc = sample_data['change_counts'].get(tag, 0)
                np_ = sample_data['n_polls']
                mn = sample_data['min_values'].get(tag)
                mx = sample_data['max_values'].get(tag)
                print(f"{tag:<55} {classify_result(tag, cc, np_, mn, mx)}")

        export_results(readable, errors, sample_data)


if __name__ == '__main__':
    main()
