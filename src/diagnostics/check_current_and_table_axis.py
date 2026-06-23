"""
check_current_and_table_axis.py
Two focused questions, single quick run:

1. Are the active/reactive current tags on servoPuller_Axis alive (like
   TorqueReference) or dead (like OutputCurrent)?
2. Is servoTable_Axis as a whole reading from a live/active axis instance,
   or is it structurally disconnected from whatever Program:P01_TableDrive
   actually uses to drive the table?

Polls every few seconds for a short window (default 2 minutes — long enough
to see real variation if it exists, short enough to get an answer fast).
Run this WHILE the machine is running for a meaningful result.

Usage:
    python3 check_current_and_table_axis.py
    python3 check_current_and_table_axis.py --duration-sec 300
"""

import argparse
import time
from datetime import datetime

from pycomm3 import LogixDriver

PLC_IP = '192.168.1.102'

# Question 1: active/reactive current cluster on the puller axis (known-live side)
CURRENT_CLUSTER = [
    'servoPuller_Axis.ActiveCurrentReference',
    'servoPuller_Axis.ActiveCurrentReferenceFiltered',
    'servoPuller_Axis.ActiveCurrentReferenceCompensated',
    'servoPuller_Axis.ActiveCurrentFeedback',
    'servoPuller_Axis.ActiveCurrentError',
    'servoPuller_Axis.ReactiveCurrentReference',
    'servoPuller_Axis.ReactiveCurrentReferenceCompensated',
    'servoPuller_Axis.ReactiveCurrentReferenceLimited',
    'servoPuller_Axis.ReactiveCurrentFeedback',
    'servoPuller_Axis.ReactiveCurrentError',
    'servoPuller_Axis.ActiveCurrentReferenceLimited',
    # Known reference points for comparison
    'servoPuller_Axis.TorqueReference',          # confirmed ALIVE last check
    'servoPuller_Axis.OutputCurrent',             # confirmed DEAD last check
]

# Question 2: every readable field on servoTable_Axis worth a broad sweep,
# plus the known-live comparison point from Program:P01_TableDrive
TABLE_AXIS_SWEEP = [
    'servoTable_Axis.ActualVelocity',             # already collected — should move
    'servoTable_Axis.ActualPosition',             # already collected — should move
    'servoTable_Axis.CommandVelocity',
    'servoTable_Axis.CommandPosition',
    'servoTable_Axis.VelocityFeedback',           # already collected — should move
    'servoTable_Axis.PositionError',
    'servoTable_Axis.VelocityError',
    'servoTable_Axis.TorqueReference',
    'servoTable_Axis.TorqueEstimate',
    'servoTable_Axis.OutputCurrent',
    'servoTable_Axis.OutputVoltage',
    'servoTable_Axis.OutputPower',
    'servoTable_Axis.ActiveCurrentFeedback',
    'servoTable_Axis.MotionStatus',
    'servoTable_Axis.AxisFault',
    'servoTable_Axis.DriveEnableStatus',
    # Live comparison point — confirmed moving (128.8 RPM) in the last check
    'Program:P01_TableDrive.ServoStatus.Motor_RPM',
    'Machine_State',
]

ALL_TAGS = list(dict.fromkeys(CURRENT_CLUSTER + TABLE_AXIS_SWEEP))


def run_check(plc, tags, duration_sec, poll_interval=2.0):
    print(f"Polling {len(tags)} tags for {duration_sec}s at {poll_interval}s intervals...\n")

    last_values = {}
    change_counts = {t: 0 for t in tags}
    min_values, max_values = {}, {}
    n_polls = 0
    running_polls = 0

    end_time = time.time() + duration_sec
    while time.time() < end_time:
        results = plc.read(*tags)
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
        time.sleep(poll_interval)

    return last_values, change_counts, min_values, max_values, n_polls, running_polls


def report(title, tags, last_values, change_counts, min_values, max_values, n_polls):
    print(f"\n{'='*100}\n{title}\n{'='*100}")
    print(f"{'Tag':<55} {'Last Value':<22} {'Changes':<10} {'Range'}")
    print('-'*100)
    for t in tags:
        if t not in last_values:
            print(f"{t:<55} {'(read error)':<22}")
            continue
        val = last_values[t]
        cc = change_counts.get(t, 0)
        mn, mx = min_values.get(t), max_values.get(t)
        range_str = f"{mn} to {mx}" if mn is not None and mx is not None and mn != mx else ""
        val_str = str(val)
        if len(val_str) > 20:
            val_str = val_str[:20] + '…'
        print(f"{t:<55} {val_str:<22} {cc}/{n_polls:<8} {range_str}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--plc-ip', default=PLC_IP)
    ap.add_argument('--duration-sec', type=float, default=120)
    ap.add_argument('--poll-interval', type=float, default=2.0)
    args = ap.parse_args()

    print(f"Connecting to {args.plc_ip}...")
    with LogixDriver(args.plc_ip) as plc:
        if not plc.connected:
            print("ERROR: could not connect.")
            return
        print(f"Connected. Controller: {plc.info.get('product_name','unknown')}\n")

        last_values, change_counts, min_values, max_values, n_polls, running_polls = \
            run_check(plc, ALL_TAGS, args.duration_sec, args.poll_interval)

        running_pct = 100*running_polls/n_polls if n_polls else 0
        print(f"\nSample complete — {n_polls} polls, {running_pct:.0f}% RUNNING "
              f"({running_polls}/{n_polls})")
        if running_pct < 50:
            print("WARNING: machine was not consistently running during this sample — "
                  "results below may be inconclusive, re-run during steadier production.")

        report("QUESTION 1: Active/Reactive Current Cluster (servoPuller_Axis)",
                CURRENT_CLUSTER, last_values, change_counts, min_values, max_values, n_polls)

        report("QUESTION 2: servoTable_Axis Sweep — is the struct alive?",
                TABLE_AXIS_SWEEP, last_values, change_counts, min_values, max_values, n_polls)

        # Quick automated verdicts
        print(f"\n{'='*100}\nQUICK VERDICT\n{'='*100}")
        ref_alive = change_counts.get('servoPuller_Axis.TorqueReference', 0) > 0
        ref_dead = change_counts.get('servoPuller_Axis.OutputCurrent', 0) == 0
        print(f"Reference check — TorqueReference changed: {ref_alive}, "
              f"OutputCurrent stayed at 0: {ref_dead}  "
              f"({'sample is valid for comparison' if ref_alive and ref_dead else 'CAUTION: reference tags behaved differently than last time — machine state may differ'})")

        active_reactive_alive = any(
            change_counts.get(t, 0) > 0 for t in CURRENT_CLUSTER
            if t not in ('servoPuller_Axis.TorqueReference', 'servoPuller_Axis.OutputCurrent')
        )
        print(f"Active/Reactive current cluster: "
              f"{'AT LEAST ONE TAG CHANGED — promote the changing ones to YES' if active_reactive_alive else 'all flat — confirmed dead, same as OutputCurrent'}")

        table_axis_tags_only = [t for t in TABLE_AXIS_SWEEP
                                  if t.startswith('servoTable_Axis.')
                                  and t not in ('servoTable_Axis.ActualVelocity',
                                                'servoTable_Axis.ActualPosition',
                                                'servoTable_Axis.VelocityFeedback')]
        table_axis_any_alive = any(change_counts.get(t, 0) > 0 for t in table_axis_tags_only)
        already_known_alive = any(
            change_counts.get(t, 0) > 0 for t in
            ('servoTable_Axis.ActualVelocity', 'servoTable_Axis.ActualPosition',
             'servoTable_Axis.VelocityFeedback')
        )
        rpm_alive = change_counts.get('Program:P01_TableDrive.ServoStatus.Motor_RPM', 0) > 0

        print(f"\nservoTable_Axis already-collected motion tags moving: {already_known_alive}")
        print(f"Program:P01_TableDrive.ServoStatus.Motor_RPM moving: {rpm_alive}")
        print(f"Other servoTable_Axis fields (Torque/Current/Position/Velocity error) moving: "
              f"{table_axis_any_alive}")

        if already_known_alive and rpm_alive and not table_axis_any_alive:
            print("\n>>> CONFIRMED: servoTable_Axis's basic motion tags (Velocity/Position) work "
                  "fine, but every OTHER field on this struct (Torque, Current, PositionError, "
                  "VelocityError) is dead even while the table is confirmed moving via "
                  "Program:P01_TableDrive.ServoStatus.Motor_RPM. This is NOT 'the whole struct "
                  "is disconnected' — ActualVelocity/ActualPosition/VelocityFeedback prove the "
                  "axis mapping itself is correct. It means this drive/AOI configuration simply "
                  "doesn't populate the torque/current/error diagnostic members on the table "
                  "axis specifically (possibly a different drive type or control mode than the "
                  "puller, which DOES populate TorqueReference). Treat remaining servoTable_Axis.* "
                  "NO classifications as confirmed, with this as the documented reason — not "
                  "'low relevance pattern' but 'confirmed empty on this drive.'")
        elif table_axis_any_alive:
            print("\n>>> At least one additional servoTable_Axis field is alive — check the "
                  "table above for which one(s) and consider promoting to YES.")


if __name__ == '__main__':
    main()
