"""
Read the actual integer value of every named state constant tag directly
from the PLC (OFF, ON, READY, STOPPED, STARTING, RUNNING, STOPPING,
PAUSING, PAUSED, ABORTING, ABORTED).

These are apparently independent DINT tags on the controller (not members
of an enum/UDT), so we can just read each one's current value directly —
no need to wait for the machine to actually be in that state.

Usage:
    python3 read_state_constants.py 192.168.1.102
"""

import sys
from pycomm3 import LogixDriver

STATE_TAG_NAMES = [
    'OFF', 'ON', 'READY', 'STOPPED', 'STARTING', 'RUNNING',
    'STOPPING', 'PAUSING', 'PAUSED', 'ABORTING', 'ABORTED',
]


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 read_state_constants.py <PLC_IP>")
        sys.exit(1)

    plc_ip = sys.argv[1]

    print(f"Connecting to PLC at {plc_ip}...")
    with LogixDriver(plc_ip) as plc:
        if not plc.connected:
            print("FAILED to connect.")
            sys.exit(1)
        print("Connected.\n")

        print("=" * 50)
        print(f"{'Tag Name':<12} {'Value':<8}")
        print("=" * 50)

        results = plc.read(*STATE_TAG_NAMES)
        # pycomm3 returns a single result if one tag, a list if multiple
        if not isinstance(results, list):
            results = [results]

        value_map = {}
        for r in results:
            print(f"{r.tag:<12} {r.value}")
            value_map[r.tag] = r.value

        print("\n" + "=" * 50)
        print("Python dict you can paste back, sorted by value:")
        print("=" * 50)
        sorted_items = sorted(value_map.items(), key=lambda kv: (kv[1] is None, kv[1]))
        print("STATE_CODES = {")
        for name, val in sorted_items:
            print(f"    {val}: '{name}',")
        print("}")

    print("\nDone.")


if __name__ == '__main__':
    main()
