"""
wire_break_mapper.py
Noble Gas Systems — Steeger HS120/48
Step-by-step wire break switch zone mapper.

Usage:
    python wire_break_mapper.py

How it works:
    - Prompts you to push each zone detector one at a time
    - Watches Local:1:I.Data for changes
    - Records which bit fired
    - Prints the final map when done
"""

from pycomm3 import LogixDriver
import time

PLC_IP = '192.168.1.102'
TOTAL_ZONES = 4

def binary_str(val):
    return f'{val:08b}'

def bits_changed(old, new):
    changed = old ^ new
    return [i for i in range(8) if changed & (1 << i)]

def wait_for_change(plc, baseline):
    """Poll until Local:1:I.Data changes from baseline. Returns new value."""
    while True:
        result = plc.read('Local:1:I.Data')
        if result.error is None and result.value != baseline:
            return result.value
        time.sleep(0.2)

def wait_for_reset(plc, baseline):
    """Poll until Local:1:I.Data returns to baseline (lever released)."""
    while True:
        result = plc.read('Local:1:I.Data')
        if result.error is None and result.value == baseline:
            return
        time.sleep(0.2)

print()
print('=' * 55)
print('  Wire Break Zone Mapper')
print('  Noble Gas Systems — Steeger HS120/48')
print('=' * 55)
print()
print('Connecting to PLC...')

try:
    with LogixDriver(PLC_IP) as plc:
        if not plc.connected:
            print('ERROR: Could not connect to PLC')
            exit()

        print(f'Connected to {PLC_IP}')
        print()

        # Get baseline
        baseline_result = plc.read('Local:1:I.Data')
        baseline = baseline_result.value
        print(f'Baseline value: {baseline}  (binary: {binary_str(baseline)})')
        print()
        print('Machine must be STOPPED and safe before proceeding.')
        input('Press Enter when ready...')
        print()

        zone_map = {}

        for zone in range(1, TOTAL_ZONES + 1):
            print('-' * 55)
            print(f'  Zone {zone} of {TOTAL_ZONES}')
            print(f'  Push down the detector/lever for zone {zone}...')
            print()

            # Wait for bit change
            new_value = wait_for_change(plc, baseline)
            changed = bits_changed(baseline, new_value)
            fired_bits = [b for b in changed if new_value & (1 << b)]

            if fired_bits:
                bit = fired_bits[0]
                zone_map[zone] = bit
                print(f'  Detected! Bit {bit} fired')
                print(f'  Value: {baseline} -> {new_value}  (binary: {binary_str(new_value)})')
            else:
                print(f'  Change detected but no new bits fired.')
                print(f'  Value: {baseline} -> {new_value}')
                zone_map[zone] = None

            print()
            print('  Release the lever...')
            wait_for_reset(plc, baseline)
            print('  Released.')
            print()

            if zone < TOTAL_ZONES:
                input(f'  Press Enter to continue to zone {zone + 1}...')
            print()

        # Results
        print('=' * 55)
        print('  MAPPING COMPLETE')
        print('=' * 55)
        print()
        print('Zone results:')
        for zone, bit in zone_map.items():
            print(f'  Zone {zone} -> Bit {bit}')

        print()
        print('Python map for braider_monitor.py:')
        print()
        print('WIRE_BREAK_ZONE_MAP = {')
        for zone, bit in zone_map.items():
            print(f"    {bit}: 'Zone {zone}',")
        print('}')
        print()
        print('Copy this into braider_monitor.py and braider_analysis.py')

except KeyboardInterrupt:
    print()
    print('Mapping cancelled.')
except Exception as e:
    print(f'Error: {e}')
