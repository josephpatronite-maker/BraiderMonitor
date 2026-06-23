"""
probe_struct_tags.py
====================
Automatically finds ALL struct/complex tags in the PLC and probes
their sub-tags using dot notation to find what's actually readable.
Run on Pi: python3 probe_struct_tags.py
"""

from pycomm3 import LogixDriver

PLC_IP = '192.168.1.102'

# ── Simple atomic types — skip these, pycomm3 reads them fine already ─────────
ATOMIC_TYPES = {
    'BOOL', 'DINT', 'REAL', 'SINT', 'INT', 'LINT',
    'UDINT', 'UINT', 'USINT', 'ULINT', 'LREAL',
    'STRING', 'TIMER', 'COUNTER', 'CONTROL',
}

# ── Sub-tag candidates — covers most Rockwell motion/drive types ──────────────
COMMON_SUBTAGS = [
    # Motion axis
    'ActualPosition', 'ActualVelocity', 'ActualAcceleration',
    'ActualTorque', 'ActualCurrent', 'CommandTorque', 'CommandVelocity',
    'CommandPosition', 'FollowingError', 'PositionError', 'VelocityError',
    'DriveTemp', 'MotorTemp', 'BusVoltage', 'DCBusVoltage',
    'OutputCurrent', 'OutputVoltage', 'OutputPower', 'OutputFrequency',
    'OutputFreq', 'FreqCommand',
    'AxisFault', 'AxisWarning', 'AxisState', 'MotionStatus',
    'FaultCode', 'WarningCode', 'StoppingFault', 'ConfigFault',
    'InPosition', 'InProfile', 'MotionFault', 'DriveFault',
    'DriveEnableStatus', 'ServoActionStatus',
    'HardOvertravelFault', 'SoftOvertravelFault',
    'EncoderFault', 'GroundFault', 'OvercurrentFault',
    'OvervoltageFault', 'UndervoltageFault', 'OvertemperatureFault',
    'TorqueReferenceLimited', 'VelocityReferenceLimited',
    'MoveStatus', 'HomeStatus', 'StopStatus', 'AccelStatus', 'DecelStatus',
    # VFD / PowerFlex
    'AtReference', 'Faulted', 'Active', 'Ready',
    'RunningForward', 'RunningReverse',
    'TorqueActual', 'TorqueCommand',
    'AccumKwh', 'RunHours',
    # Motion group
    'GroupFault', 'GroupStatus', 'GroupState', 'CoarseUpdatePeriod',
    # Ethernet / comms
    'Data', 'ConnectionStatus', 'ConnectionsFaulted',
    'InputConnectionsOpened', 'OutputConnectionsOpened',
    # Diagnostics
    'DiagFaultCode', 'DiagWarningCode',
    'PositionFeedback', 'VelocityFeedback', 'TorqueFeedback',
    'MotorCapacity', 'DriveCapacity', 'ConverterCapacity',
    'MotorElecFreq', 'MotorVoltage',
    'RotorFluxAngle', 'StatorFlux',
]

def get_struct_tags(plc):
    """Return list of (tag_name, type_name) for all non-atomic tags."""
    struct_tags = []
    raw = plc.get_tag_list()
    for t in raw:
        if not isinstance(t, dict):
            continue
        tag_name  = t.get('tag_name', '')
        dt        = t.get('data_type', {})
        type_name = t.get('data_type_name', '')
        if not type_name:
            type_name = dt.get('name', '') if isinstance(dt, dict) else str(dt)

        # Skip atomic types
        base_type = type_name.split(':')[0].upper()
        if base_type in ATOMIC_TYPES:
            continue
        if not tag_name or not type_name:
            continue

        struct_tags.append((tag_name, type_name))

    return sorted(struct_tags, key=lambda x: x[0])


def probe_tag(plc, base_tag, subtags, batch_size=40):
    """Try to read sub-tags in batches, return dict of readable ones."""
    readable = {}
    for i in range(0, len(subtags), batch_size):
        batch     = subtags[i:i + batch_size]
        full_tags = [f'{base_tag}.{sub}' for sub in batch]
        try:
            results = plc.read(*full_tags)
            if not isinstance(results, (list, tuple)):
                results = [results]
            for r in results:
                if r and r.error is None and r.value is not None:
                    sub = r.tag.replace(f'{base_tag}.', '')
                    readable[sub] = r.value
        except Exception:
            # Fall back to one-by-one
            for sub in batch:
                try:
                    r = plc.read(f'{base_tag}.{sub}')
                    if r and r.error is None and r.value is not None:
                        readable[sub] = r.value
                except Exception:
                    pass
    return readable


def main():
    print('Struct Tag Prober — Auto Discovery')
    print('===================================')
    print(f'PLC: {PLC_IP}\n')

    with LogixDriver(PLC_IP) as plc:

        # Step 1 — find all struct tags
        struct_tags = get_struct_tags(plc)
        print(f'Found {len(struct_tags)} struct/complex tags (non-atomic):')
        for name, dtype in struct_tags:
            print(f'  {name:<40} [{dtype}]')
        print()

        # Step 2 — probe each one
        all_results = {}
        for tag_name, type_name in struct_tags:
            print(f'Probing {tag_name} [{type_name}]...')
            readable = probe_tag(plc, tag_name, COMMON_SUBTAGS)
            all_results[tag_name] = (type_name, readable)
            if readable:
                print(f'  ✓ {len(readable)} readable:')
                for sub, val in sorted(readable.items()):
                    print(f'    .{sub:<35} = {val}')
            else:
                print(f'  ✗ Nothing readable with common sub-tags')
            print()

    # ── Summary ──────────────────────────────────────────────────────────────
    print('='*60)
    print('SUMMARY — All readable struct sub-tags')
    print('='*60)

    total_readable = 0
    for tag_name, (type_name, readable) in sorted(all_results.items()):
        if readable:
            print(f'\n{tag_name} [{type_name}] — {len(readable)} sub-tags:')
            for sub, val in sorted(readable.items()):
                print(f'  → {tag_name}.{sub:<40} = {val}')
            total_readable += len(readable)

    no_result = [n for n, (_, r) in all_results.items() if not r]
    if no_result:
        print(f'\nNo readable sub-tags found in ({len(no_result)} tags):')
        for n in no_result:
            print(f'  {n}')

    print(f'\nTotal readable struct sub-tags: {total_readable}')
    print('Add valuable ones to FAST_TAGS in braider_monitor.py')


if __name__ == '__main__':
    main()
