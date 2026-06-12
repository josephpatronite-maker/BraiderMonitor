"""
probe_program_tags.py
=====================
Discovers all program-scoped tags in MainProgram and P01_TableDrive
that are not visible in the controller-scope tag list.
Run on Pi: python3 probe_program_tags.py
"""

from pycomm3 import LogixDriver

PLC_IP = '192.168.1.102'

# Programs to check — from the systemd log output
PROGRAMS = [
    'MainProgram',
    'P01_TableDrive',
]

# Simple atomic types — these read fine directly
ATOMIC_TYPES = {
    'BOOL', 'DINT', 'REAL', 'SINT', 'INT', 'LINT',
    'UDINT', 'UINT', 'USINT', 'ULINT', 'LREAL', 'STRING',
    'TIMER', 'COUNTER', 'CONTROL',
}

def main():
    print('Program-Scoped Tag Discovery')
    print('============================')
    print(f'PLC: {PLC_IP}\n')

    with LogixDriver(PLC_IP) as plc:

        # Get controller-scope tags for comparison
        controller_tags = set()
        for t in plc.get_tag_list():
            if isinstance(t, dict):
                controller_tags.add(t.get('tag_name', ''))
        print(f'Controller-scope tags: {len(controller_tags)}')
        print()

        all_program_tags = []

        for program in PROGRAMS:
            print(f'Scanning {program}...')
            try:
                prog_tags = plc.get_tag_list(program=program)
                print(f'  Found {len(prog_tags)} tags')

                for t in prog_tags:
                    if not isinstance(t, dict):
                        continue
                    tag_name  = t.get('tag_name', '')
                    dt        = t.get('data_type', {})
                    type_name = t.get('data_type_name', '')
                    if not type_name:
                        type_name = dt.get('name', '') if isinstance(dt, dict) else str(dt)

                    full_name = f'Program:{program}.{tag_name.split(".")[-1] if "." in tag_name else tag_name}'
                    all_program_tags.append((full_name, tag_name, type_name, program))

            except Exception as e:
                print(f'  Error: {e}')
            print()

        # Print all program tags
        print(f'=' * 60)
        print(f'All program-scoped tags ({len(all_program_tags)} total):')
        print(f'=' * 60)

        atomic_tags  = []
        struct_tags  = []

        for full_name, tag_name, type_name, program in sorted(all_program_tags):
            base_type = type_name.split(':')[0].upper()
            if base_type in ATOMIC_TYPES:
                atomic_tags.append((full_name, type_name))
            else:
                struct_tags.append((full_name, type_name))

        print(f'\nAtomic tags ({len(atomic_tags)}) — readable directly:')
        for full_name, type_name in atomic_tags:
            print(f'  {full_name:<60} [{type_name}]')

        print(f'\nStruct/complex tags ({len(struct_tags)}) — need sub-tag probing:')
        for full_name, type_name in struct_tags:
            print(f'  {full_name:<60} [{type_name}]')

        # Try reading all atomic program tags
        print(f'\n' + '=' * 60)
        print('Reading all atomic program tags...')
        print('=' * 60)

        if atomic_tags:
            tag_names = [t[0] for t in atomic_tags]
            # Read in batches of 40
            for i in range(0, len(tag_names), 40):
                batch = tag_names[i:i+40]
                try:
                    results = plc.read(*batch)
                    if not isinstance(results, (list, tuple)):
                        results = [results]
                    for r in results:
                        if r and r.error is None:
                            print(f'  {r.tag:<60} = {r.value}')
                        elif r:
                            print(f'  {r.tag:<60} ERROR: {r.error}')
                except Exception as e:
                    print(f'  Batch error: {e}')

        print(f'\nDone. Add useful program tags to FAST_TAGS using Program:ProgramName.TagName syntax.')

if __name__ == '__main__':
    main()
