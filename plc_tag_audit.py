import os
import pandas as pd
from pycomm3 import LogixDriver

# --- CONFIGURATION ---
PLC_IP_ADDRESS = "192.168.1.102"
MASTER_REGISTRY_FILENAME = "Master_PLC_Tag_Dictionary.xlsx"
AUDIT_REPORT_FILENAME = "PLC_Tag_Comparison_Report.xlsx"

def fetch_live_tags(ip_address):
    """
    Connects to the Rockwell PLC over EtherNet/IP and discovers
    all controller tags using pycomm3's get_tag_list().
    """
    print(f"Connecting to PLC at {ip_address}...")
    tag_list = []

    with LogixDriver(ip_address) as plc:
        tags = plc.get_tag_list()
        for tag in tags:
            if isinstance(tag, dict):
                # Extract just the type name — data_type can be a complex struct dict
                dt = tag.get("data_type", "UNKNOWN")
                dt_name = tag.get("data_type_name") or (dt.get("name") if isinstance(dt, dict) else str(dt))
                tag_list.append({
                    "Tag Name":   tag.get("tag_name", ""),
                    "Data Type":  dt_name or "UNKNOWN",
                    "Dimensions": str(tag.get("dim", 0)),
                })
            else:
                tag_list.append({
                    "Tag Name":   str(tag),
                    "Data Type":  "UNKNOWN",
                    "Dimensions": "0",
                })

    print(f"Successfully discovered {len(tag_list)} live tags from the controller.")
    return pd.DataFrame(tag_list)


def main():
    # --- STEP 1: FETCH LIVE TAGS ---
    try:
        live_df = fetch_live_tags(PLC_IP_ADDRESS)
    except Exception as e:
        print(f"Could not connect to live PLC ({e}).")
        print("Attempting fallback to local plc_tags.csv snapshot...")
        if os.path.exists("plc_tags.csv"):
            raw_csv = pd.read_csv("plc_tags.csv")
            live_df = raw_csv[['tag_name', 'data_type']].copy()
            live_df.rename(columns={'tag_name': 'Tag Name', 'data_type': 'Data Type'}, inplace=True)
            live_df['Tag Name'] = live_df['Tag Name'].str.strip()
            live_df['Dimensions'] = "0"
            print(f"Loaded {len(live_df)} tags from plc_tags.csv fallback.")
        else:
            print("Error: No live connection or local fallback 'plc_tags.csv' found.")
            return

    # --- STEP 2: CREATE BASELINE IF DOESN'T EXIST ---
    if not os.path.exists(MASTER_REGISTRY_FILENAME):
        print(f"\nBaseline file not found. Creating: {MASTER_REGISTRY_FILENAME}")
        baseline_df = live_df.copy()
        baseline_df["Functional Section"] = "UNASSIGNED"
        baseline_df["What It Means"]      = "Awaiting description"
        baseline_df["Collected?"]          = "NO"
        baseline_df["Log Location"]        = ""
        baseline_df["Why / Why Not"]       = ""
        baseline_df.to_excel(MASTER_REGISTRY_FILENAME, index=False)
        print(f"Created {MASTER_REGISTRY_FILENAME} with {len(baseline_df)} tags.")
        print("Fill in the documentation columns and re-run to compare against future PLC snapshots.")
        return

    # --- STEP 3: COMPARE LIVE TAGS TO MASTER ---
    print(f"\nLoading master: {MASTER_REGISTRY_FILENAME}")
    master_df = pd.read_excel(MASTER_REGISTRY_FILENAME)
    master_df['Tag Name'] = master_df['Tag Name'].astype(str).str.strip()
    live_df['Tag Name']   = live_df['Tag Name'].astype(str).str.strip()

    # New tags in PLC not in Excel
    new_tags = live_df[~live_df['Tag Name'].isin(master_df['Tag Name'])].copy()
    new_tags['Comparison Status'] = "NEW TAG — Missing from documentation"

    # Orphaned tags in Excel not in PLC
    deleted_tags = master_df[~master_df['Tag Name'].isin(live_df['Tag Name'])].copy()
    deleted_tags['Comparison Status'] = "ORPHANED — In Excel but not in PLC"

    # Data type mismatches
    merged = pd.merge(live_df, master_df, on="Tag Name", suffixes=('_live', '_master'))
    if 'Data Type_live' in merged.columns and 'Data Type_master' in merged.columns:
        mismatches = merged[merged['Data Type_live'] != merged['Data Type_master']].copy()
        mismatches['Comparison Status'] = "TYPE MISMATCH — Data type changed in PLC"
        mismatches['Data Type'] = mismatches['Data Type_live'] + " (was: " + mismatches['Data Type_master'] + ")"
    else:
        mismatches = pd.DataFrame()

    # --- STEP 4: BUILD REPORT ---
    print("\n--- AUDIT RESULTS ---")
    print(f"  New tags (not in Excel):     {len(new_tags)}")
    print(f"  Orphaned tags (not in PLC):  {len(deleted_tags)}")
    print(f"  Data type mismatches:        {len(mismatches)}")

    audit_slices = []
    for df, label in [(new_tags, 'new'), (deleted_tags, 'orphaned'), (mismatches, 'mismatch')]:
        if not df.empty:
            clean = df.copy()
            for col in ['Functional Section', 'What It Means', 'Collected?', 'Log Location', 'Why / Why Not']:
                if col not in clean.columns:
                    clean[col] = ""
            keep = ['Comparison Status', 'Tag Name', 'Data Type',
                    'What It Means', 'Collected?', 'Log Location']
            audit_slices.append(clean[[c for c in keep if c in clean.columns]])

    if audit_slices:
        report_df = pd.concat(audit_slices, ignore_index=True)
        with pd.ExcelWriter(AUDIT_REPORT_FILENAME, engine='openpyxl') as writer:
            report_df.to_excel(writer, sheet_name="Discrepancies", index=False)
            live_df.to_excel(writer, sheet_name="Live PLC Snapshot", index=False)
        print(f"\nReport saved: {AUDIT_REPORT_FILENAME}")
    else:
        print("\nPerfect sync — Excel matches PLC exactly.")

if __name__ == "__main__":
    main()
