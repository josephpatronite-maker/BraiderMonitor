import os
import json
import pandas as pd
from pycomm3 import LogixDriver

# --- CONFIGURATION ---
PLC_IP_ADDRESS = "192.168.1.102"  # Replace with your actual Braider PLC IP Address
MASTER_REGISTRY_FILENAME = "Master_PLC_Tag_Dictionary.xlsx"
AUDIT_REPORT_FILENAME = "PLC_Tag_Comparison_Report.xlsx"

def fetch_live_tags(ip_address):
    """
    Step 1: Connects to the Rockwell PLC over EtherNet/IP and discovers
    all program and controller tags, matching the format found in plc_tags.csv.
    """
    print(f"Connecting to PLC at {ip_address}...")
    tag_list = []
    
    with LogixDriver(ip_address) as plc:
        # Discover all tags available in the controller
        plc.discover_tags()
        
        for tag_name, tag_info in plc.tags.items():
            # Filter out internal/system clutter if necessary, keeping core structures
            tag_data = {
                "Tag Name": tag_name.strip(),
                "Data Type": tag_info.get("data_type", "UNKNOWN"),
                "Dimensions": str(tag_info.get("dim", 0))
            }
            tag_list.append(tag_data)
            
    print(f"Successfully discovered {len(tag_list)} live tags from the controller.")
    return pd.DataFrame(tag_list)


def main():
    # --- STEP 1 & 2: FETCH LIVE TAGS & EXPORT BASELINE ---
    try:
        live_df = fetch_live_tags(PLC_IP_ADDRESS)
    except Exception as e:
        print(f"Could not connect to live PLC ({e}). Attempting fallback to local plc_tags.csv snapshot...")
        if os.path.exists("plc_tags.csv"):
            raw_csv = pd.read_csv("plc_tags.csv")
            live_df = raw_csv[['tag_name', 'data_type']].copy()
            live_df.rename(columns={'tag_name': 'Tag Name', 'data_type': 'Data Type'}, inplace=True)
            live_df['Tag Name'] = live_df['Tag Name'].str.strip()
            live_df['Dimensions'] = "0"
        else:
            print("Error: No live connection or local fallback 'plc_tags.csv' found.")
            return

    # Create baseline file if it does not exist yet
    if not os.path.exists(MASTER_REGISTRY_FILENAME):
        print(f"Baseline file not found. Creating a new one: {MASTER_REGISTRY_FILENAME}")
        # Initialize placeholders for descriptions and telemetry rules
        baseline_df = live_df.copy()
        baseline_df["Functional Section"] = "UNASSIGNED"
        baseline_df["What It Means"] = "Awaiting operational description"
        baseline_df["Collected?"] = "NO"
        baseline_df["Log Location"] = "N/A"
        
        baseline_df.to_excel(MASTER_REGISTRY_FILENAME, index=False)
        print("Baseline created. Fill in documentation details directly inside this Excel sheet.")
        return

    # --- STEP 3: COMPARE LIVE TAGS TO THE CURRENT EXCEL MASTER ---
    print(f"Loading master tracking sheets for comparison analysis...")
    master_df = pd.read_excel(MASTER_REGISTRY_FILENAME)
    
    # Ensure string parity for reliable joins
    master_df['Tag Name'] = master_df['Tag Name'].astype(str).str.strip()
    live_df['Tag Name'] = live_df['Tag Name'].astype(str).str.strip()

    # Find completely new tags added to the PLC that don't exist in our Excel sheet
    new_tags = live_df[~live_df['Tag Name'].isin(master_df['Tag Name'])].copy()
    if not new_tags.empty:
        new_tags['Comparison Status'] = "💥 NEW TAG (Missing from Documentation)"
    
    # Find tags that were deleted from the PLC logic but are still sitting in our Excel sheet
    deleted_tags = master_df[~master_df['Tag Name'].isin(live_df['Tag Name'])].copy()
    if not deleted_tags.empty:
        deleted_tags['Comparison Status'] = "⚠️ ORPHANED TAG (Exists in Excel but deleted from PLC)"

    # Find tags where the engineering data type changed in a software update
    merged_matched = pd.merge(live_df, master_df, on="Tag Name", suffixes=('_live', '_master'))
    type_mismatches = merged_matched[merged_matched['Data Type_live'] != merged_matched['Data Type_master']].copy()
    if not type_mismatches.empty:
        type_mismatches['Comparison Status'] = "🔄 TYPE MISMATCH (Data type modified in PLC code)"
        # Update columns to show live conflict values
        type_mismatches['Data Type'] = type_mismatches['Data Type_live'] + " (Was: " + type_mismatches['Data Type_master'] + ")"

    # Combine results into an Audit Report Sheet
    audit_slices = []
    for df in [new_tags, deleted_tags, type_mismatches]:
        if not df.empty:
            # Standardize columns to merge seamlessly
            clean_slice = df.copy()
            for col in ['Functional Section', 'What It Means', 'Collected?', 'Log Location']:
                if col not in clean_slice.columns:
                    clean_slice[col] = "N/A"
            audit_slices.append(clean_slice[['Comparison Status', 'Tag Name', 'Data Type', 'What It Means', 'Collected?']])

    print("\n--- AUDIT RESULTS SUMMARY ---")
    if audit_slices:
        report_df = pd.concat(audit_slices, ignore_index=True)
        print(report_df['Comparison Status'].value_counts())
        
        # Save a dedicated report spreadsheet for review
        with pd.ExcelWriter(AUDIT_REPORT_FILENAME, engine='openpyxl') as writer:
            report_df.to_excel(writer, sheet_name="Tag Discrepancies", index=False)
            live_df.to_excel(writer, sheet_name="Full Live Snapshot", index=False)
            
        print(f"\nDiscrepancies found! Report generated: '{AUDIT_REPORT_FILENAME}'")
    else:
        print(" Perfect Sync! Your master tracking Excel guide completely matches the live PLC code configuration.")

if __name__ == "__main__":
    main()