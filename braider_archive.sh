#!/bin/bash
# /home/pi/braider_archive.sh
#
# Handles weekly (process_log) and monthly (event_log, oee_log, wire_break_log)
# log archiving for whichever braider this Pi is running.
#
# Called by cron — see crontab entries at the bottom of this file.
# Run as user pi so file ownership stays consistent with the monitoring script.
#
# This script replaces the in-process archiver thread (_run_archive_checks /
# independent_archiver_loop) in braider1_monitor.py and braider2_monitor.py.
# Those threads should be disabled once this script is deployed, to avoid
# any possibility of a race condition between cron and the in-process thread
# both attempting to rename the same file.
#
# ARCHIVE NAMING: identical to the in-process archiver convention:
#   <base>_archived_<YYYYMMDD>_<HHMMSS>.csv
# e.g. Braider_2_process_log_archived_20260705_000500.csv
#
# SAFETY: only renames a file if it exists AND is non-empty. If the file is
# missing or empty (e.g. the braider was off all week), the script exits
# cleanly with no error and no archive created — consistent with the original
# in-process behaviour.
#
# LOGGING: appends a timestamped line to ~/braider_logs/archive_cron.log
# for every action taken and every skip, so you can verify it's firing
# without needing to check journalctl.
#
# ─── CRONTAB ENTRIES (add with: crontab -e) ────────────────────────────────
# Run weekly process_log archive every Sunday at 00:05
# 5 0 * * 0 /home/pi/braider_archive.sh weekly >> /home/pi/braider_logs/archive_cron.log 2>&1
#
# Run monthly archive (event, oee, wire_break) on 1st of month at 00:05
# 5 0 1 * * /home/pi/braider_archive.sh monthly >> /home/pi/braider_logs/archive_cron.log 2>&1
# ────────────────────────────────────────────────────────────────────────────

set -euo pipefail

LOG_DIR="/home/pi/braider_logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
NOW=$(date +"%Y-%m-%d %H:%M:%S")
MODE="${1:-}"

# Auto-detect braider ID from whichever process_log file exists
BRAIDER_ID=""
for f in "$LOG_DIR"/Braider_*_process_log.csv; do
    if [[ -f "$f" ]]; then
        fname=$(basename "$f")
        BRAIDER_ID="${fname%%_process_log.csv}"
        break
    fi
done

if [[ -z "$BRAIDER_ID" ]]; then
    echo "[$NOW] ERROR: Could not detect braider ID — no *_process_log.csv found in $LOG_DIR"
    exit 1
fi

archive_file() {
    local filepath="$1"
    local label="$2"

    if [[ ! -f "$filepath" ]]; then
        echo "[$NOW] SKIP $label: file does not exist ($filepath)"
        return
    fi

    if [[ ! -s "$filepath" ]]; then
        echo "[$NOW] SKIP $label: file is empty ($filepath)"
        return
    fi

    local base="${filepath%.csv}"
    local archive_path="${base}_archived_${TIMESTAMP}.csv"
    mv "$filepath" "$archive_path"
    echo "[$NOW] ARCHIVED $label: $(basename "$filepath") → $(basename "$archive_path")"
}

case "$MODE" in
    weekly)
        echo "[$NOW] --- Weekly archive starting (${BRAIDER_ID}) ---"
        archive_file "${LOG_DIR}/${BRAIDER_ID}_process_log.csv" "process_log"
        echo "[$NOW] --- Weekly archive complete ---"
        ;;
    monthly)
        echo "[$NOW] --- Monthly archive starting (${BRAIDER_ID}) ---"
        archive_file "${LOG_DIR}/${BRAIDER_ID}_event_log.csv"      "event_log"
        archive_file "${LOG_DIR}/${BRAIDER_ID}_oee_log.csv"        "oee_log"
        archive_file "${LOG_DIR}/${BRAIDER_ID}_wire_break_log.csv" "wire_break_log"
        echo "[$NOW] --- Monthly archive complete ---"
        ;;
    *)
        echo "[$NOW] ERROR: Unknown mode '${MODE}'. Usage: $0 weekly|monthly"
        exit 1
        ;;
esac
