"""
braider_analysis.py
Noble Gas Systems — Steeger HS120/48
Run this script to generate interactive charts from your log files.
All charts are saved to a single scrollable HTML file that opens in your browser.

Usage:
    python braider_analysis.py
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.io import to_html
import os

figs = []  # collect all figures

# ── Config ────────────────────────────────────────────────────────────────────
LOG_DIR = os.path.expanduser('~/braider_logs')

# Set to 'Braider_1', 'Braider_2', or 'All' to analyze both
BRAIDER_FILTER = 'Braider_2'

# ── Load files ────────────────────────────────────────────────────────────────
print('Loading log files...')
def load_csv(filename, **kwargs):
    path = os.path.join(LOG_DIR, filename)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return pd.read_csv(path, **kwargs)
    print(f'  Warning: {filename} not found or empty — skipping.')
    return pd.DataFrame()

# Files that are optional and expected to sometimes not exist
OPTIONAL_LOGS = {'event_log', 'wire_break_log'}

def load_braider_csvs(braider_id):
    """Load CSVs for a specific braider ID."""
    prefix = f'{braider_id}_' if braider_id != 'All' else ''
    dfs = {}
    for name in ['process_log', 'event_log', 'oee_log', 'wire_break_log']:
        path = os.path.join(LOG_DIR, f'{prefix}{name}.csv')
        if os.path.exists(path) and os.path.getsize(path) > 0:
            dfs[name] = pd.read_csv(path, parse_dates=['Timestamp'])
        else:
            if name not in OPTIONAL_LOGS:
                print(f'  Warning: {prefix}{name}.csv not found or empty — skipping.')
            dfs[name] = pd.DataFrame()
    return dfs

if BRAIDER_FILTER == 'All':
    # Load and combine both braiders
    frames = {name: [] for name in ['process_log', 'event_log', 'oee_log', 'wire_break_log']}
    for bid in ['Braider_1', 'Braider_2']:
        d = load_braider_csvs(bid)
        for name, df in d.items():
            if not df.empty:
                if 'Braider_ID' not in df.columns:
                    df['Braider_ID'] = bid
                frames[name].append(df)
    process     = pd.concat(frames['process_log'])     if frames['process_log']     else pd.DataFrame()
    events      = pd.concat(frames['event_log'])       if frames['event_log']       else pd.DataFrame()
    oee         = pd.concat(frames['oee_log'])         if frames['oee_log']         else pd.DataFrame()
    wire_breaks = pd.concat(frames['wire_break_log'])  if frames['wire_break_log']  else pd.DataFrame()
else:
    d = load_braider_csvs(BRAIDER_FILTER)
    process     = d['process_log']
    events      = d['event_log']
    oee         = d['oee_log']
    wire_breaks = d['wire_break_log']

if process.empty:
    print('ERROR: No process log found — make sure braider_monitor.py is running.')
    exit()

print(f'Braider filter: {BRAIDER_FILTER}')

running       = process[process['Machine_State'] == 16].copy() if not process.empty else pd.DataFrame()
wb_events     = events[events['Event'] == 'WIRE_BREAK'].copy() if not events.empty else pd.DataFrame()
state_changes = events[events['Event'] == 'STATE_CHANGE'].copy() if not events.empty else pd.DataFrame()

print(f'Process log:   {len(process):,} rows  ({process.Timestamp.min()} to {process.Timestamp.max()})')
print(f'Running rows:  {len(running):,}')
print(f'Wire breaks:   {len(wb_events)}')
print(f'State changes: {len(state_changes)}')
print(f'Recipes seen:  {list(process.Recipe_Name.unique())}')
print()


# ── Helper — add wire break lines to any figure ───────────────────────────────
def add_wb_lines(fig, row=None, col=None):
    for _, wb in wb_events.iterrows():
        kwargs = dict(line_color='red', line_dash='dash', line_width=1.5)
        if row is not None:
            kwargs['row'] = row
            kwargs['col'] = col
        fig.add_vline(x=str(wb['Timestamp']), **kwargs)


# ── Helper — add running/stopped background shading ──────────────────────────
def add_state_shading(fig, rows=None):
    """
    Adds green shading when RUNNING, dark shading when stopped/off.
    rows: list of row numbers for subplots, or None for single-axis figures.
    """
    if process.empty:
        return

    # Build state segments — find transitions
    states = process[['Timestamp', 'Machine_State']].copy().reset_index(drop=True)
    segments = []
    start = states.iloc[0]

    for i in range(1, len(states)):
        if states.iloc[i]['Machine_State'] != start['Machine_State']:
            segments.append({
                'start': start['Timestamp'],
                'end':   states.iloc[i]['Timestamp'],
                'state': start['Machine_State']
            })
            start = states.iloc[i]
    # Last segment
    segments.append({
        'start': start['Timestamp'],
        'end':   states.iloc[-1]['Timestamp'],
        'state': start['Machine_State']
    })

    for seg in segments:
        if seg['state'] == 16:
            color, opacity = '#66bb6a', 0.15   # green — running (more visible)
        elif seg['state'] in (1,):
            color, opacity = '#90a4ae', 0.18   # lighter grey — off (more contrast)
        else:
            color, opacity = '#ef5350', 0.12   # red tint — stopped/fault

        kwargs = dict(
            x0=str(seg['start']), x1=str(seg['end']),
            fillcolor=color, opacity=opacity,
            line_width=0, layer='below'
        )
        if rows:
            for row in rows:
                fig.add_vrect(row=row, col=1, **kwargs)
        else:
            fig.add_vrect(**kwargs)


# ── Chart 1 — Speed Overview ──────────────────────────────────────────────────
print('Building chart 1: Speed Overview...')
fig1 = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    subplot_titles=('Table Speed (rev/s)', 'Puller Speed (in/s)', 'Speed Ratio (puller ÷ table)'),
    vertical_spacing=0.06
)

# Resample to 2s so gaps when machine is off break the line visually
running_gapped = running.set_index('Timestamp')[['Table_Speed','Puller_Speed','Speed_Ratio']].resample('2s').mean().reset_index()

fig1.add_trace(go.Scatter(
    x=running_gapped['Timestamp'], y=running_gapped['Table_Speed'],
    mode='lines', name='Table Speed',
    line=dict(color='#4fc3f7', width=1),
    connectgaps=False
), row=1, col=1)

fig1.add_trace(go.Scatter(
    x=running_gapped['Timestamp'], y=running_gapped['Puller_Speed'],
    mode='lines', name='Puller Speed',
    line=dict(color='#81c784', width=1),
    connectgaps=False
), row=2, col=1)

fig1.add_trace(go.Scatter(
    x=running_gapped['Timestamp'], y=running_gapped['Speed_Ratio'],
    mode='lines', name='Speed Ratio',
    line=dict(color='#ffb74d', width=1),
    connectgaps=False
), row=3, col=1)

add_state_shading(fig1, rows=[1, 2, 3])
for row in [1, 2, 3]:
    add_wb_lines(fig1, row=row, col=1)

fig1.update_layout(height=700, title='Speed Overview', template='plotly_dark')
figs.append((fig1, "Chart 1"))


# ── Chart 2 — Speed Ratio Anomaly Detection ───────────────────────────────────
print('Building chart 2: Speed Ratio Anomaly Detection...')
ratio = running[['Timestamp', 'Speed_Ratio']].dropna()
mean  = ratio['Speed_Ratio'].mean()
std   = ratio['Speed_Ratio'].std()
upper = mean + 3 * std
lower = mean - 3 * std
anomalies = ratio[(ratio['Speed_Ratio'] > upper) | (ratio['Speed_Ratio'] < lower)]

print(f'  Speed ratio mean:  {mean:.6f}')
print(f'  Normal band:       {lower:.6f} to {upper:.6f}')
print(f'  Anomalous points:  {len(anomalies)}')

# Resample to 2s intervals — gaps where machine was off become NaN, breaking the line
ratio_gapped = ratio.set_index('Timestamp').resample('2s').mean().reset_index()

fig2 = go.Figure()
fig2.add_trace(go.Scatter(
    x=ratio_gapped['Timestamp'], y=ratio_gapped['Speed_Ratio'],
    mode='lines', name='Speed Ratio',
    line=dict(color='#ffb74d', width=1),
    connectgaps=False
))
fig2.add_trace(go.Scatter(
    x=anomalies['Timestamp'], y=anomalies['Speed_Ratio'],
    mode='markers', name='Anomaly (±3σ)',
    marker=dict(color='red', size=8, symbol='x')
))
ratio_gapped = ratio.copy().set_index('Timestamp').resample('2s').mean().reset_index()
add_state_shading(fig2)
add_wb_lines(fig2)
fig2.add_hline(y=mean, line_color='white', line_dash='dot', line_width=1,
               annotation_text=f'mean={mean:.5f}')
fig2.update_layout(height=450, title='Speed Ratio Anomaly Detection (±3σ band)',
                   template='plotly_dark', yaxis_title='Speed Ratio')
figs.append((fig2, "Chart 2"))


# ── Chart 3 — Machine State Timeline ─────────────────────────────────────────
print('Building chart 3: Machine State Timeline...')
STATE_COLORS = {
    'RUNNING':  '#66bb6a',
    'STOPPED':  '#ef5350',
    'OFF':      '#455a64',
    'PAUSED':   '#ffa726',
    'PAUSING':  '#ffa726',
    'STOPPING': '#ff7043',
    'ABORTING': '#ab47bc',
    'ABORTED':  '#ab47bc',
}

fig3 = go.Figure()
for state, color in STATE_COLORS.items():
    mask = process['State_Name'] == state
    if mask.any():
        fig3.add_trace(go.Scatter(
            x=process.loc[mask, 'Timestamp'],
            y=process.loc[mask, 'State_Name'],
            mode='markers', name=state,
            marker=dict(color=color, size=4, symbol='square'),
        ))
add_state_shading(fig3)
add_wb_lines(fig3)
fig3.update_layout(height=350, title='Machine State Timeline',
                   template='plotly_dark', yaxis_title='State')
figs.append((fig3, "Chart 3"))


# ── Chart 4 — Feet Produced Per Run ──────────────────────────────────────────
print('Building chart 4: Feet Per Run...')

# Find each RUNNING segment — start and end timestamps
# A run = Machine_State transitions to 16, then away from 16
runs = []
in_run = False
run_start = None
run_start_feet = None

for _, row in process.iterrows():
    if row['Machine_State'] == 16 and not in_run:
        in_run = True
        run_start = row['Timestamp']
        run_start_feet = row['Puller_Pos_Feet']
    elif row['Machine_State'] != 16 and in_run:
        in_run = False
        run_end = row['Timestamp']
        run_end_feet = row['Puller_Pos_Feet']
        feet_produced = run_end_feet - run_start_feet if run_end_feet and run_start_feet else 0
        duration_mins = (run_end - run_start).total_seconds() / 60
        # Count wire breaks during this run
        wb_in_run = len(wb_events[
            (wb_events['Timestamp'] >= run_start) &
            (wb_events['Timestamp'] <= run_end)
        ]) if not wb_events.empty else 0
        runs.append({
            'Run':           f'Run {len(runs)+1}',
            'Start':         run_start,
            'End':           run_end,
            'Feet':          round(feet_produced, 2),
            'Duration_Mins': round(duration_mins, 1),
            'Wire_Breaks':   wb_in_run,
        })

# Handle run still in progress at end of log
if in_run:
    last = process.iloc[-1]
    feet_produced = last['Puller_Pos_Feet'] - run_start_feet if run_start_feet else 0
    duration_mins = (last['Timestamp'] - run_start).total_seconds() / 60
    runs.append({
        'Run':           f'Run {len(runs)+1} (in progress)',
        'Start':         run_start,
        'End':           last['Timestamp'],
        'Feet':          round(feet_produced, 2),
        'Duration_Mins': round(duration_mins, 1),
        'Wire_Breaks':   0,
    })

runs_df = pd.DataFrame(runs)

if runs_df.empty:
    print('  No completed runs found yet.')
else:
    print(f'  Found {len(runs_df)} runs')
    for _, r in runs_df.iterrows():
        print(f'  {r["Run"]}: {r["Feet"]} ft in {r["Duration_Mins"]} min, {r["Wire_Breaks"]} wire breaks')

    # Bar color — red if wire break occurred, green otherwise
    bar_colors = ['#ef5350' if wb > 0 else '#66bb6a' for wb in runs_df['Wire_Breaks']]

    fig4 = go.Figure()
    fig4.add_trace(go.Bar(
        x=runs_df['Run'],
        y=runs_df['Feet'],
        marker_color=bar_colors,
        text=[f'{f} ft<br>{d} min<br>{w} WB' for f, d, w in
              zip(runs_df['Feet'], runs_df['Duration_Mins'], runs_df['Wire_Breaks'])],
        textposition='outside',
        name='Feet Produced'
    ))

    fig4.update_layout(
        height=450,
        title='Feet Produced Per Run  |  Green = no wire breaks  |  Red = wire break occurred',
        template='plotly_dark',
        yaxis_title='Feet Produced',
        xaxis_title='Run',
        showlegend=False
    )
    figs.append((fig4, "Chart 4 — Feet Per Run"))


# ── Chart 5b — VFD Frequency Delta ──────────────────────────────────────────
# Difference between commanded and actual VFD frequency
# When motor is under load, actual lags command — deviation = load signal
if 'VFD_Freq_Command' in process.columns and 'VFD_Freq_Actual' in process.columns:
    print('Building chart 5b: VFD Frequency Delta...')
    vfd = running[['Timestamp','VFD_Freq_Command','VFD_Freq_Actual','VFD_Freq_Delta']].dropna()
    vfd_gapped = vfd.set_index('Timestamp').resample('2s').mean().reset_index()

    fig5b = make_subplots(rows=2, cols=1, shared_xaxes=True,
                          subplot_titles=('VFD Frequency — Actual vs Command',
                                          'Frequency Delta (Command - Actual)'),
                          vertical_spacing=0.1)

    fig5b.add_trace(go.Scatter(
        x=vfd_gapped['Timestamp'], y=vfd_gapped['VFD_Freq_Command'],
        mode='lines', name='Command', line=dict(color='#4fc3f7', width=1),
        connectgaps=False
    ), row=1, col=1)

    fig5b.add_trace(go.Scatter(
        x=vfd_gapped['Timestamp'], y=vfd_gapped['VFD_Freq_Actual'],
        mode='lines', name='Actual', line=dict(color='#81c784', width=1),
        connectgaps=False
    ), row=1, col=1)

    fig5b.add_trace(go.Scatter(
        x=vfd_gapped['Timestamp'], y=vfd_gapped['VFD_Freq_Delta'],
        mode='lines', name='Delta', line=dict(color='#ffb74d', width=1.5),
        connectgaps=False
    ), row=2, col=1)

    fig5b.add_hline(y=0, line_color='white', line_dash='dot', line_width=1, row=2, col=1)
    add_state_shading(fig5b, rows=[1, 2])
    add_wb_lines(fig5b, row=1, col=1)
    add_wb_lines(fig5b, row=2, col=1)

    fig5b.update_layout(height=500, title='VFD Load Indicator — Freq Delta',
                        template='plotly_dark')
    figs.append((fig5b, "Chart 5b — VFD Frequency Delta"))
else:
    print('Skipping chart 5b: VFD tags not yet in process log.')


# ── Chart 5 — Axis Sync Flags ─────────────────────────────────────────────────
print('Building chart 5: Axis Sync Flags...')
axis_cols = ['AxisSynced_1','AxisSynced_2','AxisSynced_3','AxisSynced_4','AxisSynced_5']
colors    = ['#4fc3f7','#81c784','#ffb74d','#f06292','#ce93d8']

fig5 = go.Figure()
for col, color in zip(axis_cols, colors):
    if col in process.columns:
        offset = axis_cols.index(col)
        y = process[col].astype(float) + offset
        fig5.add_trace(go.Scatter(
            x=process['Timestamp'], y=y,
            mode='lines', name=col,
            line=dict(color=color, width=1.5)
        ))
add_state_shading(fig5)
add_wb_lines(fig5)
fig5.update_layout(
    height=400, title='Servo Axis Sync Flags (1=synced, 0=lost)',
    template='plotly_dark', yaxis_title='Synced (offset per axis)',
    yaxis=dict(tickvals=[0,1,2,3,4,5,6],
               ticktext=['','OS1','OS2','OS3','OS4','OS5',''])
)
figs.append((fig5, "Chart 5"))


# ── Chart 6 — Wire Break Overlay ─────────────────────────────────────────────
# All wire breaks overlaid on one chart, x-axis = seconds relative to break
# Makes it easy to compare pre-break patterns across events
if len(wb_events) == 0:
    print('No wire break events recorded yet — skipping chart 6.')
else:
    print(f'Building chart 6: Wire Break Overlay ({len(wb_events)} events)...')
    from datetime import timedelta

    BREAK_COLORS = [
        '#ffb74d','#4fc3f7','#81c784','#f06292','#ce93d8',
        '#ff7043','#26c6da','#d4e157','#ab47bc','#ef5350'
    ]

    fig6 = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=(
            'Speed Ratio — all breaks overlaid (x = seconds relative to break)',
            'Table Speed — all breaks overlaid'
        ),
        vertical_spacing=0.1
    )

    for i, (_, wb) in enumerate(wb_events.iterrows()):
        t = wb['Timestamp']
        color = BREAK_COLORS[i % len(BREAK_COLORS)]
        label = f'Break #{i+1} — {t.strftime("%m/%d %H:%M")} | {wb["Puller_Feet"]:.1f} ft'

        window = process[
            (process['Timestamp'] >= t - timedelta(seconds=30)) &
            (process['Timestamp'] <= t + timedelta(seconds=30))
        ].copy()

        if window.empty:
            continue

        # Convert timestamps to seconds relative to break moment
        window['seconds'] = (window['Timestamp'] - t).dt.total_seconds()

        fig6.add_trace(go.Scatter(
            x=window['seconds'], y=window['Speed_Ratio'],
            mode='lines+markers', name=label,
            line=dict(color=color, width=2),
            marker=dict(size=4)
        ), row=1, col=1)

        fig6.add_trace(go.Scatter(
            x=window['seconds'], y=window['Table_Speed'],
            mode='lines', name=label,
            line=dict(color=color, width=1.5),
            showlegend=False
        ), row=2, col=1)

    # Break moment line at x=0
    fig6.add_vline(x=0, line_color='red', line_dash='dash', line_width=2,
                   row=1, col=1)
    fig6.add_vline(x=0, line_color='red', line_dash='dash', line_width=2,
                   row=2, col=1)

    fig6.update_xaxes(title_text='Seconds relative to wire break', row=2, col=1)
    fig6.update_layout(
        height=600,
        title=f'Wire Break Comparison — {len(wb_events)} events  |  Red line = break moment',
        template='plotly_dark'
    )
    figs.append((fig6, "Chart 6 — Wire Break Overlay"))


# ── Chart 6b — Job Progress ──────────────────────────────────────────────────
# Loop count, length to run, vessel completions
if 'Loop_Count' in process.columns and 'Length_To_Run' in process.columns:
    print('Building chart 6b: Job Progress...')
    job = process[['Timestamp','Loop_Count','Length_To_Run','Run_Complete','New_Part']].copy()
    job_gapped = job.set_index('Timestamp').resample('2s').first().reset_index()

    fig6b = go.Figure()

    fig6b.add_trace(go.Scatter(
        x=job_gapped['Timestamp'], y=job_gapped['Length_To_Run'],
        mode='lines', name='Length To Run',
        line=dict(color='#4fc3f7', width=1.5), connectgaps=False
    ))

    # Zero reference line — machine is at target when this crosses zero
    fig6b.add_hline(y=0, line_color='white', line_dash='dot', line_width=1,
                    annotation_text='target reached')

    # Mark vessel completions
    completions = job[job['Run_Complete'] == True] if 'Run_Complete' in job.columns else pd.DataFrame()
    for _, rc in completions.iterrows():
        fig6b.add_vline(x=str(rc['Timestamp']), line_color='#66bb6a',
                        line_dash='dash', line_width=2)

    add_wb_lines(fig6b)

    fig6b.update_layout(height=400,
                        title='Job Progress — Length To Run  |  Green = run complete  |  Red = wire break',
                        template='plotly_dark',
                        yaxis_title='Length To Run (ft)')
    figs.append((fig6b, "Chart 6b — Job Progress"))
else:
    print('Skipping chart 6b: job progress tags not yet in process log.')


# ── Chart 6c — Fault & Wire Break Events Timeline ────────────────────────────
if not events.empty:
    print('Building chart 6c: Fault & Event Timeline...')
    
    all_events = events.copy()
    # Fill NaN in text columns so hover labels show cleanly
    for col in ['Detail', 'To_State', 'From_State', 'Recipe_Name']:
        if col in all_events.columns:
            all_events[col] = all_events[col].fillna('')
    EVENT_COLORS = {
        'STATE_CHANGE':      '#4fc3f7',
        'WIRE_BREAK':        '#ef5350',
        'WIRE_BREAK_CLEARED':'#ffb74d',
    }
    
    fig6c = go.Figure()
    
    for event_type, color in EVENT_COLORS.items():
        mask = all_events['Event'] == event_type
        if mask.any():
            subset = all_events[mask]
            # Build descriptive hover text per event type
            if event_type == 'STATE_CHANGE':
                hover_text = (
                    subset['From_State'] + ' → ' + subset['To_State'] +
                    '<br>Feet: ' + subset['Puller_Feet'].astype(str) +
                    '<br>Recipe: ' + subset['Recipe_Name']
                )
            elif event_type in ('WIRE_BREAK', 'WIRE_BREAK_CLEARED'):
                hover_text = (
                    subset['Detail'] +
                    '<br>Feet: ' + subset['Puller_Feet'].astype(str) +
                    '<br>Recipe: ' + subset['Recipe_Name']
                )
            else:
                hover_text = subset['Detail']

            fig6c.add_trace(go.Scatter(
                x=subset['Timestamp'],
                y=[event_type] * len(subset),
                mode='markers',
                name=event_type,
                marker=dict(color=color, size=12, symbol='diamond'),
                text=hover_text,
                hovertemplate='<b>%{y}</b><br>%{x}<br>%{text}<extra></extra>'
            ))
    
    # Fault events from FAULT_ prefix
    fault_events = all_events[all_events['Event'].str.startswith('FAULT_', na=False)]
    if not fault_events.empty:
        fig6c.add_trace(go.Scatter(
            x=fault_events['Timestamp'],
            y=fault_events['Event'],
            mode='markers',
            name='Fault',
            marker=dict(color='#ab47bc', size=10, symbol='x'),
            text=fault_events.get('Detail', ''),
            hovertemplate='%{x}<br>%{y}<br>%{text}<extra></extra>'
        ))

    fig6c.update_layout(height=400, title='Event Timeline — All Events',
                        template='plotly_dark', yaxis_title='Event Type')
    figs.append((fig6c, "Chart 6c — Event Timeline"))
else:
    print('Skipping chart 6c: no event log yet.')


# ── Chart 6c2 — Safety Flags Timeline ───────────────────────────────────────
safety_cols = ['Door_Ok', 'Estop_Ok', 'Guards_Ok', 'All_Safties_Ok', 'All_Axes_Ok', 'All_Axes_Running']
has_safety = any(c in process.columns for c in safety_cols)

if has_safety:
    print('Building chart 6c2: Safety Flags Timeline...')

    safety_colors = {
        'Door_Ok':          '#4fc3f7',
        'Estop_Ok':         '#81c784',
        'Guards_Ok':        '#ffb74d',
        'All_Safties_Ok':   '#f06292',
        'All_Axes_Ok':      '#ce93d8',
        'All_Axes_Running': '#80cbc4',
    }

    fig6c2 = go.Figure()

    # Add state shading so you can see when machine was running
    add_state_shading(fig6c2)

    for i, (col, color) in enumerate(safety_colors.items()):
        if col in process.columns:
            # Offset each flag slightly so they don't overlap when all True
            offset = i * 0.05
            y = process[col].astype(float) + offset
            fig6c2.add_trace(go.Scatter(
                x=process['Timestamp'],
                y=y,
                mode='lines',
                name=col.replace('_', ' '),
                line=dict(color=color, width=1.5),
                connectgaps=False
            ))

    # Mark transitions where any safety flag goes False (not every False row)
    safety_df = process[safety_cols].copy() if all(c in process.columns for c in safety_cols) else pd.DataFrame()
    if not safety_df.empty:
        any_false = (safety_df == False).any(axis=1)
        # Only mark the moment it transitions from OK to NOT OK
        transitions = any_false & ~any_false.shift(1).fillna(False)
        false_times = process[transitions]['Timestamp']
        for t in false_times[:20]:  # cap at 20 lines max
            fig6c2.add_vline(x=str(t), line_color='#ef5350',
                             line_dash='dot', line_width=1)

    add_wb_lines(fig6c2)

    fig6c2.update_layout(
        height=400,
        title='Safety Flags Timeline  |  1 = OK  |  0 = Not OK  |  Red dots = any flag False',
        template='plotly_dark',
        yaxis=dict(tickvals=[0, 1], ticktext=['NOT OK', 'OK']),
        yaxis_title='Status'
    )
    figs.append((fig6c2, "Chart 6c2 — Safety Flags"))
else:
    print('Skipping chart 6c2: safety flag columns not found.')


# ── Chart 6d — Motor & Sensor Health ─────────────────────────────────────────
sensor_cols = ['Taper_Sensor', 'Tube_Dia_mm', 'PPI_Pos']
has_sensor = any(c in process.columns for c in sensor_cols)
has_motor  = 'I_Table_Motor_OL' in process.columns

if has_sensor or has_motor:
    print('Building chart 6d: Motor & Sensor Health...')
    rows_needed = sum([has_sensor, has_motor])
    titles = []
    if has_sensor: titles.append('Taper Sensor / Tube Diameter')
    if has_motor:  titles.append('Motor Overload Flag')

    fig6d = make_subplots(rows=rows_needed, cols=1, shared_xaxes=True,
                          subplot_titles=titles, vertical_spacing=0.1)
    
    row_idx = 1
    if has_sensor:
        for col, color, name in [('Tube_Dia_mm','#4fc3f7','Tube Dia (mm)'),
                                   ('PPI_Pos','#81c784','Sensor PPI'),
                                   ('Taper_Sensor','#ffb74d','Raw Sensor')]:
            if col in process.columns:
                gapped = process[['Timestamp', col]].dropna()
                if not gapped.empty:
                    gapped = gapped.set_index('Timestamp').resample('2s').mean().reset_index()
                    fig6d.add_trace(go.Scatter(
                        x=gapped['Timestamp'], y=gapped[col],
                        mode='lines', name=name,
                        line=dict(color=color, width=1.5), connectgaps=False
                    ), row=row_idx, col=1)
        add_wb_lines(fig6d, row=row_idx, col=1)
        row_idx += 1

    if has_motor:
        motor = process[['Timestamp','I_Table_Motor_OL']].dropna()
        if not motor.empty:
            fig6d.add_trace(go.Scatter(
                x=motor['Timestamp'], y=motor['I_Table_Motor_OL'].astype(int),
                mode='lines', name='Motor OL',
                line=dict(color='#ef5350', width=2), connectgaps=False,
                fill='tozeroy', fillcolor='rgba(239,83,80,0.15)'
            ), row=row_idx, col=1)
            add_wb_lines(fig6d, row=row_idx, col=1)

    add_state_shading(fig6d, rows=list(range(1, rows_needed + 1)))
    fig6d.update_layout(height=400, title='Motor & Sensor Health',
                        template='plotly_dark')
    figs.append((fig6d, "Chart 6d — Motor & Sensor Health"))
else:
    print('Skipping chart 6d: motor/sensor tags not yet in process log.')


# ── Chart 7 — OEE Summary ─────────────────────────────────────────────────────
if oee.empty:
    print('Skipping chart 7: oee_log.csv not found yet.')
else:
 print('Building chart 7: OEE Summary...')
 latest       = oee.iloc[-1]
running_hrs  = latest['Cum_Running_Hrs']
stopped_hrs  = latest['Cum_Stopped_Hrs']
ready_hrs    = latest['Cum_Ready_Hrs']
total        = running_hrs + stopped_hrs + ready_hrs
availability = 100 * running_hrs / total if total > 0 else 0

print(f'  As of:        {latest["Timestamp"]}')
print(f'  Running:      {running_hrs:,} hrs  ({100*running_hrs/total:.1f}%)')
print(f'  Stopped:      {stopped_hrs:,} hrs  ({100*stopped_hrs/total:.1f}%)')
print(f'  Ready/Idle:   {ready_hrs:,} hrs  ({100*ready_hrs/total:.1f}%)')
print(f'  Availability: {availability:.1f}%')
print(f'  Puller Life:  {latest["Puller_Life_Ft"]:,} ft')

fig7 = go.Figure(go.Pie(
    labels=['Running', 'Stopped', 'Ready/Idle'],
    values=[running_hrs, stopped_hrs, ready_hrs],
    marker_colors=['#66bb6a', '#ef5350', '#ffa726'],
    hole=0.4
))
fig7.update_layout(
    title=f'Lifetime OEE Availability — {availability:.1f}% Running',
    template='plotly_dark', height=400
)
figs.append((fig7, "Chart 7"))

# ── Export all charts to one HTML file ───────────────────────────────────────
OUTPUT_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'braider_report.html')

html_parts = ['''<!DOCTYPE html>
<html>
<head>
    <title>Braider Analysis — Noble Gas Systems</title>
    <style>
        body { background: #1a1a1a; color: #e0e0e0; font-family: monospace; margin: 0; padding: 20px; }
        h1   { color: #4fc3f7; border-bottom: 1px solid #333; padding-bottom: 10px; }
        .sub { color: #888; font-size: 13px; margin-bottom: 30px; }
        .chart-block { margin-bottom: 40px; }
        .chart-title { font-size: 13px; color: #555; text-transform: uppercase;
                       letter-spacing: 2px; margin-bottom: 8px; }
    </style>
</head>
<body>
    <h1>Braider Production Analysis — ''' + BRAIDER_FILTER + '''</h1>
    <div class="sub">Noble Gas Systems — Steeger HS120/48 &nbsp;|&nbsp; Generated from ~/braider_logs/</div>
''']

for fig, title in figs:
    chart_html = to_html(fig, full_html=False, include_plotlyjs='cdn')
    html_parts.append(f'<div class="chart-block"><div class="chart-title">{title}</div>{chart_html}</div>')

html_parts.append('</body></html>')

with open(OUTPUT_HTML, 'w') as f:
    f.write('\n'.join(html_parts))

print(f'\nAll charts saved to: {OUTPUT_HTML}')
print('Open that file in your browser to see all charts on one scrollable page.')

import webbrowser
webbrowser.open(f'file:///{OUTPUT_HTML.replace(chr(92), "/")}')
