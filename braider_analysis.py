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

# ── Load files ────────────────────────────────────────────────────────────────
print('Loading log files...')
def load_csv(filename, **kwargs):
    path = os.path.join(LOG_DIR, filename)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return pd.read_csv(path, **kwargs)
    print(f'  Warning: {filename} not found or empty — skipping.')
    return pd.DataFrame()

process     = load_csv('process_log.csv',   parse_dates=['Timestamp'])
events      = load_csv('event_log.csv',     parse_dates=['Timestamp'])
oee         = load_csv('oee_log.csv',       parse_dates=['Timestamp'])
wire_breaks = load_csv('wire_break_log.csv',parse_dates=['Timestamp'])

if process.empty:
    print('ERROR: process_log.csv is missing — make sure braider_monitor.py is running.')
    exit()

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
            color, opacity = '#66bb6a', 0.07   # green — running
        elif seg['state'] in (1,):
            color, opacity = '#455a64', 0.10   # dark grey — off
        else:
            color, opacity = '#ef5350', 0.07   # red tint — stopped/fault

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
# No state shading on chart 2 — data is running-only, shading is redundant
# Break the line across gaps so it doesn't connect across stopped periods
# Insert NaN rows where machine was not running to create visual gaps
ratio_gapped = ratio.copy().set_index('Timestamp').resample('2s').mean().reset_index()
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


# ── Chart 4 — Production Feet ─────────────────────────────────────────────────
print('Building chart 4: Production Feet...')
fig4 = go.Figure()
fig4.add_trace(go.Scatter(
    x=process['Timestamp'], y=process['Puller_Pos_Feet'],
    mode='lines', name='Feet Produced',
    line=dict(color='#4fc3f7', width=1.5)
))
add_state_shading(fig4)
add_wb_lines(fig4)
fig4.update_layout(height=400, title='Cumulative Production (Puller Position)',
                   template='plotly_dark', yaxis_title='Feet')
figs.append((fig4, "Chart 4"))


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
    <h1>Braider Production Analysis</h1>
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