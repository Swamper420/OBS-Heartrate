# OBS-Heartrate

A high-performance, visually animated heart rate overlay for OBS Studio and web browsers, supporting Polar devices (Polar OH1, Verity Sense, H10, H9, H7) and standard Bluetooth LE heart rate monitors.

## Features

- **Polar OH1 & Verity Sense Support**: Full support for Polar PMD (Polar Measurement Data) PPI (Pulse-to-Pulse Interval) streaming for high-precision optical beat intervals and real HRV calculation.
- **Universal BLE HR Fallback**: Automatic beat interval synthesis and RMSSD HRV calculation for optical devices when standard BLE packets omit RR intervals.
- **Dynamic OBS Overlay**: Live ECG canvas, zone personality animations, beat thumps, border sweeps, and HRV stress metrics.

## Getting Started

### 1. Setup Virtual Environment
```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

### 2. Run Backend Server
```bash
./venv/bin/python3 main.py
```

### 3. Open Overlay in OBS
Add `index.html` as a **Browser Source** in OBS Studio (or open directly in any web browser).
