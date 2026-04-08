import asyncio
import json
import math
import websockets
from collections import deque
from bleak import BleakScanner, BleakClient

HR_RX_CHAR_UUID = "00002a37-0000-1000-8000-00805f9b34fb"

connected_clients = set()

# Keep a rolling window of the last 20 RR intervals to calculate HRV
rr_window = deque(maxlen=20)

async def broadcast_data(hr, hrv, current_rrs):
    """Send the heart rate, HRV, and raw beat intervals to all connected web pages."""
    if connected_clients:
        message = json.dumps({"bpm": hr, "hrv": hrv, "rrs": current_rrs})
        await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

def hr_value_handler(sender, data):
    """Callback for when the heart rate monitor sends data."""
    flags = data[0]
    is_16_bit = flags & 0x01
    offset = 1

    # 1. Extract BPM
    if is_16_bit:
        hr = int.from_bytes(data[offset:offset+2], byteorder='little')
        offset += 2
    else:
        hr = data[offset]
        offset += 1

    # Skip Energy Expended if present
    if flags & 0x08:
        offset += 2

    # 2. Extract RR Intervals and Calculate HRV
    hrv = 0
    current_rrs = [] # Store the raw intervals from this specific packet

    if flags & 0x10:
        while offset < len(data):
            # RR interval is in units of 1/1024 seconds
            rr_raw = int.from_bytes(data[offset:offset+2], byteorder='little')
            rr_seconds = rr_raw / 1024.0
            rr_window.append(rr_seconds)
            current_rrs.append(rr_seconds)
            offset += 2

        # Calculate RMSSD (Root Mean Square of Successive Differences) for HRV
        if len(rr_window) > 1:
            diffs = [(rr_window[i] - rr_window[i-1])**2 for i in range(1, len(rr_window))]
            rmssd = math.sqrt(sum(diffs) / len(diffs))
            hrv = int(rmssd * 1000) # Convert to milliseconds

    print(f"Live HR: {hr} bpm | HRV (Stress): {hrv} ms")
    asyncio.create_task(broadcast_data(hr, hrv, current_rrs))

async def ws_handler(websocket):
    print("New web client connected!")
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)
        print("Web client disconnected.")

async def connect_polar():
    print("Scanning for Polar heart rate monitors...")
    devices = await BleakScanner.discover()
    polar_device = next((d for d in devices if d.name and "Polar" in d.name), None)

    if not polar_device:
        print("No Polar device found. Wake it up and try again.")
        return

    print(f"Connecting to {polar_device.name}...")

    while True:
        try:
            async with BleakClient(polar_device.address) as client:
                print("BLE Connected successfully! Broadcasting on ws://localhost:8765")
                await client.start_notify(HR_RX_CHAR_UUID, hr_value_handler)
                while client.is_connected:
                    await asyncio.sleep(1)
        except Exception as e:
            print(f"Connection lost or error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def main():
    async with websockets.serve(ws_handler, "localhost", 8765):
        await connect_polar()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer shut down.")
