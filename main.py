import asyncio
import json
import websockets
from bleak import BleakScanner, BleakClient

# Standard Bluetooth SIG Heart Rate Service and Measurement Characteristic UUIDs
HR_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"
HR_SERVICE_UUID = "0000180d-0000-1000-8000-00805f9b34fb"

connected_clients = set()

async def broadcast_hr(bpm):
    """Broadcast live heart rate (BPM) to all connected WebSocket clients."""
    if connected_clients:
        message = json.dumps({"bpm": bpm})
        await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

def hr_notification_handler(sender, data: bytearray):
    """Parse standard Bluetooth LE Heart Rate notification packets (0x2A37)."""
    if not data or len(data) < 2:
        return

    flags = data[0]
    is_16_bit = flags & 0x01

    if is_16_bit and len(data) >= 3:
        bpm = int.from_bytes(data[1:3], byteorder='little')
    else:
        bpm = data[1]

    # Ignore invalid 0 bpm or off-body readings
    if bpm > 0:
        print(f"Live HR: {bpm} bpm")
        asyncio.create_task(broadcast_hr(bpm))

async def ws_handler(websocket):
    print("New overlay client connected!")
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)
        print("Overlay client disconnected.")

async def find_hr_device():
    print("Scanning for Bluetooth Heart Rate monitors (Polar, Garmin, Wahoo, etc.)...")
    devices_dict = await BleakScanner.discover(timeout=5.0, return_adv=True)
    
    candidates = []
    keywords = ["polar", "oh1", "verity", "sense", "h10", "h9", "h7", "heart", "hr", "tickr", "garmin", "coospo", "magene"]
    
    for addr, (device, adv) in devices_dict.items():
        name = ((device.name or "") + " " + ((adv.local_name if adv else "") or "")).lower()
        uuids = [u.lower() for u in (adv.service_uuids if adv and adv.service_uuids else [])]

        if any(kw in name for kw in keywords) or HR_SERVICE_UUID in uuids:
            candidates.append((device, adv))

    if not candidates:
        print("No Heart Rate monitor found. Ensure device is turned on and not connected to another app.")
        return None

    def score_device(item):
        dev, adv = item
        n = ((dev.name or "") + " " + ((adv.local_name if adv else "") or "")).lower()
        if "polar" in n or "oh1" in n or "verity" in n or "h10" in n: return 10
        if "heart" in n or "hr" in n: return 8
        return 5

    candidates.sort(key=score_device, reverse=True)
    selected_device, selected_adv = candidates[0]
    dev_name = selected_device.name or (selected_adv.local_name if selected_adv else None) or selected_device.address
    print(f"Found Heart Rate monitor: {dev_name} [{selected_device.address}]")
    return selected_device

async def connect_and_stream():
    while True:
        device = await find_hr_device()
        if not device:
            await asyncio.sleep(4)
            continue

        dev_name = device.name or device.address
        print(f"Connecting to {dev_name}...")

        try:
            async with BleakClient(device.address) as client:
                print(f"Connected to {dev_name}! Streaming live Heart Rate to ws://localhost:8765...")
                await client.start_notify(HR_MEASUREMENT_UUID, hr_notification_handler)

                while client.is_connected:
                    await asyncio.sleep(1)
        except Exception as e:
            print(f"Connection lost or error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

async def main():
    async with websockets.serve(ws_handler, "localhost", 8765):
        await connect_and_stream()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer shut down.")
