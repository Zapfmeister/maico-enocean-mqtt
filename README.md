# MAICO PP 45 RC - EnOcean MQTT Bridge

A Docker-based bridge that connects **MAICO PP 45 RC** decentralized ventilation units to **Home Assistant** via an EnOcean USB 300 DE transceiver and MQTT.

This project reverse-engineers the MAICO MSC protocol (EnOcean RORG 0xD1, function byte 0x27) to provide full control without the official MAICO commissioning software.

## Features

- **Direct MSC protocol control** — fan levels 0-5, operating modes, sleep & boost timers
- **Auto-discovery** — devices are detected automatically from EnOcean traffic, no hardcoded IDs needed
- **Master/slave detection** — relationships between paired units are identified from sync telegrams
- **Web UI** — dashboard with live status, device pairing, settings (German & English)
- **MQTT Discovery** — devices appear automatically in Home Assistant as fan, select, and sensor entities
- **Dual deployment** — run as a standalone Docker container or as a Home Assistant Add-on

## Architecture

```
[PP 45 RC x5] <--EnOcean--> [USB 300 DE on RPi]
                                    |
                             [Docker Container]
                             +------+------+
                        [Python Bridge]  [Web UI :8080]
                             |
                        [MQTT Broker]
                             |
                        [Home Assistant]
```

## Requirements

- **EnOcean USB 300 DE** transceiver (connected via USB)
- **MQTT broker** (e.g. Mosquitto, or the one built into Home Assistant)
- **Docker** on the host machine (Raspberry Pi, NAS, server, etc.)

## Setup Option 1: Standalone Docker (Recommended)

Best when the USB stick is on a different machine than Home Assistant.

### 1. Clone the repository

```bash
git clone https://github.com/zapfmeister/maico-enocean-mqtt.git
cd maico-enocean-mqtt
```

### 2. Find your USB stick path

```bash
ls /dev/serial/by-id/
# Example output: usb-EnOcean_GmbH_USB_300_DE_XXXXXXXX-if00-port0
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
MQTT_HOST=homeassistant.local
MQTT_PORT=1883
MQTT_USERNAME=your_user
MQTT_PASSWORD=your_password
MAICO_WEB_PASSWORD=your_web_password
MAICO_SERIAL_PORT=/dev/enocean
```

### 4. Update docker-compose.yml

Replace the USB device path with your actual path from step 2:

```yaml
devices:
  - /dev/serial/by-id/usb-EnOcean_GmbH_USB_300_DE_XXXXXXXX-if00-port0:/dev/enocean
```

### 5. Start the bridge

```bash
docker compose up -d
```

### 6. Open the Web UI

Navigate to `http://<your-host-ip>:8080` or `http://maico-controller.local:8080` (mDNS).

## Setup Option 2: Home Assistant Add-on

Best when the USB stick is directly connected to the Home Assistant host.

### 1. Add the repository

In Home Assistant, go to **Settings > Add-ons > Add-on Store > Menu (top right) > Repositories** and add:

```
https://github.com/zapfmeister/maico-enocean-mqtt
```

### 2. Install the Add-on

Find **MAICO EnOcean Bridge** in the Add-on Store and click **Install**.

### 3. Configure

In the Add-on configuration tab, set:

| Option | Description |
|---|---|
| `serial_port` | Path to your EnOcean USB stick (e.g. `/dev/ttyUSB0`) |
| `poll_interval` | Status poll interval in seconds (default: 10) |
| `web_password` | Password for the Web UI (optional) |

MQTT credentials are automatically obtained from the Home Assistant Supervisor.

### 4. Start

Click **Start**. The Web UI is available on port 8080.

## Pairing Devices

1. Open the Web UI and navigate to **Pairing**
2. On the PP 45 RC unit: **hold the Learn button for 2 seconds** until all 3 LEDs blink
3. Click **Start Scan** in the Web UI
4. The device will appear — give it a name and save
5. The device immediately appears in Home Assistant

Alternatively, devices are **auto-discovered** from EnOcean traffic. Simply power on your PP 45 RC units and they will appear in the dashboard as they send status reports.

## Home Assistant Entities

Per master device:

| Entity | Type | Description |
|---|---|---|
| Fan | `fan` | On/Off, speed level 1-5, preset modes (Sleep, Boost) |
| Mode | `select` | Heat Exchanger / Summer |
| Direction | `sensor` | Inflow / Exhaust (read-only, firmware-controlled) |
| Timer | `sensor` | Remaining minutes for Sleep/Boost timer |
| Connection | `sensor` | Managed / Passive (diagnostic) |
| Role | `sensor` | Master / Slave / Standalone (diagnostic) |
| Last Seen | `sensor` | Seconds since last radio contact (diagnostic) |

Per slave device: read-only sensor showing current state.

## Web UI

The built-in Web UI provides:

- **Dashboard** — live device status, fan level buttons, mode selection
- **Pairing** — scan and pair new devices
- **Devices** — rename or remove paired devices
- **Settings** — MQTT, poll interval, password, language (DE/EN)

The system language (configurable in Settings) also affects entity names in Home Assistant.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USERNAME` | | MQTT username |
| `MQTT_PASSWORD` | | MQTT password |
| `MAICO_SERIAL_PORT` | `/dev/ttyUSB0` | EnOcean USB stick path |
| `MAICO_POLL_INTERVAL` | `10` | Poll interval in seconds |
| `MAICO_WEB_PORT` | `8080` | Web UI port |
| `MAICO_WEB_PASSWORD` | | Web UI password (empty = no auth) |
| `MAICO_HOSTNAME` | `maico-controller` | mDNS hostname |
| `MAICO_LANGUAGE` | `de` | System language (`de` or `en`) |

Environment variables take precedence over `config.yaml`.

## Protocol Documentation

The MAICO MSC protocol is documented in detail in [`MAICO_ENOCEAN_PROTOCOL.md`](MAICO_ENOCEAN_PROTOCOL.md). This includes:

- MSC telegram structure (27 10 status, 27 20 control, 27 30/40 pairing, 27 00 sync)
- Level byte encoding table
- Mode flags and sleep/boost behavior
- Pairing procedure

## License

[PolyForm Noncommercial License 1.0.0](LICENSE) — free for noncommercial use
(private/home, hobby, research, education). **Commercial use requires a
separate license** — see [COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md).

> Note: releases up to and including the last MIT-licensed commit remain
> available under the MIT License; this change applies going forward.

## Credits

Built by [Gerard Zapf](https://github.com/zapfmeister).

This is a community project and is **not affiliated with or endorsed by MAICO Elektroapparate-Fabrik GmbH**.
