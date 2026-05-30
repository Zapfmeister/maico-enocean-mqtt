"""Device state model and registry for the MAICO bridge.

Holds the per-device runtime state (`DeviceStatus`), the EnOcean ID validation
and the sync-byte decoding tables, plus `DeviceTable` — the in-memory store that
maps device IDs/names to their config, ventilation state and status. Extracted
from main.py so the bridge orchestration and this state model can evolve (and be
tested) independently.
"""

import time
from dataclasses import dataclass

from .config import DeviceConfig
from .maico_protocol import AirflowDirection, VentilationState

SYNC_ROLE_TIMEOUT = 120          # seconds without sync before a role resets
DEVICE_OFFLINE_TIMEOUT = 300     # seconds without traffic before HA marks unavailable
AVAILABILITY_CHECK_INTERVAL = 30  # how often to re-evaluate per-device availability

# IDs that must never be treated as devices
_INVALID_DEVICE_IDS: set[str] = {
    "FFFFFFFF",  # EnOcean broadcast address
    "00000000",  # Null address
}

# 27 00 sync byte → (master direction, level)
SYNC_STATUS_MAP: dict[int, tuple[AirflowDirection, int]] = {
    0x00: (AirflowDirection.UNKNOWN, 0),
    # Inflow (Zuluft) — 0x2X
    0x21: (AirflowDirection.INFLOW, 1),
    0x22: (AirflowDirection.INFLOW, 2),
    0x23: (AirflowDirection.INFLOW, 3),
    0x24: (AirflowDirection.INFLOW, 4),
    0x25: (AirflowDirection.INFLOW, 5),
    # Exhaust (Abluft) — 0x0X
    0x01: (AirflowDirection.EXHAUST, 1),
    0x02: (AirflowDirection.EXHAUST, 2),
    0x03: (AirflowDirection.EXHAUST, 3),
    0x04: (AirflowDirection.EXHAUST, 4),
    0x05: (AirflowDirection.EXHAUST, 5),
    # Sleep mode (0x60 seen in sync during sleep)
    0x60: (AirflowDirection.UNKNOWN, 0),
}

OPPOSITE_DIR: dict[AirflowDirection, AirflowDirection] = {
    AirflowDirection.INFLOW: AirflowDirection.EXHAUST,
    AirflowDirection.EXHAUST: AirflowDirection.INFLOW,
    AirflowDirection.UNKNOWN: AirflowDirection.UNKNOWN,
}


def is_valid_device_id(device_id_str: str) -> bool:
    """Check if an EnOcean device ID looks like a real MAICO device.

    Rejects broadcast addresses, null addresses, and IDs with too many
    0xFF bytes (typically from RF noise or corrupt serial data).
    """
    clean = device_id_str.upper().replace(":", "")
    if clean in _INVALID_DEVICE_IDS:
        return False
    # Count 0xFF bytes — real MAICO IDs never have more than one
    ff_count = sum(1 for i in range(0, 8, 2) if clean[i:i + 2] == "FF")
    if ff_count >= 2:
        return False
    return True


class ConnectionStatus:
    MANAGED = "managed"
    PASSIVE = "passive"
    UNKNOWN = "unknown"


@dataclass
class DeviceStatus:
    connection: str = ConnectionStatus.UNKNOWN
    last_seen: float = 0.0
    last_response_to_us: float = 0.0
    response_count: int = 0
    rssi: int | None = None           # last receive signal strength in dBm
    # Detected relationships from 27 00 sync traffic
    syncs_to: str | None = None       # Device ID this device sends 27 00 to (= is master of)
    synced_from: str | None = None    # Device ID that sends 27 00 to us (= our master)
    last_sync: float = 0.0            # Timestamp of last sync traffic

    @property
    def last_seen_ago(self) -> int:
        if self.last_seen == 0:
            return -1
        return int(time.time() - self.last_seen)

    @property
    def detected_role(self) -> str:
        """Role detected from traffic, not config. Pure read.

        A relationship that hasn't been refreshed within SYNC_ROLE_TIMEOUT no
        longer counts as active; the underlying fields are cleared separately
        by expire_stale_role() so that reading the role has no side effects.
        """
        if self.last_sync > 0 and (time.time() - self.last_sync) > SYNC_ROLE_TIMEOUT:
            return "standalone"
        if self.syncs_to:
            return "master"
        if self.synced_from:
            return "slave"
        return "standalone"

    def expire_stale_role(self, now: float | None = None) -> None:
        """Clear master/slave relationship once the sync traffic has gone stale.

        Called periodically (from the availability loop) instead of on every
        read of detected_role."""
        if now is None:
            now = time.time()
        if self.last_sync > 0 and (now - self.last_sync) > SYNC_ROLE_TIMEOUT:
            self.syncs_to = None
            self.synced_from = None
            self.last_sync = 0.0

    def to_dict(self) -> dict:
        return {
            "connection": self.connection,
            "last_seen_ago": self.last_seen_ago,
            "response_count": self.response_count,
            "detected_role": self.detected_role,
            "syncs_to": self.syncs_to,
            "synced_from": self.synced_from,
            "rssi": self.rssi,
        }


class DeviceTable:
    """In-memory store of devices, indexed by both EnOcean ID and name."""

    def __init__(self) -> None:
        self.states: dict[str, VentilationState] = {}
        self.status: dict[str, DeviceStatus] = {}
        self.id_to_name: dict[str, str] = {}
        self.name_to_config: dict[str, DeviceConfig] = {}
        self.state_known: set[str] = set()  # devices with confirmed state from traffic

    @staticmethod
    def clean_id(device_id: str) -> str:
        return device_id.upper().replace(":", "")

    def register(self, device: DeviceConfig) -> None:
        """Add (or refresh) a device's mappings and initialise its state/status."""
        self.id_to_name[self.clean_id(device.device_id)] = device.name
        self.name_to_config[device.name] = device
        self.states.setdefault(device.name, VentilationState())
        self.status.setdefault(device.name, DeviceStatus())

    def remove(self, name: str, device_id: str) -> None:
        self.id_to_name.pop(self.clean_id(device_id), None)
        self.name_to_config.pop(name, None)
        self.states.pop(name, None)
        self.status.pop(name, None)
        self.state_known.discard(name)
