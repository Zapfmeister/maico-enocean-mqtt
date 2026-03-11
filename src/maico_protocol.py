"""MAICO PP 45 RC MSC protocol encoding and decoding.

Uses MSC (Manufacturer Specific Communication, RORG 0xD1) with function byte 0x27.
Replaces the old RPS virtual-switch approach with direct MSC level control.

Message types:
  27 20 - Set level (Controller → Device): level byte E0-E5
  27 10 - Status report (Device → Controller): periodic feedback
  27 30 - Teach-in scan (Controller → Broadcast)
  27 40 - Teach-in response (Device → Controller)
  27 00 - Master-Slave sync
  27 50 - Device teach-in announcement
  27 70 - Slave ACK
"""

import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

RORG_MSC = 0xD1
MSC_FUNC = 0x27


class VentilationMode(str, Enum):
    OFF = "off"
    HEAT_EXCHANGER = "heat_exchanger"
    SUMMER = "summer"
    SLEEP_HEAT = "sleep_heat"
    SLEEP_SUMMER = "sleep_summer"
    BOOST = "boost"


class AirflowDirection(str, Enum):
    INFLOW = "inflow"
    EXHAUST = "exhaust"
    UNKNOWN = "unknown"


class MscType(str, Enum):
    MASTER_SLAVE_SYNC = "27_00"
    STATUS_REPORT = "27_10"
    SET_LEVEL = "27_20"
    TEACH_IN_SCAN = "27_30"
    TEACH_IN_RESPONSE = "27_40"
    DEVICE_ANNOUNCE = "27_50"
    SLAVE_ACK = "27_70"
    UNKNOWN = "unknown"


@dataclass
class VentilationState:
    mode: VentilationMode = VentilationMode.OFF
    fan_level: int = 0
    direction: AirflowDirection = AirflowDirection.UNKNOWN

    @property
    def is_on(self) -> bool:
        return self.mode != VentilationMode.OFF and self.fan_level > 0

    def to_dict(self) -> dict:
        return {
            "mode": self.mode.value,
            "fan_level": self.fan_level,
            "direction": self.direction.value,
            "is_on": self.is_on,
        }


@dataclass
class MscTelegram:
    msg_type: MscType
    sender: list[int]
    dest: list[int] | None = None
    state: VentilationState | None = None
    raw_data: list[int] | None = None


# Status decoding table: stufe byte -> (mode, level, direction)
# The stufe byte in 27 10 reports encodes both mode and level.
# Verified from live traffic: 27 10 [stufe] [0xE0] 00 00 00
_STATUS_MAP: dict[int, tuple[VentilationMode, int, AirflowDirection]] = {
    0x00: (VentilationMode.OFF, 0, AirflowDirection.UNKNOWN),

    # Heat exchanger - exhaust (0x01-0x05)
    0x01: (VentilationMode.HEAT_EXCHANGER, 1, AirflowDirection.EXHAUST),
    0x02: (VentilationMode.HEAT_EXCHANGER, 2, AirflowDirection.EXHAUST),
    0x03: (VentilationMode.HEAT_EXCHANGER, 3, AirflowDirection.EXHAUST),
    0x04: (VentilationMode.HEAT_EXCHANGER, 4, AirflowDirection.EXHAUST),
    0x05: (VentilationMode.HEAT_EXCHANGER, 5, AirflowDirection.EXHAUST),

    # Heat exchanger - inflow (0x21-0x25)
    0x21: (VentilationMode.HEAT_EXCHANGER, 1, AirflowDirection.INFLOW),
    0x22: (VentilationMode.HEAT_EXCHANGER, 2, AirflowDirection.INFLOW),
    0x23: (VentilationMode.HEAT_EXCHANGER, 3, AirflowDirection.INFLOW),
    0x24: (VentilationMode.HEAT_EXCHANGER, 4, AirflowDirection.INFLOW),
    0x25: (VentilationMode.HEAT_EXCHANGER, 5, AirflowDirection.INFLOW),

    # Summer mode (0x08-0x0D)
    0x08: (VentilationMode.SUMMER, 0, AirflowDirection.UNKNOWN),
    0x09: (VentilationMode.SUMMER, 1, AirflowDirection.UNKNOWN),
    0x0A: (VentilationMode.SUMMER, 2, AirflowDirection.UNKNOWN),
    0x0B: (VentilationMode.SUMMER, 3, AirflowDirection.UNKNOWN),
    0x0C: (VentilationMode.SUMMER, 4, AirflowDirection.UNKNOWN),
    0x0D: (VentilationMode.SUMMER, 5, AirflowDirection.UNKNOWN),

    # Sleep modes
    0x40: (VentilationMode.SLEEP_HEAT, 0, AirflowDirection.UNKNOWN),
    0x48: (VentilationMode.SLEEP_SUMMER, 0, AirflowDirection.UNKNOWN),
}


def id_to_str(id_bytes: list[int] | bytes) -> str:
    """Convert 4-byte ID to hex string (e.g. '051EA803')."""
    return "".join(f"{b:02X}" for b in id_bytes)


def str_to_id(id_str: str) -> list[int]:
    """Convert hex string to 4-byte ID list. Accepts '051EA803' or '05:1E:A8:03'."""
    clean = id_str.replace(":", "").replace(" ", "")
    return [int(clean[i:i+2], 16) for i in range(0, 8, 2)]


def build_set_level(base_id: list[int], device_id: list[int], level: int,
                    mode: VentilationMode = VentilationMode.HEAT_EXCHANGER) -> tuple[list[int], list[int]]:
    """Build MSC 27 20 set-level command.

    The level byte encodes both mode and level:
      - Heat exchanger: 0xE0 + level (0xE0-0xE5)
      - Summer:         0xE8 + level (0xE8-0xED)
    Sleep mode uses level byte 0xE0 + a flag byte (0x01) at position 5.

    Args:
        base_id: USB stick base ID (4 bytes)
        device_id: Target device ID (4 bytes)
        level: Ventilation level 0-5
        mode: Operating mode

    Returns:
        (data, optional) tuple for send_esp3
    """
    level = max(0, min(5, level))

    if mode in (VentilationMode.SLEEP_HEAT, VentilationMode.SLEEP_SUMMER):
        # Sleep: level byte E0 (off), flag byte 0x01
        level_byte = 0xE0
        flag_byte = 0x01
    elif mode == VentilationMode.SUMMER:
        level_byte = 0xE8 + level
        flag_byte = 0x00
    else:
        # Heat exchanger (default) or boost (= just level 5)
        level_byte = 0xE0 + level
        flag_byte = 0x00

    data = [RORG_MSC, MSC_FUNC, 0x20, level_byte, 0x00, flag_byte] + base_id + [0x00]
    optional = [0x03] + device_id + [0xFF, 0x00]
    return data, optional


def build_scan(base_id: list[int]) -> tuple[list[int], list[int]]:
    """Build MSC 27 30 teach-in scan (broadcast).

    Returns:
        (data, optional) tuple for send_esp3
    """
    data = [RORG_MSC, MSC_FUNC, 0x30] + base_id + [0x00]
    optional = [0x03, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00]
    return data, optional


def parse_msc_telegram(user_data: list[int], sender: list[int], dest: list[int] | None = None) -> MscTelegram | None:
    """Parse MSC telegram user_data into structured MscTelegram.

    Args:
        user_data: Bytes after RORG, before sender ID (from ESP3 parsing)
        sender: 4-byte sender ID
        dest: 4-byte destination ID (from optional data)

    Returns:
        MscTelegram or None if not a valid MAICO MSC telegram
    """
    if len(user_data) < 2 or user_data[0] != MSC_FUNC:
        return None

    type_byte = user_data[1]
    msg_type = _TYPE_MAP.get(type_byte, MscType.UNKNOWN)

    telegram = MscTelegram(
        msg_type=msg_type,
        sender=sender,
        dest=dest,
        raw_data=user_data,
    )

    if msg_type == MscType.STATUS_REPORT and len(user_data) >= 5:
        # 27 10 [stufe] [mode_byte] 00 00 00
        # Decode using the status map from the original protocol data
        stufe = user_data[2]
        mode_byte = user_data[3]
        telegram.state = _decode_status_report(stufe, mode_byte)

    elif msg_type == MscType.SET_LEVEL and len(user_data) >= 3:
        # 27 20 [level_byte] [byte4] [flag_byte]
        # Level byte: 0xE0 + mode_offset + level
        # Flag byte (user_data[4]): 0x01 = sleep mode
        level_byte = user_data[2]
        flag_byte = user_data[4] if len(user_data) >= 5 else 0x00
        if 0xE0 <= level_byte <= 0xED:
            if flag_byte == 0x01:
                # Sleep mode
                telegram.state = VentilationState(
                    mode=VentilationMode.SLEEP_HEAT, fan_level=0)
            else:
                raw = level_byte - 0xE0
                if raw >= 0x08:  # Summer mode
                    level = raw - 0x08
                    mode = VentilationMode.SUMMER
                else:  # Heat exchanger
                    level = raw
                    mode = VentilationMode.HEAT_EXCHANGER if level > 0 else VentilationMode.OFF
                telegram.state = VentilationState(mode=mode, fan_level=level)

    return telegram


def _decode_status_report(stufe: int, mode_byte: int) -> VentilationState:
    """Decode a 27 10 status report's stufe and mode bytes.

    The stufe byte encodes both mode and level (verified from traffic):
      - 0x00:       Off
      - 0x01-0x05:  Wärmetauscher (exhaust) level 1-5
      - 0x21-0x25:  Wärmetauscher (inflow) level 1-5
      - 0x08:       Sommer off
      - 0x09-0x0D:  Sommer level 1-5
      - 0x40:       Schlaf (Wärmetauscher)
      - 0x48:       Schlaf (Sommer)
    The mode_byte is typically 0xE0 and not used for mode detection.
    """
    # Look up stufe in status map (key is 0x00XX where XX = stufe)
    entry = _STATUS_MAP.get(stufe)
    if entry:
        mode, level, direction = entry
        return VentilationState(mode=mode, fan_level=level, direction=direction)

    # Fallback: unknown stufe value
    logger.warning("Unknown status stufe byte: 0x%02X (mode_byte: 0x%02X)", stufe, mode_byte)
    return VentilationState(mode=VentilationMode.OFF, fan_level=0, direction=AirflowDirection.UNKNOWN)


_TYPE_MAP: dict[int, MscType] = {
    0x00: MscType.MASTER_SLAVE_SYNC,
    0x10: MscType.STATUS_REPORT,
    0x20: MscType.SET_LEVEL,
    0x30: MscType.TEACH_IN_SCAN,
    0x40: MscType.TEACH_IN_RESPONSE,
    0x50: MscType.DEVICE_ANNOUNCE,
    0x70: MscType.SLAVE_ACK,
}
