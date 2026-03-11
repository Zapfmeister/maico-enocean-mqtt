"""MSC Teach-In for pairing MAICO PP 45 RC devices.

Pairing procedure:
1. Put device in learn mode: hold 2sec, 1 short press, hold 2sec
2. Send 27 30 broadcast scan
3. Device responds with 27 40 (teach-in response)
4. Confirm with 27 20 (set level) command

This module provides both programmatic API (for web-UI) and CLI usage.
"""

import asyncio
import logging
import time

from .enocean_serial import EnOceanSerial, RORG_MSC, parse_esp3_packets
from .maico_protocol import (
    MSC_FUNC,
    MscType,
    build_scan,
    build_set_level,
    id_to_str,
    parse_msc_telegram,
)

logger = logging.getLogger(__name__)

SCAN_TIMEOUT = 30  # seconds to wait for teach-in response
SCAN_INTERVAL = 5  # seconds between scan broadcasts


class TeachInResult:
    def __init__(self):
        self.found_devices: list[str] = []
        self.status: str = "idle"
        self.error: str | None = None


async def run_teach_in(serial: EnOceanSerial, timeout: int = SCAN_TIMEOUT) -> TeachInResult:
    """Run teach-in scan and wait for responses.

    The serial receive loop must be running. This function sends periodic
    27 30 scans and collects 27 40 responses.
    """
    result = TeachInResult()

    if not serial.base_id:
        result.status = "error"
        result.error = "No base ID available"
        return result

    result.status = "scanning"
    found: list[str] = []
    start = time.monotonic()

    # Store original callback
    original_callback = serial._on_packet

    def on_packet(pkt: dict) -> None:
        if pkt.get('type') == 'radio' and pkt.get('rorg') == RORG_MSC:
            telegram = parse_msc_telegram(
                pkt['user_data'], pkt['sender'], pkt.get('dest')
            )
            if telegram and telegram.msg_type in (MscType.TEACH_IN_RESPONSE, MscType.DEVICE_ANNOUNCE):
                dev_id = id_to_str(telegram.sender)
                if dev_id not in found:
                    found.append(dev_id)
                    logger.info("Teach-in: found device %s", dev_id)

        # Also call original callback
        if original_callback:
            original_callback(pkt)

    serial._on_packet = on_packet

    try:
        while time.monotonic() - start < timeout:
            # Send scan broadcast
            data, optional = build_scan(serial.base_id)
            serial.send(data, optional)
            logger.info("Teach-in: scan sent (%d/%ds)", int(time.monotonic() - start), timeout)

            # Wait before next scan
            await asyncio.sleep(SCAN_INTERVAL)

            if found:
                # Found at least one device, confirm with 27 20
                for dev_id_str in found:
                    dev_id = [int(dev_id_str[i:i+2], 16) for i in range(0, 8, 2)]
                    data, optional = build_set_level(serial.base_id, dev_id, 0)
                    serial.send(data, optional)
                    logger.info("Teach-in: confirmed pairing with %s", dev_id_str)
                break

    finally:
        serial._on_packet = original_callback

    result.found_devices = found
    result.status = "found" if found else "timeout"
    return result
