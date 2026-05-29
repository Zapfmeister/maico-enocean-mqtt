"""Airflow direction must come from 27 00 sync, never from 27 10 status reports.

27 10 status reports always report the exhaust form regardless of the unit's
real reversal phase (verified from live traffic), so they must not drive the
direction sensor.
"""

from src.config import AppConfig, DeviceConfig
from src.main import MaicoMqttBridge
from src.maico_protocol import AirflowDirection, VentilationMode, VentilationState


def _bridge_with(name, device_id):
    bridge = MaicoMqttBridge(AppConfig())
    bridge.config.devices.append(DeviceConfig(name=name, device_id=device_id))
    bridge._setup_device_mappings()
    return bridge


def test_status_report_does_not_override_sync_direction():
    bridge = _bridge_with("leo", "051EA5D9")
    # Sync previously established inflow.
    bridge._states["leo"].direction = AirflowDirection.INFLOW
    # A status report decodes to exhaust — must NOT change the direction.
    report = VentilationState(mode=VentilationMode.HEAT_EXCHANGER, fan_level=2,
                              direction=AirflowDirection.EXHAUST)
    bridge._handle_status_report("051EA5D9", report)
    assert bridge._states["leo"].direction == AirflowDirection.INFLOW
    # ...but mode/level from the report are applied.
    assert bridge._states["leo"].fan_level == 2
    assert bridge._states["leo"].mode == VentilationMode.HEAT_EXCHANGER


def test_status_report_leaves_direction_unknown_without_sync():
    bridge = _bridge_with("bad", "051EF6BA")
    report = VentilationState(mode=VentilationMode.HEAT_EXCHANGER, fan_level=3,
                              direction=AirflowDirection.EXHAUST)
    bridge._handle_status_report("051EF6BA", report)
    assert bridge._states["bad"].direction == AirflowDirection.UNKNOWN
    assert bridge._states["bad"].fan_level == 3


def test_sync_sets_direction():
    bridge = _bridge_with("leo", "051EA5D9")
    # 27 00 sync from master (sender), status byte 0x22 -> inflow level 2.
    bridge._handle_sync("051EA5D9", [0x05, 0x22, 0x96, 0x57],
                        [0x27, 0x00, 0x22, 0x31, 0x00])
    assert bridge._states["leo"].direction == AirflowDirection.INFLOW
