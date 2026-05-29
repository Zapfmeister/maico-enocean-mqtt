"""Tests for the HomeKit-friendly mode controls (Sommer switch + buttons)."""

from src.config import AppConfig, DeviceConfig
from src.maico_protocol import VentilationMode, VentilationState
from src.mqtt_client import MqttClient


class FakeBridge:
    def __init__(self):
        self.mode_calls = []
        self.level_calls = []
        self.power_calls = []

    def dispatch(self, fn, *args):
        fn(*args)  # mirror the no-loop fallback

    def set_mode(self, name, mode):
        self.mode_calls.append((name, mode))

    def set_level(self, name, level):
        self.level_calls.append((name, level))

    def set_power(self, name, on):
        self.power_calls.append((name, on))


class Msg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload.encode()


class Recorder:
    def __init__(self):
        self.pub = []

    def publish(self, topic, payload, retain=False):
        self.pub.append((topic, payload))


def _client():
    cfg = AppConfig()
    cfg.devices = [DeviceConfig(name="Bad", friendly_name="Bad", device_id="051EA803")]
    bridge = FakeBridge()
    return MqttClient(cfg, bridge), bridge


def test_summer_switch_on_sets_summer():
    mc, bridge = _client()
    mc._on_message(None, None, Msg("maico/Bad/set/summer", "ON"))
    assert bridge.mode_calls == [("Bad", VentilationMode.SUMMER)]


def test_summer_switch_off_sets_heat_exchanger():
    mc, bridge = _client()
    mc._on_message(None, None, Msg("maico/Bad/set/summer", "OFF"))
    assert bridge.mode_calls == [("Bad", VentilationMode.HEAT_EXCHANGER)]


def test_sleep_button_sets_sleep():
    mc, bridge = _client()
    mc._on_message(None, None, Msg("maico/Bad/set/sleep", "PRESS"))
    assert bridge.mode_calls == [("Bad", VentilationMode.SLEEP_HEAT)]


def test_boost_button_sets_boost():
    mc, bridge = _client()
    mc._on_message(None, None, Msg("maico/Bad/set/boost", "PRESS"))
    assert bridge.mode_calls == [("Bad", VentilationMode.BOOST)]


def test_publish_state_emits_summer_on_in_summer():
    mc, _ = _client()
    mc._client = Recorder()
    mc._connected = True
    mc.publish_state("Bad", VentilationState(mode=VentilationMode.SUMMER, fan_level=3))
    assert ("maico/Bad/summer", "ON") in mc._client.pub


def test_publish_state_emits_summer_off_otherwise():
    mc, _ = _client()
    mc._client = Recorder()
    mc._connected = True
    mc.publish_state("Bad", VentilationState(mode=VentilationMode.HEAT_EXCHANGER, fan_level=2))
    assert ("maico/Bad/summer", "OFF") in mc._client.pub
