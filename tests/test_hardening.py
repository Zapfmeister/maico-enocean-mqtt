"""Hardening: unique MQTT client_id and opt-in in-container mDNS."""

import src.mqtt_client as mc
from src.config import AppConfig, load_config


def test_mdns_defaults_off_and_env_override(tmp_path, monkeypatch):
    assert load_config(str(tmp_path / "missing.yaml")).web.mdns is False
    monkeypatch.setenv("MAICO_MDNS", "true")
    assert load_config(str(tmp_path / "missing.yaml")).web.mdns is True


def test_client_id_defaults_blank_and_env_override(tmp_path, monkeypatch):
    assert load_config(str(tmp_path / "missing.yaml")).mqtt.client_id == ""
    monkeypatch.setenv("MAICO_MQTT_CLIENT_ID", "pinned-id")
    assert load_config(str(tmp_path / "missing.yaml")).mqtt.client_id == "pinned-id"


class _FakeClient:
    captured: list = []

    def __init__(self, *a, **k):
        _FakeClient.captured.append(k.get("client_id"))

    def username_pw_set(self, *a, **k): pass
    def will_set(self, *a, **k): pass
    def reconnect_delay_set(self, *a, **k): pass
    def loop_start(self): pass
    def connect_async(self, *a, **k): pass


def test_generated_client_id_is_unique(monkeypatch):
    _FakeClient.captured = []
    monkeypatch.setattr(mc.mqtt, "Client", _FakeClient)
    mc.MqttClient(AppConfig(), bridge=None).connect()
    mc.MqttClient(AppConfig(), bridge=None).connect()
    a, b = _FakeClient.captured
    assert a.startswith("maico-enocean-bridge-")
    assert a != b  # two instances never collide


def test_configured_client_id_is_used(monkeypatch):
    _FakeClient.captured = []
    monkeypatch.setattr(mc.mqtt, "Client", _FakeClient)
    cfg = AppConfig()
    cfg.mqtt.client_id = "fixed-id"
    mc.MqttClient(cfg, bridge=None).connect()
    assert _FakeClient.captured[0] == "fixed-id"
