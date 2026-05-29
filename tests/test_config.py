"""Tests for configuration loading, env overrides and save/reload round trip."""

from src.config import AppConfig, DeviceConfig, load_config


def test_defaults(tmp_path):
    cfg = load_config(str(tmp_path / "missing.yaml"))
    assert cfg.mqtt.host == "localhost"
    assert cfg.mqtt.port == 1883
    assert cfg.poll_interval == 10
    assert cfg.language == "de"
    assert cfg.devices == []


def test_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("MQTT_HOST", "broker.lan")
    monkeypatch.setenv("MQTT_PORT", "8883")
    monkeypatch.setenv("MAICO_POLL_INTERVAL", "20")
    monkeypatch.setenv("MAICO_RLS_GLOBAL_SYNC", "true")
    cfg = load_config(str(tmp_path / "missing.yaml"))
    assert cfg.mqtt.host == "broker.lan"
    assert cfg.mqtt.port == 8883
    assert cfg.poll_interval == 20
    assert cfg.rls_global_sync is True


def test_save_reload_round_trip(tmp_path):
    path = tmp_path / "config.yaml"
    cfg = AppConfig(config_path=str(path))
    cfg.mqtt.host = "10.0.0.5"
    cfg.add_device(DeviceConfig(name="buero", friendly_name="Büro", device_id="051EA803"))
    cfg.remote.device_id = "05229657"
    assert cfg.save() is True
    assert path.exists()

    reloaded = load_config(str(path))
    assert reloaded.mqtt.host == "10.0.0.5"
    assert len(reloaded.devices) == 1
    assert reloaded.devices[0].name == "buero"
    assert reloaded.devices[0].friendly_name == "Büro"
    assert reloaded.remote.device_id == "05229657"


def test_save_failure_keeps_existing_config(tmp_path, monkeypatch):
    import src.config as cfgmod

    path = tmp_path / "config.yaml"
    cfg = AppConfig(config_path=str(path))
    cfg.mqtt.host = "good-host"
    assert cfg.save() is True
    original = path.read_text()

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(cfgmod.yaml, "dump", boom)
    cfg.mqtt.host = "bad-host"
    assert cfg.save() is False
    # The previously saved file must be untouched and no temp file left behind.
    assert path.read_text() == original
    leftovers = [p.name for p in tmp_path.iterdir() if p.name != "config.yaml"]
    assert leftovers == []


def test_add_device_deduplicates_by_id(tmp_path):
    cfg = AppConfig(config_path=str(tmp_path / "c.yaml"))
    cfg.add_device(DeviceConfig(name="a", device_id="051EA803"))
    cfg.add_device(DeviceConfig(name="a2", friendly_name="updated", device_id="051EA803"))
    assert len(cfg.devices) == 1
    assert cfg.devices[0].friendly_name == "updated"


def test_get_device_by_id_normalizes(tmp_path):
    cfg = AppConfig(config_path=str(tmp_path / "c.yaml"))
    cfg.add_device(DeviceConfig(name="a", device_id="051EA803"))
    assert cfg.get_device_by_id("05:1e:a8:03") is not None
    assert cfg.get_device_by_id("DEADBEEF") is None


def test_legacy_slaves_are_migrated(tmp_path):
    path = tmp_path / "legacy.yaml"
    path.write_text(
        "devices:\n"
        "  - name: master1\n"
        "    device_id: '051EA5D9'\n"
        "    slaves:\n"
        "      - name: slave1\n"
        "        device_id: '05229657'\n"
    )
    cfg = load_config(str(path))
    names = {d.name for d in cfg.devices}
    assert names == {"master1", "slave1"}
