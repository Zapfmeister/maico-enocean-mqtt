"""Wiring for Feature #33: the bridge drives role-aware HA discovery, re-shapes
on role changes (confirm-before-emit), and restores roles on restart
(persist-then-reconcile)."""
import time

from tests.fakes import make_bridge
from tests.test_bridge import sync_pkt


def test_sync_groups_slave_under_master():
    b = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    b._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))
    assert b.mqtt.discovery_roles.get("leo") == "master"
    assert b.mqtt.discovery_roles.get("schlaf") == "slave"


def test_decay_reverts_slave_to_standalone():
    b = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    b._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))
    # Sync gone stale → run expiry + refresh exactly as the availability loop does.
    for ds in b._device_status.values():
        ds.last_sync = time.time() - 10_000
    for name in ("leo", "schlaf"):
        b._device_status[name].expire_stale_role()
        b.refresh_discovery(name)
    assert b.mqtt.discovery_roles.get("schlaf") == "standalone"
    assert b.mqtt.discovery_roles.get("leo") == "standalone"


def test_refresh_idempotent_without_change():
    b = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    b._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))
    n = len(b.mqtt.discovery)
    b.refresh_discovery("schlaf")  # role unchanged
    assert len(b.mqtt.discovery) == n, "confirm-before-emit: kein erneutes Publish ohne Form-Änderung"


def test_persist_then_reconcile_restores_slave_shape(tmp_path):
    roles = str(tmp_path / "roles.json")
    b1 = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    b1._roles_path = roles
    b1._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))  # detect → _save_roles
    # 'Neustart': frische Bridge, gleiche persistierte Rollen, restore VOR Publish.
    b2 = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    b2._roles_path = roles
    b2._restore_roles()
    b2.publish_discovery("schlaf")
    assert b2.mqtt.discovery_roles.get("schlaf") == "slave", "Slave-Form sofort nach Restore (kein Transient)"
