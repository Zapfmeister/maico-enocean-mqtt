"""Tests for DeviceStatus role detection and stale-role expiry."""

import time

from src.main import DeviceStatus, SYNC_ROLE_TIMEOUT


def test_role_master_and_slave():
    now = time.time()
    master = DeviceStatus(syncs_to="05229657", last_sync=now)
    assert master.detected_role == "master"

    slave = DeviceStatus(synced_from="051EA5D9", last_sync=now)
    assert slave.detected_role == "slave"

    assert DeviceStatus().detected_role == "standalone"


def test_stale_role_reads_as_standalone_without_mutation():
    old = time.time() - (SYNC_ROLE_TIMEOUT + 10)
    ds = DeviceStatus(syncs_to="05229657", last_sync=old)
    # Reading the role must NOT clear the underlying fields (no side effects).
    assert ds.detected_role == "standalone"
    assert ds.syncs_to == "05229657"
    assert ds.last_sync == old


def test_expire_stale_role_clears_fields():
    old = time.time() - (SYNC_ROLE_TIMEOUT + 10)
    ds = DeviceStatus(syncs_to="05229657", synced_from="051EA5D9", last_sync=old)
    ds.expire_stale_role()
    assert ds.syncs_to is None
    assert ds.synced_from is None
    assert ds.last_sync == 0.0


def test_expire_keeps_fresh_role():
    now = time.time()
    ds = DeviceStatus(syncs_to="05229657", last_sync=now)
    ds.expire_stale_role(now)
    assert ds.syncs_to == "05229657"
    assert ds.detected_role == "master"
