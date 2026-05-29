"""Real EnOcean ESP3 frames captured from live MAICO PP 45 RC traffic.

Source: capture_01.txt ("Schalte alle Stufen durch" — cycling through levels).
These are used as regression fixtures so the ghost-device / parser bugs that
were fixed across several commits can never silently reappear.
"""


def hexs(s: str) -> bytes:
    """Convert a space-separated hex string into bytes."""
    return bytes(int(b, 16) for b in s.split())


# 27 20 set-level command, level 2 (E2 = heat-exchanger lvl 2)
# sender 05A2B6C1 (bridge base), dest 051EA803
SETLEVEL_L2 = hexs(
    "55 00 0B 07 01 80 D1 27 20 E2 00 00 05 A2 B6 C1 81 00 "
    "05 1E A8 03 3D 00 33"
)

# 27 10 status report, stufe 0x02 -> heat-exchanger level 2 exhaust
# sender 051EA803, dest 05A2B6C1
STATUS_L2 = hexs(
    "55 00 0D 07 01 FD D1 27 10 02 E0 00 00 00 05 1E A8 03 81 00 "
    "05 A2 B6 C1 2E 00 04"
)

# 27 10 status report, stufe 0x05 -> heat-exchanger level 5 exhaust
STATUS_L5 = hexs(
    "55 00 0D 07 01 FD D1 27 10 05 E0 00 00 00 05 A2 A8 F2 81 00 "
    "05 A2 B6 C1 4C 00 19"
)

# 27 10 status report, stufe 0x00 -> off
STATUS_OFF = hexs(
    "55 00 0D 07 01 FD D1 27 10 00 E0 00 00 00 05 A2 A8 F2 81 00 "
    "05 A2 B6 C1 4A 00 51"
)

# 27 00 master-slave sync, status byte 0x22 -> inflow level 2
# sender 051EA5D9 (master), dest 05229657 (slave)
SYNC_INFLOW_L2 = hexs(
    "55 00 0B 07 01 80 D1 27 00 22 31 00 05 1E A5 D9 81 00 "
    "05 22 96 57 3D 00 E1"
)

# 27 00 master-slave sync, status byte 0x01 -> exhaust level 1
SYNC_EXHAUST_L1 = hexs(
    "55 00 0B 07 01 80 D1 27 00 01 3B 00 05 1E A5 D9 81 00 "
    "05 22 96 57 40 00 40"
)

# 27 70 slave ACK (sender 05229657)
SLAVE_ACK = hexs(
    "55 00 08 07 01 3D D1 27 70 05 22 96 57 81 00 "
    "05 1E A5 D9 4F 00 77"
)

# Three 27 20 set-level packets concatenated in one serial read (line 4 of
# the capture) — exercises multi-packet parsing in a single buffer.
THREE_SETLEVEL = hexs(
    "55 00 0B 07 01 80 D1 27 20 E2 00 00 05 A2 B6 C1 81 00 05 1E A8 03 3D 00 33 "
    "55 00 0B 07 01 80 D1 27 20 E2 00 00 05 A2 B6 C1 81 00 05 1E A5 D9 3D 00 65 "
    "55 00 0B 07 01 80 D1 27 20 E2 00 00 05 A2 B6 C1 81 00 05 1E F6 BA 3D 00 DB"
)
