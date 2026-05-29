"""Web-UI for MAICO EnOcean Bridge.

FastAPI + Jinja2 server-side rendering. Provides:
  - Dashboard with device status and quick controls
  - Device pairing page
  - Settings page
  - Device management page
  - REST API for automation
"""

import asyncio
import hashlib
import hmac
import logging
import secrets
import socket
import time
from typing import TYPE_CHECKING

from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import uvicorn

if TYPE_CHECKING:
    from .main import MaicoMqttBridge

logger = logging.getLogger(__name__)

SESSION_COOKIE = "maico_session"
LANG_COOKIE = "maico_lang"
SESSION_MAX_AGE = 86400  # 24 hours

TRANSLATIONS = {
    "de": {
        "lang": "de",
        "brand": "MAICO Controller",
        "dashboard": "Dashboard",
        "pairing": "Anlernen",
        "devices": "Geräte",
        "settings": "Einstellungen",
        "logout": "Logout",
        "mode": "Modus",
        "fan": "Lüfter",
        "airflow": "Gebläse",
        "level": "Stufe",
        "off": "Aus",
        "on": "AN",
        "off_badge": "AUS",
        "paired": "Gepairt",
        "passive": "Passiv",
        "slave": "Slave",
        "unknown": "?",
        "standalone": "Standalone",
        "master": "Master",
        "connection": "Verbindung",
        "role": "Erkannte Rolle",
        "actions": "Aktionen",
        "rename": "Umbenennen",
        "remove": "Entfernen",
        "no_devices": "Keine Geräte konfiguriert.",
        "pair_link": "Geräte anlernen",
        "base_id": "EnOcean Base ID",
        "new_name": "Neuer Anzeigename:",
        "confirm_remove": "Gerät wirklich entfernen?",
        "wrong_password": "Falsches Passwort",
        "password": "Passwort",
        "login": "Anmelden",
        "slave_via": "Slave (via Master)",
        "name": "Name",
        "device_id": "Device ID",
        "save": "Speichern",
        "saved": "Einstellungen gespeichert!",
        # Pair page
        "pair_title": "Geräte anlernen",
        "pair_instructions": "Anleitung",
        "pair_step1": "Am PP 45 RC: Learn-Taste <strong>2s halten</strong> — alle 3 LEDs blinken (Empfangsmodus)",
        "pair_step2": 'Klicke unten auf <strong>"Scan starten"</strong>',
        "pair_step3": "Gerät wird innerhalb von 30 Sekunden erkannt",
        "scan": "Scan",
        "scan_start": "Scan starten",
        "scanning": "Scanne...",
        "scan_sending": "Sende EnOcean Scan-Telegramme... (bis zu 30 Sekunden)",
        "scan_found": "Gerät(e) gefunden!",
        "scan_not_found": "Kein Gerät gefunden. Ist das Gerät im Anlernmodus?",
        "save_device": "Gerät speichern",
        "internal_name": "Name (intern, keine Leerzeichen)",
        "internal_name_placeholder": "z.B. Buero",
        "friendly_name": "Anzeigename",
        "friendly_name_placeholder": "z.B. Büro",
        "device_saved": "gespeichert! Erscheint jetzt in Home Assistant.",
        "name_required": "Name ist erforderlich",
        # Settings page
        "settings_title": "Einstellungen",
        "mqtt_host": "Host",
        "mqtt_port": "Port",
        "mqtt_username": "Username",
        "mqtt_password": "Passwort",
        "mqtt_password_hint": "(unverändert lassen für bisheriges)",
        "bridge": "Bridge",
        "poll_interval": "Poll-Intervall (Sekunden)",
        "web_ui": "Web-UI",
        "new_password": "Neues Passwort (leer lassen für unverändert)",
        "enocean_readonly": "EnOcean (read-only)",
        "serial_port": "Serial Port",
        "language_label": "Sprache (System + Home Assistant)",
        "rls_status": "RLS 45 K",
        "rls_connected": "Verbunden",
        "rls_not_connected": "Nicht verbunden",
        "rls_last_level": "Aktuelle Stufe",
        "rls_global_sync": "RLS steuert alle Geräte",
        "rls_pair_title": "RLS 45 K Fernbedienung anlernen",
        "rls_pair_desc": "Damit die Bridge Befehle der Wandfernbedienung empfängt, muss die RLS mit der Bridge gepairt werden. Die Bridge gibt sich dabei als PP 45 Gerät aus.",
        "rls_pair_btn": "RLS anlernen",
        "rls_pair_step1": 'Klicke unten auf <strong>"RLS anlernen"</strong> (Bridge wartet auf RLS-Scan)',
        "rls_pair_step2": "Am RLS 45 K: <strong>Anlern-Taste drücken</strong> um den Scan zu senden",
        "rls_scanning": "Warte auf RLS-Scan...",
        "rls_found": "RLS gefunden und gepairt!",
        "rls_not_found": "Kein RLS gefunden. Ist die Fernbedienung im Anlernmodus?",
        "rls_device_id": "RLS Device-ID",
        "pp45_pair_title": "PP 45 RC Lüfter anlernen",
        "pp45_pair_desc": "Neue Lüftungsgeräte mit der Bridge verbinden. Nach dem Anlernen kann die Bridge die Geräte direkt steuern.",
        "polling_pause": "Funkverkehr pausieren",
        "polling_resume": "Funkverkehr fortsetzen",
        "polling_paused": "Funkverkehr ist pausiert — Bridge sendet keine Telegramme.",
        "polling_active": "Funkverkehr aktiv",
        "polling_pause_hint": "Vor dem Zurücksetzen der Geräte den Funkverkehr pausieren, damit die Bridge die Geräte nicht sofort wieder anlernt.",
        "pp45_slave_title": "Slave am Master einlernen",
        "pp45_slave_desc": "Ein Lüftungsgerät wird automatisch zum Master, wenn es im Empfangsmodus ein Einlerntelegramm von einem Slave empfängt. Jedem Master kann nur 1 Slave zugeordnet werden.",
        "pp45_slave_step1": "Frontabdeckung an beiden Geräten entfernen",
        "pp45_slave_step2": "<strong>Master</strong> in Einlernmodus: Learn-Taste <strong>2s halten</strong> — alle 3 LEDs blinken (120s Timeout)",
        "pp45_slave_step3": "<strong>Slave</strong> in Einlernmodus: Learn-Taste <strong>2s halten</strong> — alle 3 LEDs blinken",
        "pp45_slave_step4": "Am <strong>Slave</strong> Send-Learn-Modus: Learn-Taste <strong>1× kurz</strong> drücken",
        "pp45_slave_step5": "Am <strong>Slave</strong> Telegramm senden: Learn-Taste <strong>ca. 5s halten</strong> — LEDs leuchten kurz auf",
        "pp45_slave_step6": "<strong>30–40 Sekunden warten</strong> — alle LEDs an Master und Slave schalten aus",
        "pp45_reset_title": "Tastenkombinationen Learn-Taste",
        "pp45_reset_desc": "Alle Kombinationen beginnen und enden mit Learn-Taste 2s lang halten. Bestätigung abwarten!",
        "pp45_reset_warning": "Nach dem Löschen ist das Gerät 120 Sekunden im Empfangsmodus. In dieser Zeit darf kein Controller (Bridge, RLS) senden, da das Gerät sich sonst sofort wieder anlernt. Vorher den Funkverkehr oben pausieren!",
        "pp45_reset_learn": "<strong>Anlernen (1×):</strong> 2s lang, <strong>1× kurz</strong>, 2s lang — Bestätigung: 3 LEDs 1× kurz. Achtung: nicht mit Speicher löschen verwechseln!",
        "pp45_reset_delete": "<strong>Speicher löschen (2×):</strong> 2s lang, <strong>2× kurz</strong>, 2s lang — Bestätigung: 3 LEDs 1× <strong>lang</strong>. Löscht alle Pairings inkl. Master-Slave-Verbund. An <strong>beiden</strong> Geräten eines Paares durchführen + danach Strom kurz trennen!",
        "pp45_reset_rep_off": "<strong>Repeater aus:</strong> 2s lang, <strong>3× kurz</strong>, 2s lang",
        "pp45_reset_rep1": "<strong>Repeater Stufe 1:</strong> 2s lang, 3× kurz, <strong>1× kurz</strong>, 2s lang",
        "pp45_reset_rep2": "<strong>Repeater Stufe 2:</strong> 2s lang, 3× kurz, <strong>2× kurz</strong>, 2s lang",
        "mode_btn_heat": "Wärmetauscher",
        "mode_btn_summer": "Sommer",
        "mode_btn_sleep": "Schlafen",
        "mode_btn_boost": "Stoßlüften",
        "mode_labels": {
            "off": "Aus",
            "heat_exchanger": "Wärmetauscher",
            "summer": "Sommer",
            "sleep_heat": "Schlaf (WT)",
            "sleep_summer": "Schlaf (Sommer)",
            "boost": "Stoßlüftung",
        },
        "direction_labels": {
            "inflow": "Zuluft",
            "exhaust": "Abluft",
            "unknown": "Unbekannt",
        },
    },
    "en": {
        "lang": "en",
        "brand": "MAICO Controller",
        "dashboard": "Dashboard",
        "pairing": "Pairing",
        "devices": "Devices",
        "settings": "Settings",
        "logout": "Logout",
        "mode": "Mode",
        "fan": "Fan",
        "airflow": "Airflow",
        "level": "Level",
        "off": "Off",
        "on": "ON",
        "off_badge": "OFF",
        "paired": "Paired",
        "passive": "Passive",
        "slave": "Slave",
        "unknown": "?",
        "standalone": "Standalone",
        "master": "Master",
        "connection": "Connection",
        "role": "Detected Role",
        "actions": "Actions",
        "rename": "Rename",
        "remove": "Remove",
        "no_devices": "No devices configured.",
        "pair_link": "Pair devices",
        "base_id": "EnOcean Base ID",
        "new_name": "New display name:",
        "confirm_remove": "Really remove device?",
        "wrong_password": "Wrong password",
        "password": "Password",
        "login": "Log in",
        "slave_via": "Slave (via Master)",
        "name": "Name",
        "device_id": "Device ID",
        "save": "Save",
        "saved": "Settings saved!",
        # Pair page
        "pair_title": "Pair Devices",
        "pair_instructions": "Instructions",
        "pair_step1": "On PP 45 RC: <strong>hold Learn button for 2s</strong> — all 3 LEDs blink (receive mode)",
        "pair_step2": 'Click <strong>"Start Scan"</strong> below',
        "pair_step3": "Device is detected within 30 seconds",
        "scan": "Scan",
        "scan_start": "Start Scan",
        "scanning": "Scanning...",
        "scan_sending": "Sending EnOcean scan telegrams... (up to 30 seconds)",
        "scan_found": "Device(s) found!",
        "scan_not_found": "No device found. Is the device in pairing mode?",
        "save_device": "Save Device",
        "internal_name": "Name (internal, no spaces)",
        "internal_name_placeholder": "e.g. Office",
        "friendly_name": "Display Name",
        "friendly_name_placeholder": "e.g. Office",
        "device_saved": "saved! Now visible in Home Assistant.",
        "name_required": "Name is required",
        # Settings page
        "settings_title": "Settings",
        "mqtt_host": "Host",
        "mqtt_port": "Port",
        "mqtt_username": "Username",
        "mqtt_password": "Password",
        "mqtt_password_hint": "(leave empty to keep current)",
        "bridge": "Bridge",
        "poll_interval": "Poll Interval (seconds)",
        "web_ui": "Web UI",
        "new_password": "New password (leave empty to keep current)",
        "enocean_readonly": "EnOcean (read-only)",
        "serial_port": "Serial Port",
        "language_label": "Language (System + Home Assistant)",
        "rls_status": "RLS 45 K",
        "rls_connected": "Connected",
        "rls_not_connected": "Not connected",
        "rls_last_level": "Current level",
        "rls_global_sync": "RLS controls all devices",
        "rls_pair_title": "RLS 45 K Remote Control",
        "rls_pair_desc": "Pair the wall remote with the bridge so it can receive RLS commands. The bridge pretends to be a PP 45 device.",
        "rls_pair_btn": "Pair RLS",
        "rls_pair_step1": 'Click <strong>"Pair RLS"</strong> below (bridge waits for RLS scan)',
        "rls_pair_step2": "On RLS 45 K: <strong>press learn button</strong> to send the scan",
        "rls_scanning": "Waiting for RLS scan...",
        "rls_found": "RLS found and paired!",
        "rls_not_found": "No RLS found. Is the remote in pairing mode?",
        "rls_device_id": "RLS Device ID",
        "pp45_pair_title": "PP 45 RC Ventilation Units",
        "pp45_pair_desc": "Pair new ventilation units with the bridge. After pairing, the bridge can control the devices directly.",
        "polling_pause": "Pause radio traffic",
        "polling_resume": "Resume radio traffic",
        "polling_paused": "Radio traffic paused — bridge is not sending any telegrams.",
        "polling_active": "Radio traffic active",
        "polling_pause_hint": "Pause radio traffic before resetting devices, so the bridge does not re-pair them immediately.",
        "pp45_slave_title": "Pair slave to master",
        "pp45_slave_desc": "A device automatically becomes a master when it receives a pairing telegram from a slave in receive mode. Each master supports only 1 slave.",
        "pp45_slave_step1": "Remove front cover from both devices",
        "pp45_slave_step2": "<strong>Master</strong> learn mode: hold Learn button <strong>2s</strong> — all 3 LEDs blink (120s timeout)",
        "pp45_slave_step3": "<strong>Slave</strong> learn mode: hold Learn button <strong>2s</strong> — all 3 LEDs blink",
        "pp45_slave_step4": "On <strong>Slave</strong> send-learn mode: press Learn button <strong>1× short</strong>",
        "pp45_slave_step5": "On <strong>Slave</strong> send telegram: hold Learn button <strong>~5s</strong> — LEDs flash briefly",
        "pp45_slave_step6": "<strong>Wait 30–40 seconds</strong> — all LEDs on master and slave turn off",
        "pp45_reset_title": "Learn button combinations",
        "pp45_reset_desc": "All combinations start and end with holding Learn button for 2s.",
        "pp45_reset_warning": "After clearing, the device listens for 120 seconds. No controller (bridge, RLS) may send during this time, or the device will re-pair automatically. Pause radio traffic above first!",
        "pp45_reset_learn": "<strong>Pairing (1×):</strong> hold 2s, <strong>1× short</strong>, hold 2s — confirmation: 3 LEDs 1× short. Do not confuse with clear memory!",
        "pp45_reset_delete": "<strong>Clear memory (2×):</strong> hold 2s, <strong>2× short</strong>, hold 2s — confirmation: 3 LEDs 1× <strong>long</strong>. Clears all pairings incl. master-slave. Do on <strong>both</strong> devices of a pair + briefly disconnect power afterwards!",
        "pp45_reset_rep_off": "<strong>Repeater off:</strong> hold 2s, <strong>3× short</strong>, hold 2s",
        "pp45_reset_rep1": "<strong>Repeater level 1:</strong> hold 2s, 3× short, <strong>1× short</strong>, hold 2s",
        "pp45_reset_rep2": "<strong>Repeater level 2:</strong> hold 2s, 3× short, <strong>2× short</strong>, hold 2s",
        "mode_btn_heat": "Heat Exchanger",
        "mode_btn_summer": "Summer",
        "mode_btn_sleep": "Sleep",
        "mode_btn_boost": "Boost",
        "mode_labels": {
            "off": "Off",
            "heat_exchanger": "Heat Exchanger",
            "summer": "Summer",
            "sleep_heat": "Sleep (HE)",
            "sleep_summer": "Sleep (Summer)",
            "boost": "Boost",
        },
        "direction_labels": {
            "inflow": "Inflow",
            "exhaust": "Exhaust",
            "unknown": "Unknown",
        },
    },
}


def _get_lang(request: Request, default: str = "de") -> dict:
    lang = request.cookies.get(LANG_COOKIE, default)
    return TRANSLATIONS.get(lang, TRANSLATIONS["de"])


async def _parse_json(request: Request) -> dict | None:
    """Parse JSON body, return None on failure."""
    try:
        return await request.json()
    except Exception:
        return None


def create_web_app(bridge: "MaicoMqttBridge") -> FastAPI:
    app = FastAPI(title="MAICO Controller", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory="templates")
    default_lang = bridge.config.language

    # Session store: token -> expiry timestamp
    sessions: dict[str, float] = {}

    def _check_auth(request: Request) -> bool:
        """Check if request is authenticated. Returns True if no password set."""
        password = bridge.config.web.password
        if not password:
            return True
        token = request.cookies.get(SESSION_COOKIE)
        if token and token in sessions:
            if sessions[token] > time.time():
                return True
            sessions.pop(token, None)
        return False

    def _create_session() -> str:
        token = secrets.token_urlsafe(32)
        sessions[token] = time.time() + SESSION_MAX_AGE
        return token

    # --- Auth ---

    @app.get("/lang/{lang}")
    async def set_language(lang: str, request: Request):
        if lang not in TRANSLATIONS:
            lang = "de"
        referer = request.headers.get("referer", "/")
        response = RedirectResponse(referer, status_code=302)
        response.set_cookie(LANG_COOKIE, lang, max_age=365 * 86400)
        return response

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        if _check_auth(request):
            return RedirectResponse("/", status_code=302)
        t = _get_lang(request, default_lang)
        return templates.TemplateResponse(request, "login.html", {"error": "", "t": t})

    @app.post("/login")
    async def login(request: Request, password: str = Form(...)):
        expected = bridge.config.web.password
        if hmac.compare_digest(password, expected):
            token = _create_session()
            response = RedirectResponse("/", status_code=302)
            response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_MAX_AGE, httponly=True)
            return response
        t = _get_lang(request, default_lang)
        return templates.TemplateResponse(request, "login.html", {
            "error": t["wrong_password"], "t": t,
        })

    @app.get("/logout")
    async def logout(request: Request):
        token = request.cookies.get(SESSION_COOKIE)
        if token:
            sessions.pop(token, None)
        response = RedirectResponse("/login", status_code=302)
        response.delete_cookie(SESSION_COOKIE)
        return response

    # --- Dashboard ---

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)

        all_devs = {}
        for dev in bridge.config.devices:
            state = bridge.states.get(dev.name)
            conn = bridge.device_status.get(dev.name)
            all_devs[dev.name] = {
                "name": dev.name,
                "friendly_name": dev.friendly_name or dev.name,
                "device_id": dev.device_id,
                "state": state.to_dict() if state else None,
                "connection": conn.to_dict() if conn else None,
            }

        # Group into pairs (master+slaves) and standalone
        pairs = []       # [{"master": dev, "slaves": [dev, ...]}, ...]
        standalone = []  # [dev, ...]
        used = set()

        # Find masters and attach their slaves
        for dev in all_devs.values():
            conn = dev.get("connection")
            if conn and conn.get("detected_role") == "master" and conn.get("syncs_to"):
                slave_id = conn["syncs_to"]
                slave_name = bridge._id_to_name.get(slave_id)
                slaves = []
                if slave_name and slave_name in all_devs:
                    slaves.append(all_devs[slave_name])
                    used.add(slave_name)
                pairs.append({"master": dev, "slaves": slaves})
                used.add(dev["name"])

        # Remaining devices are standalone
        for dev in all_devs.values():
            if dev["name"] not in used:
                standalone.append(dev)

        t = _get_lang(request, default_lang)
        rls_info = None
        if bridge.config.remote.device_id:
            rls_last = bridge._last_rls_state
            rls_info = {
                "device_id": bridge.config.remote.device_id,
                "level": rls_last.fan_level if rls_last else None,
                "mode": rls_last.mode.value if rls_last else None,
                "sync_enabled": bridge.config.rls_global_sync,
            }

        return templates.TemplateResponse(request, "dashboard.html", {
            "pairs": pairs,
            "standalone": standalone,
            "bridge_base_id": bridge.serial.base_id_str if bridge.serial else "N/A",
            "id_to_name": bridge._id_to_name,
            "t": t,
            "mode_labels": t["mode_labels"],
            "direction_labels": t["direction_labels"],
            "rls": rls_info,
        })

    # --- Pairing ---

    @app.get("/pair", response_class=HTMLResponse)
    async def pair_page(request: Request):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)
        t = _get_lang(request, default_lang)
        return templates.TemplateResponse(request, "pair.html", {
            "t": t,
            "rls_device_id": bridge.config.remote.device_id or "",
            "polling_paused": bridge._polling_paused,
        })

    @app.post("/api/pair")
    async def api_pair(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        from .teach_in import run_teach_in
        # Enable passive auto-discovery only for the duration of the scan window,
        # so a device that announces itself via traffic during pairing is picked up
        # while normal operation can never silently register ghost devices.
        bridge._discovery_enabled = True
        try:
            result = await run_teach_in(bridge.serial, timeout=30)
        finally:
            bridge._discovery_enabled = False
        return JSONResponse({
            "status": result.status,
            "found_devices": result.found_devices,
            "error": result.error,
        })

    @app.post("/api/pair/save")
    async def api_pair_save(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        body = await _parse_json(request)
        if body is None:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        device_id = body.get("device_id", "")
        name = body.get("name", "")
        friendly_name = body.get("friendly_name", name)

        if not device_id or not name:
            return JSONResponse({"error": "device_id and name required"}, status_code=400)

        from .config import DeviceConfig
        new_device = DeviceConfig(
            name=name,
            friendly_name=friendly_name,
            device_id=device_id,
        )
        bridge.config.add_device(new_device)
        bridge.config.save()

        # Update bridge mappings
        dev_id_clean = device_id.upper().replace(":", "")
        bridge._id_to_name[dev_id_clean] = name
        bridge._name_to_config[name] = new_device
        from .maico_protocol import VentilationState
        bridge._states[name] = VentilationState()

        # Publish MQTT discovery
        bridge.mqtt.publish_device_discovery(new_device)

        # Set default level 2
        bridge.set_level(name, 2)

        return JSONResponse({"status": "saved", "name": name})

    # --- Polling control ---

    @app.post("/api/polling/pause")
    async def api_polling_pause(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        bridge._polling_paused = True
        logger.info("Polling paused via Web-UI")
        return JSONResponse({"status": "paused"})

    @app.post("/api/polling/resume")
    async def api_polling_resume(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        bridge._polling_paused = False
        logger.info("Polling resumed via Web-UI")
        return JSONResponse({"status": "active"})

    @app.get("/api/polling/status")
    async def api_polling_status(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return JSONResponse({"paused": bridge._polling_paused})

    # --- RLS Pairing ---

    @app.post("/api/pair/rls")
    async def api_pair_rls(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        from .teach_in import run_rls_teach_in
        result = await run_rls_teach_in(bridge, timeout=30)
        return JSONResponse({
            "status": result.status,
            "found_devices": result.found_devices,
            "error": result.error,
        })

    # --- Settings ---

    @app.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)

        t = _get_lang(request, default_lang)
        return templates.TemplateResponse(request, "settings.html", {
            "config": bridge.config,
            "base_id": bridge.serial.base_id_str if bridge.serial else "N/A",
            "t": t,
        })

    @app.post("/settings")
    async def save_settings(
        request: Request,
        mqtt_host: str = Form(""),
        mqtt_port: int = Form(1883),
        mqtt_username: str = Form(""),
        mqtt_password: str = Form(""),
        poll_interval: int = Form(10),
        web_password: str = Form(""),
        language: str = Form("de"),
        rls_global_sync: str = Form(""),
    ):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)

        if mqtt_host:
            bridge.config.mqtt.host = mqtt_host
        bridge.config.mqtt.port = max(1, min(65535, mqtt_port))
        if mqtt_username:
            bridge.config.mqtt.username = mqtt_username
        if mqtt_password:
            bridge.config.mqtt.password = mqtt_password
        bridge.config.poll_interval = max(5, min(3600, poll_interval))
        if web_password:
            bridge.config.web.password = web_password
        if language in ("de", "en"):
            bridge.config.language = language
        bridge.config.rls_global_sync = rls_global_sync == "on"

        bridge.config.save()

        return RedirectResponse("/settings?saved=1", status_code=302)

    # --- Devices ---

    @app.get("/devices", response_class=HTMLResponse)
    async def devices_page(request: Request):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)

        # Build device list with slave indentation info
        all_devices = []
        for dev in bridge.config.devices:
            conn = bridge.device_status.get(dev.name)
            all_devices.append({
                "name": dev.name,
                "friendly_name": dev.friendly_name or dev.name,
                "device_id": dev.device_id,
                "connection": conn.to_dict() if conn else None,
            })

        # Sort: masters first, then their slaves directly after
        masters = []
        slaves = {}  # master_device_id -> [slave_devs]
        standalone = []
        for dev in all_devices:
            conn = dev.get("connection")
            if conn and conn.get("detected_role") == "slave" and conn.get("synced_from"):
                slaves.setdefault(conn["synced_from"], []).append(dev)
            elif conn and conn.get("detected_role") == "master":
                masters.append(dev)
            else:
                standalone.append(dev)

        ordered_devices = []
        for master in masters:
            master["is_slave"] = False
            ordered_devices.append(master)
            master_id = master["device_id"].upper().replace(":", "")
            for slave in slaves.pop(master_id, []):
                slave["is_slave"] = True
                ordered_devices.append(slave)
        # Remaining slaves without a known master
        for slave_list in slaves.values():
            for slave in slave_list:
                slave["is_slave"] = True
                ordered_devices.append(slave)
        for dev in standalone:
            dev["is_slave"] = False
            ordered_devices.append(dev)

        t = _get_lang(request, default_lang)
        return templates.TemplateResponse(request, "devices.html", {
            "devices": ordered_devices,
            "id_to_name": bridge._id_to_name,
            "t": t,
        })

    @app.post("/api/device/{name}/remove")
    async def api_device_remove(name: str, request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        device = bridge.config.get_device_by_name(name)
        if device:
            bridge.mqtt.remove_device_discovery(device)
            bridge.mqtt.clear_device_topics(device.name)
            # Clean up internal mappings
            dev_id = device.device_id.upper().replace(":", "")
            bridge._id_to_name.pop(dev_id, None)
            bridge._name_to_config.pop(device.name, None)
            bridge._states.pop(device.name, None)
            bridge._device_status.pop(device.name, None)
            bridge._state_known.discard(device.name)
            bridge.config.remove_device(name)
            bridge.config.save()
            return JSONResponse({"status": "removed"})
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.post("/api/device/{name}/rename")
    async def api_device_rename(name: str, request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        body = await _parse_json(request)
        if body is None:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        new_friendly = body.get("friendly_name", "")
        new_internal = body.get("name", "")
        device = bridge.config.get_device_by_name(name)
        if not device or (not new_friendly and not new_internal):
            return JSONResponse({"error": "not found or no name"}, status_code=400)

        # Remove old discovery entries first
        bridge.mqtt.remove_device_discovery(device)

        old_name = device.name
        if new_friendly:
            device.friendly_name = new_friendly
        if new_internal and new_internal != old_name:
            # Update internal name — migrate all bridge mappings
            device.name = new_internal
            dev_id = device.device_id.upper().replace(":", "")
            bridge._id_to_name[dev_id] = new_internal
            bridge._name_to_config.pop(old_name, None)
            bridge._name_to_config[new_internal] = device
            state = bridge._states.pop(old_name, None)
            if state:
                bridge._states[new_internal] = state
            status = bridge._device_status.pop(old_name, None)
            if status:
                bridge._device_status[new_internal] = status
            # Clean old MQTT retained state topics
            bridge.mqtt.clear_device_topics(old_name)

        bridge.config.save()
        # Publish fresh discovery with new name
        bridge.mqtt.publish_device_discovery(device)
        return JSONResponse({"status": "renamed"})

    # --- API ---

    @app.get("/api/devices")
    async def api_devices(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        result = []
        for dev in bridge.config.devices:
            state = bridge.states.get(dev.name)
            conn = bridge.device_status.get(dev.name)
            result.append({
                "name": dev.name,
                "friendly_name": dev.friendly_name,
                "device_id": dev.device_id,
                "state": state.to_dict() if state else None,
                "connection": conn.to_dict() if conn else None,
            })
        return JSONResponse(result)

    @app.post("/api/device/{name}/level")
    async def api_set_level(name: str, request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        body = await _parse_json(request)
        if body is None:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        level = body.get("level", 0)
        ok = bridge.set_level(name, int(level))
        if ok:
            return JSONResponse({"status": "ok", "level": level})
        return JSONResponse({"error": "failed"}, status_code=400)

    @app.post("/api/device/{name}/mode")
    async def api_set_mode(name: str, request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        from .maico_protocol import VentilationMode
        body = await _parse_json(request)
        if body is None:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        mode_str = body.get("mode", "")
        mode_map = {
            "heat_exchanger": VentilationMode.HEAT_EXCHANGER,
            "summer": VentilationMode.SUMMER,
            "sleep": VentilationMode.SLEEP_HEAT,
            "boost": VentilationMode.BOOST,
        }
        mode = mode_map.get(mode_str)
        if mode:
            ok = bridge.set_mode(name, mode)
            if ok:
                return JSONResponse({"status": "ok", "mode": mode_str})
        return JSONResponse({"error": "invalid mode"}, status_code=400)

    @app.post("/api/device/{name}/power")
    async def api_set_power(name: str, request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        body = await _parse_json(request)
        if body is None:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        on = body.get("on", False)
        ok = bridge.set_power(name, bool(on))
        if ok:
            return JSONResponse({"status": "ok", "on": on})
        return JSONResponse({"error": "failed"}, status_code=400)

    @app.get("/api/status")
    async def api_status(request: Request):
        return JSONResponse({
            "status": "online",
            "base_id": bridge.serial.base_id_str if bridge.serial else None,
            "device_count": len(bridge.config.devices),
            "poll_interval": bridge.config.poll_interval,
        })

    @app.get("/healthz")
    async def healthz():
        """Liveness probe for Docker/Kubernetes. No auth required."""
        serial_ok = bool(bridge.serial and bridge.serial.base_id)
        mqtt_ok = bool(bridge.mqtt._connected)
        poll_ok = bool(bridge._poll_task and not bridge._poll_task.done())
        healthy = serial_ok and mqtt_ok and poll_ok
        body = {
            "healthy": healthy,
            "serial": serial_ok,
            "mqtt": mqtt_ok,
            "poll": poll_ok,
        }
        return JSONResponse(body, status_code=200 if healthy else 503)

    return app


async def start_web_server(app: FastAPI, port: int) -> None:
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


def register_mdns(hostname: str, port: int) -> None:
    """Register mDNS service for the web-UI."""
    try:
        from zeroconf import ServiceInfo, Zeroconf
        import socket

        local_ip = _get_local_ip()
        info = ServiceInfo(
            "_http._tcp.local.",
            f"{hostname}._http._tcp.local.",
            addresses=[socket.inet_aton(local_ip)],
            port=port,
            properties={"path": "/", "name": "MAICO Controller"},
            server=f"{hostname}.local.",
        )
        zc = Zeroconf()
        zc.register_service(info)
        logger.info("mDNS: registered %s.local:%d (%s)", hostname, port, local_ip)
    except ImportError:
        logger.debug("zeroconf not installed, mDNS disabled")
    except Exception:
        logger.debug("mDNS registration failed", exc_info=True)


def _get_local_ip() -> str:
    """Get the local IP address."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("1.1.1.1", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
