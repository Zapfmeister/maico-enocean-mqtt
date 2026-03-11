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
        "pair_step1": "Am PP 45 RC: Learn-Taste <strong>2 Sekunden halten</strong> bis alle 3 LEDs blinken",
        "pair_step2": 'Klicke unten auf <strong>"Scan starten"</strong>',
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
        "pair_step1": "On PP 45 RC: <strong>Hold Learn button for 2 seconds</strong> until all 3 LEDs blink",
        "pair_step2": 'Click <strong>"Start Scan"</strong> below',
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
        return templates.TemplateResponse("login.html", {"request": request, "error": "", "t": t})

    @app.post("/login")
    async def login(request: Request, password: str = Form(...)):
        expected = bridge.config.web.password
        if hmac.compare_digest(password, expected):
            token = _create_session()
            response = RedirectResponse("/", status_code=302)
            response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_MAX_AGE, httponly=True)
            return response
        t = _get_lang(request, default_lang)
        return templates.TemplateResponse("login.html", {
            "request": request, "error": t["wrong_password"], "t": t,
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
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "pairs": pairs,
            "standalone": standalone,
            "bridge_base_id": bridge.serial.base_id_str if bridge.serial else "N/A",
            "id_to_name": bridge._id_to_name,
            "t": t,
            "mode_labels": t["mode_labels"],
            "direction_labels": t["direction_labels"],
        })

    # --- Pairing ---

    @app.get("/pair", response_class=HTMLResponse)
    async def pair_page(request: Request):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)
        t = _get_lang(request, default_lang)
        return templates.TemplateResponse("pair.html", {"request": request, "t": t})

    @app.post("/api/pair")
    async def api_pair(request: Request):
        if not _check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        from .teach_in import run_teach_in
        result = await run_teach_in(bridge.serial, timeout=30)
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

        return JSONResponse({"status": "saved", "name": name})

    # --- Settings ---

    @app.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        if not _check_auth(request):
            return RedirectResponse("/login", status_code=302)

        t = _get_lang(request, default_lang)
        return templates.TemplateResponse("settings.html", {
            "request": request,
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
        return templates.TemplateResponse("devices.html", {
            "request": request,
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
        new_name = body.get("friendly_name", "")
        device = bridge.config.get_device_by_name(name)
        if device and new_name:
            device.friendly_name = new_name
            bridge.config.save()
            bridge.mqtt.publish_device_discovery(device)
            return JSONResponse({"status": "renamed"})
        return JSONResponse({"error": "not found or no name"}, status_code=400)

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
