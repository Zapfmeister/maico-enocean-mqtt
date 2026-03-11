#!/usr/bin/env bash
set -e

# HA Add-on mode: read options from Supervisor API via bashio
if command -v bashio &> /dev/null; then
    SERIAL_PORT=$(bashio::config 'serial_port')
    POLL_INTERVAL=$(bashio::config 'poll_interval')
    WEB_PASSWORD=$(bashio::config 'web_password')

    # MQTT credentials from HA Supervisor
    if bashio::services.available "mqtt"; then
        export MQTT_HOST=$(bashio::services mqtt "host")
        export MQTT_PORT=$(bashio::services mqtt "port")
        export MQTT_USERNAME=$(bashio::services mqtt "username")
        export MQTT_PASSWORD=$(bashio::services mqtt "password")
    fi

    export MAICO_SERIAL_PORT="${SERIAL_PORT}"
    export MAICO_POLL_INTERVAL="${POLL_INTERVAL}"
    export MAICO_WEB_PASSWORD="${WEB_PASSWORD}"
    export MAICO_WEB_PORT="8080"
fi

exec python3 -m src.main "$@"
