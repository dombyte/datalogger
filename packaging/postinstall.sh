#!/bin/sh
# Pick up the new or changed unit. The service is not enabled: the config must be written
# first. On an upgrade a running service is restarted with the new binary.
set -e
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload || true
	systemctl try-restart datalogger.service || true
fi
