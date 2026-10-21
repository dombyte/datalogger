#!/bin/sh
# On removal (deb: "remove", rpm: 0 packages left), not on upgrade, stop and disable the
# service. It drains its outputs on SIGTERM, so no queued reading is lost.
set -e
case "$1" in
remove | 0)
	if [ -d /run/systemd/system ]; then
		systemctl disable --now datalogger.service >/dev/null 2>&1 || true
	fi
	;;
esac
