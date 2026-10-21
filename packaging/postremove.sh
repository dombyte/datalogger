#!/bin/sh
# Forget the removed unit.
set -e
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload || true
fi
