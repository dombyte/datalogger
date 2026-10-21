#!/bin/sh
# Create the service user before the files are unpacked (/etc/datalogger belongs to its
# group). It is kept on removal: data in /var/lib/datalogger stays owned by it.
set -e
if ! getent group datalogger >/dev/null; then
	groupadd --system datalogger
fi
if ! getent passwd datalogger >/dev/null; then
	useradd --system --gid datalogger --home-dir /var/lib/datalogger --no-create-home \
		--shell /usr/sbin/nologin --comment "Datalogger" datalogger
fi
# Modbus RTU: serial ports belong to the dialout group.
if getent group dialout >/dev/null; then
	usermod --append --groups dialout datalogger
fi
