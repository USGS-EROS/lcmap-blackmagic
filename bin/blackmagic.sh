#!/usr/bin/env bash
set -e
echo "WORKER_TIMEOUT:$WORKER_TIMEOUT"
echo "HTTP_PORT:$HTTP_PORT"
echo "WORKERS:$WORKERS"

gunicorn --timeout $WORKER_TIMEOUT --bind :$HTTP_PORT --workers $WORKERS blackmagic.app:app
