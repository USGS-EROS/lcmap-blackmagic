#!/usr/bin/env bash
set -e

gunicorn --max_requests $max_requests --timeout $WORKER_TIMEOUT --bind :$HTTP_PORT --workers $WORKERS blackmagic.app:app
