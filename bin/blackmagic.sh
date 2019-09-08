#!/usr/bin/env bash
set -e

gunicorn --log-level warning --max-requests $MAX_REQUESTS --timeout $WORKER_TIMEOUT --bind :$HTTP_PORT --workers $WORKERS blackmagic.app:app
