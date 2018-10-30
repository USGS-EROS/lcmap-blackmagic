#!/usr/bin/env bash
set -e

gunicorn --timeout $WORKER_TIMEOUT --bind :$HTTP_PORT --workers $WORKERS blackmagic.app:app
