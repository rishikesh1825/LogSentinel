#!/bin/sh
# Runs automatically when the nginx container starts (official image runs
# /docker-entrypoint.d/*.sh). Writes config.js from environment variables so the
# backend URL can be changed without rebuilding the image.
set -e
envsubst '${API_BASE_URL} ${API_KEY} ${GRAFANA_URL} ${MLFLOW_URL}' \
  < /usr/share/nginx/html/config.template.js \
  > /usr/share/nginx/html/config.js
echo "LogSentinel frontend: API_BASE_URL='${API_BASE_URL}'"
