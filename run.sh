#!/bin/sh
set -e

(
  cd ./app
  go build -o ../tmp
)

exec /tmp/app "$@"