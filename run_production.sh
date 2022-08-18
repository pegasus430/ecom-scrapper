#!/usr/bin/env bash
set -e # Exit with nonzero exit code if anything fails

cd /app
/usr/local/bin/python content_analytics/main.py
