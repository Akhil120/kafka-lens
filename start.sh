#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
  .venv/bin/pip install -r requirements.txt --quiet
fi

echo ""
echo "  ◆ KafkaLens"
echo "  → http://localhost:8000"
echo ""

.venv/bin/python run.py
