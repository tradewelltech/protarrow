#!/usr/bin/env bash
set -euo pipefail

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT=$(realpath "${DIR}/../")

python3 -m venv test-venv
source test-venv/bin/activate
pip install --upgrade pip
pip install protarrow

protoc --proto_path=${ROOT}/protos/ ${ROOT}/protos/example.proto --python_out ./

PYTHONPATH=./ python ${ROOT}/examples/example.py
