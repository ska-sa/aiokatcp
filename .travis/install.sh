#!/bin/bash
set -e -u -x
pip install -r requirements.txt
pip install --no-deps .
pip check
