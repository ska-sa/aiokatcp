#!/bin/bash
set -e -u

py38="$(python -c 'import sys; print(sys.version_info >= (3, 8))')"
if [ "$py38" = "True" ]; then
    # Python 3.8 deprecates some features still in use to support Python 3.5
    # or are used by dependencies, so we can't yet fail on
    # DeprecationWarnings.
    warn_args=""
else
    warn_args="-Werror"
fi

set -x
pytest $warn_args --cov=aiokatcp
flake8
mypy
