#!/bin/bash
set -e -u

py38="$(python -c 'import sys; print(sys.version_info >= (3, 8))')"
if [ "$py38" = "True" ]; then
    # Python 3.8 deprecates some features still in use to support Python 3.5
    # or are used by dependencies, so we can't yet fail on
    # DeprecationWarnings.
    warn_args=""
else
    warn_args="
        -Werror
        -Wignore::DeprecationWarning:site
        -Wignore::PendingDeprecationWarning:nose.importer
        -Wignore::DeprecationWarning:nose.importer
        -Wignore::DeprecationWarning:nose.suite"
fi

set -x
python $warn_args `which nosetests` --with-coverage --cover-erase --cover-package aiokatcp
# The pinned version of flake8 doesn't work with Python 3.8
# (and it can't be upgraded until https://github.com/PyCQA/pyflakes/issues/445
# is fixed).
if [ "$py38" = "False" ]; then
    flake8 aiokatcp examples
fi
mypy aiokatcp examples

