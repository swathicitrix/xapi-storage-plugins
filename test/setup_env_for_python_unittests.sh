#!/bin/bash
set -eux

PLUGINROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$PLUGINROOT/.env"

#if [ "${USE_PYTHON26:-yes}" == "yes" ]; then
#    virtualenv-2.6 --no-site-packages "$ENVDIR"
#else
    virtualenv "$ENVDIR"
#fi

set +u
. "$ENVDIR/bin/activate"
set -u

pip install nose
pip install coverage
pip install mock

# Remove this once we break the links to xapi
pip install xenapi

