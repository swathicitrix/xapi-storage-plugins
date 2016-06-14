#!/bin/bash
#set -eux # for debug only
set -eu

PLUGINROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$PLUGINROOT/.env"
TESTROOT="$PLUGINROOT/test"

function finish {
    (
        cd "$PLUGINROOT"/libs
        if [ -h xapi ]; then rm xapi; fi
        if [ -h storage ]; then rm storage; fi
        if [ -h libs ]; then rm libs; fi
    )
}

trap finish EXIT

set +u
. "$ENVDIR/bin/activate"
set -u

(
    cd "$PLUGINROOT"

    LIBDIRS=`find $PLUGINROOT/libs -maxdepth 1 -type d | tr '\n' ' '`
    LIBTESTS=`find $PLUGINROOT/test/libs -name \*Test.py`

    # Run pylint over the code under test first
    #pylint $SOURCE

    # clear the coverage
    coverage erase

    # Create some namespace symlink
    (
        cd "$PLUGINROOT"/libs
        if [ -h xapi ]; then rm xapi; fi
        ln -s . xapi
        if [ -h storage ]; then rm storage; fi
        ln -s . storage
        if [ -h libs ]; then rm libs; fi
        ln -s . libs
    )

    # Test the libs
    PYTHONPATH="`echo "$LIBDIRS" | tr ' ' ':'`" \
    coverage run --branch $(which nosetests) \
        --with-xunit                \
        --xunit-file=nosetests-libs.xml  \
        $LIBTESTS

    # Test datapath plugins
    DATAPATHDIRS=`find $PLUGINROOT/datapath -maxdepth 1 -type d | tr '\n' ' '`
    DATAPATHTESTROOT="$TESTROOT/datapath"
    for datapath in $DATAPATHDIRS; do
        LEAF=$(basename $datapath)
        # if test folder exists
        if [ -d "$DATAPATHTESTROOT/$LEAF" ]; then
            # run nosetest for this datapath folder (use coverage -a)
            echo "Found test folder for $LEAF"
        fi
    done

    # Test volume plugins
    VOLUMEDIRS=`find $PLUGINROOT/volume -maxdepth 1 -type d  | tr '\n' ' '`
    VOLUMETESTROOT="$TESTROOT/volume"
    for volume in $VOLUMEDIRS; do
        LEAF=$(basename $volume)
        # if test folder exists
        if [ -d "$DATAPATHTESTROOT/$LEAF" ]; then
            # run nosetest for this volume folder
            echo "Found test folder for $LEAF"
        fi
    done

    SOURCEDIRS="$DATAPATHDIRS $VOLUMEDIRS $LIBDIRS $TESTROOT"

    SOURCE=`find $SOURCEDIRS -name \*.py | tr '\n' ','`

    coverage report --include="$SOURCE"
    coverage xml --include="$SOURCE"
    coverage html --include="$SOURCE"
)

