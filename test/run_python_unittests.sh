#!/bin/bash
#set -eux # for debug only
set -eu

PLUGINROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$PLUGINROOT/.env"
TESTROOT="$PLUGINROOT/test"

echo "Running unit tests from $PLUGINROOT"

function finish {
    (
        cd "$PLUGINROOT"/libs
        if [ -h xapi ]; then rm xapi; fi
        if [ -h storage ]; then rm storage; fi
        if [ -h libs ]; then rm libs; fi
    )
}

trap finish EXIT

if [ -z "${CHROOT-default}" ]; then
    if [ ! -d $ENVDIR ]; then
        $(dirname $0)/setup_env_for_python_unittests.sh
    fi

    set +u
    . "$ENVDIR/bin/activate"
    set -u
fi

(
    cd "$PLUGINROOT"

    LIBDIRS="$PLUGINROOT/libs"
    DATAPATHDIRS=`find $PLUGINROOT/datapath -maxdepth 1 -type d | tr '\n' ' '`
    VOLUMEDIRS=`find $PLUGINROOT/volume -maxdepth 1 -type d  | tr '\n' ' '`

    SOURCEDIRS="$DATAPATHDIRS $VOLUMEDIRS $LIBDIRS $TESTROOT"

    SOURCE=`find $SOURCEDIRS -name \*.py | tr '\n' ','`

    # Run pylint over the code under test first
    pylint -E $SOURCE

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
    LIBTESTS=`find $PLUGINROOT/test/libs -name test_\*.py`
    PYTHONPATH="`echo "$LIBDIRS" | tr ' ' ':'`" \
    coverage run --branch $(which nosetests) \
        --with-xunit                \
        --xunit-file=nosetests-libs.xml  \
        $LIBTESTS

    # Test datapath plugins
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
    VOLUMETESTROOT="$TESTROOT/volume"
    for volume in $VOLUMEDIRS; do
        LEAF=$(basename $volume)
        # if test folder exists
        if [ -d "$DATAPATHTESTROOT/$LEAF" ]; then
            # run nosetest for this volume folder
            echo "Found test folder for $LEAF"
        fi
    done

    coverage report --include="$SOURCE"
    coverage xml --include="$SOURCE"
    coverage html --include="$SOURCE"
)

