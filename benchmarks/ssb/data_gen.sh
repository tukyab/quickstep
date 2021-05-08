set -e

SCALE=2
LOC=`pwd`"/ssb_$SCALE"

if [ ! -d $LOC ]; then
  mkdir $LOC
  cp ssb-dbgen/dists.dss $LOC
fi

pushd .
cd ssb-dbgen
cmake -D EOL_HANDLING=ON . && cmake --build .
DBGEN=`pwd`/dbgen
cd $LOC

$DBGEN -v -s $SCALE
popd
