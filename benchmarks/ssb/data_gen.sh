set -e

SCALE=2
LOC=`pwd`"/ssb_$SCALE"

if [ ! -d $LOC ]; then
  mkdir $LOC
  cp dbgen/dists.dss $LOC
fi

pushd .
cd dbgen
make
DBGEN=`pwd`/dbgen
cd $LOC
$DBGEN -s $SCALE -T a
popd
