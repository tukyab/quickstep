#!/bin/bash

set -e

echo "Loading settings from $1"
if ! source $1 ; then echo "Failed to load config" ; exit 1 ; fi

# Print some debug information about this test.
grep -v ^# $1

QS_ARGS_STORAGE="-storage_path="$QS_STORAGE

function load_data {
  # Creates a fresh load of the tpc-h data.
  if [ -d $DATA_PATH ] ; then
    rm -rf $QS_STORAGE
    QSEXE="$QS $QS_ARGS_BASE $QS_ARGS_STORAGE $QS_ARGS_NUMA_LOAD"

    # Use quickstep to generate the catalog file in a new folder.
    $QSEXE -initialize_db < $CREATE_SQL

    COUNTER=0
    for tblfile in $DATA_PATH/*.tbl* ; do
      # Resolve which table the file should be loaded into.
      TBL=""
      if [[ $tblfile == *"supplier"* ]]
      then
        TBL="supplier"
      elif [[ $tblfile == *"customer"* ]]
      then
        TBL="customer"
      elif [[ $tblfile == *"part"* ]]
      then
        TBL="part"
      elif [[ $tblfile == *"lineorder"* ]]
      then
        TBL="lineorder"
      elif [[ $tblfile == *"datetable"* ]]
      then
        TBL="datetable"
      fi

      echo Loading $TBL from file: $tblfile;
      if ! echo "COPY $TBL FROM '$tblfile' WITH (DELIMITER ',');" | $QSEXE;
      then
        echo "Quickstep load failed.";
        exit 1;
      fi

      let COUNTER=COUNTER+1
    done
    echo Done loading. Loaded $COUNTER files.
    $QSEXE <<< "\analyze"
    # Print the disk footprint of the newly created database
    CUT=" | cut -f 1"
    DBSIZE="du -m $QS_STORAGE"$CUT
    echo -n "Datatbase footprint in MB is: "
    eval $DBSIZE

  else
    echo "data folder $DATA_PATH not found, quitting"
    exit
  fi
}

function run_queries {
  # Runs each TPC-H query several times.
  QSEXE="$QS $QS_ARGS_BASE_RUN $QS_ARGS_NUMA_RUN $QS_ARGS_STORAGE"
  TOTALRUNS=2
  queries=( 01 02 03 04 05 06 07 08 09 10 11 12 13 )
  if [ "$QUERIES" != "ALL" ]; then
    unset queries
    read -r -a queries <<< "$QUERIES"
    echo "Running a subset of queries with length ${#queries[@]}"
  fi
  echo $QSEXE
  for query in ${queries[@]} ; do
    echo "Query $query.sql"
    touch tmp.sql
    # Run each query a variable number of times.
    for i in `seq 1 $TOTALRUNS`;
    do
      cat queries/$query.sql >> tmp.sql
    done
    timeout 30m $QSEXE < tmp.sql
    rc=$?
    if [ $rc = 124 ] ;
    then
      echo "Quickstep timed out on query $query, continuing to next query."
    elif [ $rc != 0  ] ;
    then
      echo "Quickstep failed on query $query, continuing to next query."
    fi
    rm -f tmp.sql
    mv /home/tenzin/quickstep/benchmarks/ssb/record.json /ssd1/tenzin/ssb/json_${DATA_SIZE}g/q${query}_c${WORKERS}.json
    rm -f /home/tenzin/quickstep/benchmarks/ssb/record.json
  done
}

function analyze_tables {
  # Runs the analyze command on quickstep.
  QSEXE="$QS $QS_ARGS_BASE $QS_ARGS_NUMA_RUN $QS_ARGS_STORAGE"
  touch tmp.sql
  echo "\analyze" >> tmp.sql
  if ! $QSEXE < tmp.sql ;
  then
    echo "Quickstep failed on analyze, exiting."
    exit 1
  fi
  rm -f tmp.sql
}

if [ ! -x $QS ] ; then
  echo "Given Quickstep executable not found: $QS"
  echo "Specify it in quickstep.cfg."
  exit
fi

# Load data.
if [ $LOAD_DATA = "true" ] ; then
  load_data
  analyze_tables
fi

run_queries
