for DATA_SIZE in 2 5 10 20 50 80 100
do
  echo $DATA_SIZE
  TPCH_DATA_PATH=/home/tenzin/quickstep/benchmarks/tpch/tpch_${DATA_SIZE}
  ./run-benchmark.sh quickstep.cfg
done
