for DATA_SIZE in 2 5 10 20 50 80 100
do
	WORKERS=2
	echo "WORKERS=$WORKERS
	DATA_SIZE=$DATA_SIZE
	QS=/home/tenzin/quickstep/build/quickstep_cli_shell
	QS_ARGS_NUMA_LOAD=''
	QS_ARGS_NUMA_RUN=''
	CREATE_SQL='create.sql'
	LOAD_DATA=true
	QUERIES='ALL'
	QS_STORAGE=/ssd1/tenzin/ssb_qsstor_${DATA_SIZE}/
	QS_ARGS_BASE='-printing_enabled=false -num_workers=30'
	QS_ARGS_BASE_RUN='-printing_enabled=false -tenzin_profiling=true -num_workers=$WORKERS'
	DATA_PATH=/ssd1/tenzin/ssb_${DATA_SIZE}" > q.cfg

	./run-benchmark.sh q.cfg
	
	for WORKERS in 5 10 30
	do

    echo "WORKERS=$WORKERS
    DATA_SIZE=$DATA_SIZE
    QS=/home/tenzin/quickstep/build/quickstep_cli_shell
    QS_ARGS_NUMA_LOAD=''
    QS_ARGS_NUMA_RUN=''
    CREATE_SQL='create.sql'
    LOAD_DATA=false
    QUERIES='ALL'
    QS_STORAGE=/ssd1/tenzin/ssb_qsstor_${DATA_SIZE}/
    QS_ARGS_BASE='-printing_enabled=false -num_workers=30'
    QS_ARGS_BASE_RUN='-printing_enabled=false -tenzin_profiling=true -num_workers=$WORKERS'
    DATA_PATH=/ssd1/tenzin/ssb_${DATA_SIZE}" > q.cfg

	  ./run-benchmark.sh q.cfg
	done
done
