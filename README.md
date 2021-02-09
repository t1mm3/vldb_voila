# VOILA

## Setup
VOILA can be installed using sandbox.sh.
For instance, run ```N=<CORES> PREFIX=<DIR> sandbox.sh``` to install VOILA and its depedencies in <DIR> while compilation will use up to <CORES> threads.

## Direct
Queries can run directly using:
```/voila -s 1 -q q9 --default_blend "computation_type=vector(128),concurrent_fsms=1,prefetch=0"```

This will run TPC-H Q9 on scale factor 1 using vectorized execution (with 128 tuple chunk size) with 5 repetitions and all available cores.

Alternatively, one can set a direct/old flavors:

For Hyper:
```/voila -s 1 -q q1 --flavor hyper```

For Vectorwise:
```/voila -s 1 -q q1 --flavor vectorwise```

## Exploration

One can explore base flavors, i.e. one flavor per query, with:
```explorer -s 1 --base -q q9```

One can explore per-pipeline flavors with:
```explorer -s 1 --pipeline -q q9```


## Experiments

* ```run_all.py``` runs all experiments.
* ```gen_tables.py``` generates plots/tables etc.
