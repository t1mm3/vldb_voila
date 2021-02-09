#!/bin/bash
SF=${SF:-1}
DIRECTORY=${DIRECTORY:-tpch_${SF}}
if [ -d "$DIRECTORY" ]; then
	echo "Already exists"
	exit 0
fi

mkdir -p "$DIRECTORY"

[ -d tpch-dbgen ] || git clone https://github.com/eyalroz/tpch-dbgen || exit -1
cd tpch-dbgen

cmake .  \
	&& cmake --build . \
	&& echo "Generating TPC-H scale factor $scale_factor" \
	&& ./dbgen -f -s $SF \
	&& mv *.tbl $DIRECTORY \
	&& exit 0
exit -1