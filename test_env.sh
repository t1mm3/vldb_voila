#! /bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:@PROJECT_BINARY_DIR@
export GCC_ARGS="-L./ -Ldb-engine-paradigms/ -lcommon -lvoila_runtime -ldl -I @CMAKE_CURRENT_SOURCE_DIR@/db-engine-paradigms/include"
