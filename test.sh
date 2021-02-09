#! /bin/bash

make

# ./voila > test.c
source test_env.sh

SF=1 ./generate_tpch.sh
g++ -g -O0 test1.cpp $GCC_ARGS -o test1
./test1 --data=tpch_1 --profile=profile.txt