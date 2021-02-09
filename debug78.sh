#! /bin/env bash

make -j40

./voila -s 1 -q q9 --default_blend "computation_type=avx512,concurrent_fsms=1,prefetch=0" --blend_key_check "computation_type=scalar,prefetch=0,concurrent_fsms=1"  -r 1 --num_threads 1 > blend.txt
./voila -s 1 -q q9 --default_blend "computation_type=avx512,concurrent_fsms=1,prefetch=0"  -r 1 --num_threads 1 > vector.txt

cat blend.txt | grep Pipeline_6 > blend6.txt
cat vector.txt | grep Pipeline_6 > vector6.txt

sort blend6.txt > blend6_sorted.txt
sort vector6.txt > vector6_sorted.txt
