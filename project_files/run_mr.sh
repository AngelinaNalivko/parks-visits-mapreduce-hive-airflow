#!/bin/bash
# Usage: ./run_mr.sh input_dir1 output_dir3

input_dir1=$1
output_dir3=$2

# Remove previous output if exists
hadoop fs -rm -r -f $output_dir3

mapred streaming\
    -input $input_dir1 \
    -output $output_dir3 \
    -mapper mapper.py \
    -reducer reducer.py \
    -combiner combiner.py \
    -file mapper.py \
    -file reducer.py \
    -file combiner.py