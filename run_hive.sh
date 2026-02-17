#!/bin/bash
# Usage: ./run_hive.sh input_dir3 input_dir4 output_dir6

input_dir3=$1   
input_dir4=$2   
output_dir6=$3  

beeline -hiveconf input_dir3=$input_dir3 \
     -hiveconf input_dir4=$input_dir4 \
     -hiveconf output_dir6=$output_dir6 \
     -f hive.hql