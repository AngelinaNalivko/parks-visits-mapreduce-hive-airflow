#!/usr/bin/env python3
import sys

current_key = None
sum_groups = 0
count_visits = 0

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        continue
    key, group_size, count = parts

    if current_key and current_key != key:
        print(f"{current_key}\t{sum_groups}\t{count_visits}")
        sum_groups = 0
        count_visits = 0

    current_key = key
    sum_groups += int(group_size)
    count_visits += int(count)

if current_key:
    print(f"{current_key}\t{sum_groups}\t{count_visits}")