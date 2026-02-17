#!/usr/bin/env python3
import sys

current_key = None
sum_groups = 0
count_visits = 0

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        continue
    key, sum_groups_part, count_part = parts

    if current_key and current_key != key:
        park_id, date = current_key.split(',', 1)
        avg_group_size = sum_groups / count_visits if count_visits else 0
        print(f"{park_id},{date},{count_visits},{avg_group_size:.2f}")
        sum_groups = 0
        count_visits = 0

    current_key = key
    sum_groups += int(sum_groups_part)
    count_visits += int(count_part)

if current_key:
    park_id, date = current_key.split(',', 1)
    avg_group_size = sum_groups / count_visits if count_visits else 0
    print(f"{park_id},{date},{count_visits},{avg_group_size:.2f}")