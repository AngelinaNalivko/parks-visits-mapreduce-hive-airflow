#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

reader = csv.reader(sys.stdin)
for row in reader:
    if len(row) < 7 or row[0] == "visit_id":
        continue
    park_id = row[1]
    entry_time = row[3]
    group_size = row[5]

    try:
        date = datetime.strptime(entry_time.strip(), "%Y-%m-%dT%H:%M").date()
    except Exception:
        continue

    try:
        group_size = int(group_size)
    except ValueError:
        continue

    # Key: (park_id, date)
    # Value: group_size, 1 visit
    print(f"{park_id},{date}\t{group_size}\t1")