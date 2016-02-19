#!/usr/bin/env python
import sys

for line in sys.stdin:
    key, value = map(lambda e: e.strip(), line.strip().split(','))
    if value == 'ABC' or value.isdigit():
        print('%s\t%s' % (key, value))    

