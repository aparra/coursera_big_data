#!/usr/bin/env python
import sys

program       = "  "
abc_found     = False
running_total = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')

    if key != program:
        if abc_found:
             print('%s %d' % (program, running_total))
        
        abc_found = False
        running_total = 0

    program = key
    
    if value == 'ABC':
        abc_found = True
    else:
        running_total += int(value)

if abc_found:
    print('%s %d' % (program, running_total))
 
