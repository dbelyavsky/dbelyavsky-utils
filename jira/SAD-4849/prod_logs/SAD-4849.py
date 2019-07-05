import sys
import re

hit_date_pattern = re.compile(' updateLastHitDate_([0-9]+)')
count_pattern = re.compile(' bad records number ([0-9]+)')
found_hit_date = False
hit_date = ''
for line in sys.stdin:
    if (not found_hit_date):
        m = hit_date_pattern.search(line)
        if (m != None):
            found_hit_date = True
            hit_date = m.group(1)
    else:
        m2 = count_pattern.search(line)
        if (m2 != None):
            count = m2.group(1)
            print "%s,%s" % (hit_date, count)
            found_hit_date = False
            hit_date = ''
