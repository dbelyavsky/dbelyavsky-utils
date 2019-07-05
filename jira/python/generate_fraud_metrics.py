#!/usr/bin/python

import random
import time

keys = {
	"etl.ivt.tivt" : {
		"etl.ivt.sivt" : {
			"etl.ivt.sivt.ha" : {},
			"etl.ivt.sivt.nht" : {},
			"etl.ivt.sivt.ds" : {}
		},
		"etl.ivt.givt" : {
			"etl.ivt.givt.givta" : {},
			"etl.ivt.givt.givtb" : {},
			"etl.ivt.givt.givtc" : {},
			"etl.ivt.givt.givts" : {},
			"etl.ivt.givt.givtt" : {}
		},
		"etl.ivt.rvi" : {
			"etl.ivt.rvi.ls" : {},
			"etl.ivt.rvi.ib" : {}
		}
	}
}


def foo(metricsKeys, timeStamp, count):
	totalCount = 0
	for k,v in metricsKeys.items():
		if v:
			count = foo(v, timeStamp, 0)
		else:
			count = random.randint(1,10)
		totalCount += count
		print ('%s %d %s' % (k,count,timeStamp))
	return totalCount


for d in range (1,31):
	for h in range (0,23):
		foo(keys, int(time.mktime(time.strptime("%d Apr 19 %02d EST" % (d,h), "%d %b %y %H %Z"))), 0)