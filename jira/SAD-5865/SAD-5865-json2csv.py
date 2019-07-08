#!/usr/local/bin/python
import json

fname = 'SAD-5865-hourly.json'

keys = [u'sivt', u'givtc_desktop', u'mobileApp', u'givts_mobileWeb', u'givtt_desktop', u'givtb_mobileApp', u'givt', u'givtb_mobileWeb', u'givtt_mobileApp', u'sivt_mobileApp', u'givtc_mobileApp', u'desktop', u'givta_mobileApp', u'givtb_desktop', u'TOTAL', u'givts_mobileApp', u'mobileWeb', u'givtt_mobileWeb', u'givtc_mobileWeb', u'sivt_desktop', u'sivt_mobileWeb', u'givts_desktop', u'givta_mobileWeb', u'givta_desktop']

print "DATE-TIME",
for k in keys:
    print k,
print

with open(fname, 'r') as f:
    for line in f:
        datastore = json.loads(line)
        print datastore["_1"],
        for k in keys:
            print datastore["_2"][k],
        print
