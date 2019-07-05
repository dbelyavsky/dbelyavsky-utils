#!/usr/bin/python

# submit using this command
#   set -o pipefail && cat tmp.txt | nc graphstage01 2003

scores = { 
    "etl.ivt.tivt" : {
        "etl.ivt.sivt" : {
            "etl.ivt.sivt.ha" : 0,
            "etl.ivt.sivt.nht" : 0,
            "etl.ivt.sivt.ds" : 0
        },
        "etl.ivt.givt" : {
            "etl.ivt.givt.givta" : 0,
            "etl.ivt.givt.givtb" : 0,
            "etl.ivt.givt.givtc" : 0,
            "etl.ivt.givt.givta" : 0,
            "etl.ivt.givt.givta" : 0,
        },
        "etl.ivt.rvi" : {
            "etl.ivt.rvi.ib" : 0,
            "etl.ivt.rvi.ls" : 0
        }
    }
}

def foo( aValue ):
    total = 0

    if ( type(vAlue) is dict):
        total += 
    else
        print aValue

    return total

for v in scores:
    print (scores[v])
