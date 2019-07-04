#!/usr/local/opt/python/bin/python3
import re
import requests
import os
import argparse

JIRA = 'SAD-772'
team = '59'
baseURL = f'http://localhost:8080/rs/api/teams/{team}/cm'
logSql = 'logSql=true'
hotPeriod = 'period=last7days'
coldPeriod = 'period=%5B2018-12-15..2018-12-22%5D'

parser = argparse.ArgumentParser(
    description='Hit Endpoints to verify Vertica migration changes')
parser.add_argument('ENV',
                    help='which tests to run <HOT|COLD|LEGACY>',
                    default='LEGACY')
args = parser.parse_args()
ENV = args.ENV

outDir = f'data-{ENV}'
os.makedirs(outDir, exist_ok=True)
for (_, _, files) in os.walk(outDir):
    for file in files:
        if re.match(r".*\.json", file):
            os.remove(f'{outDir}/{file}')

if ENV == 'LEGACY':
    ENV = 'HOT'

endPoint = [
    'viewability3ms',
    'viewabilityrendered',
    'cumulativeviewability',
    'videoviewability'
]

queryString = {
    'HOT': [
        f'period={hotPeriod}&groups=%5Bdate:camp%5D',
        f'period={hotPeriod}&groups=%5Bweekly:pub%5D',
        f'period={hotPeriod}&groups=%5Bmonthly:plac%5D',
        f'period={hotPeriod}&cutoff=0&groups=%5Bpub%3Achan%3Aplac%3Amonthly%5D&includeBenchmark=true&_search=false&rows=5000&page=1&sortIndex=channelName+asc%2C+hitDate&sortOrder=asc&totalrows=5000'
    ],
    'COLD': [
        f'{coldPeriod}&groups=%5Bdate:camp%5D',
        f'{coldPeriod}&groups=%5Bweekly:pub%5D',
        f'{coldPeriod}&groups=%5Bmonthly:plac%5D',
        f'{coldPeriod}&cutoff=0&groups=%5Bpub%3Achan%3Aplac%3Amonthly%5D&includeBenchmark=true&_search=false&rows=5000&page=1&sortIndex=channelName+asc%2C+hitDate&sortOrder=asc&totalrows=5000'
    ],
}

for ep in endPoint:
    outFile = open(f'{outDir}/{JIRA}-{ep}-verify.json', 'w')
    for qs in queryString[ENV]:
        aURL = f'{baseURL}/{ep}?{qs}&{logSql}'
        print (f'Will query [{aURL}]')
        r = requests.get(aURL)
        outFile.write(f'{r.text}')
        print (f'Status [{r.status_code}]')
    outFile.close
