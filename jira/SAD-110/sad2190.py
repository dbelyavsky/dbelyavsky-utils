#!/usr/bin/env python

import json

def parse_sca_results(sca_results, limit):
    if sca_results == '':
        return None
    else:
        try:
            fields = json.loads(sca_results)
            #print '{0}'.format(fields)
            # overall result
            results = str(fields['r']) if 'r' in fields else '3'
            # specific rules
            rules = fields['rules'] if 'rules' in fields else []
            for n in range(1, limit + 1): # this helps to populate rules, which were passed, and thus not in the sca_results
                # print '  * results:{0}'.format(results)
                rule = [x for x in rules if x['n'] == n] # rule is now a list of 1 or 0 elements
                # print '\trule {0}:{1}'.format(n, rule)
                if len(rule) == 0: # if a given rule is not in 'rules' it means it was passed
                    results += '0'
                elif len(rule) == 1: # if exactly one element, we extract it and parse it out
                    rule = rule[0] # extract this single element
                    if 'r' in rule:
                        r = rule['r']
                        # print '\t\tr:{0}'.format(r)
                        if r in ['0', '1', '2']:
                            results += str(r) # if it is valid, keep the value
                        else:
                            results += '3'
                    else: # if 'r' is not in the rule's data, something is wrong
                        results += '3'
                else: # else, something is wrong
                    results += '3'
            return results
        except Exception as e:
            print e
            return None

if __name__ == '__main__':
    with open ('query_result.csv') as fin:
        for line in fin:
            print '{0}\t{1}'.format(line.strip(), parse_sca_results(line.strip(), 17))
