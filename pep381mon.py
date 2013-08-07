#!/usr/bin/env python
# -*- coding: utf-8 -*- 
# A tool to monitor pep381client(https://bitbucket.org/loewis/pep381client) process to initiate a local mirror of the official Pypi server.

import sys,os, time, subprocess as sbp

def sync(i):
    if i>1: time.sleep(60)
    print 'Try', i, 'times ...'
    lgidx = max(filter(lambda x:x.startswith('pep381run'),os.listdir('.'))).split('.')[0][9:]
    try:lgidx = int(lgidx)
    except:lgidx = 0
    return i, sbp.call(['pep381run.py', 
                    'pypi',
                    '>pep381run%s.log'% (lgidx+i),
                    ], shell=True)

if __name__ == '__main__':
    tries = int(sys.argv[1]) if len(sys.argv)>1 else 3
    rt = [sync(i) for i in range(1, tries)]
    print 'Try %d times, %d failed.' % (len(rt), sum(zip(*rt)[1]))