#!/usr/bin/env python
# -*- coding: utf-8 -*- 
#
# A simple tool to find out what directories have been modified.
# John Wang(john.wang507@gmail.com)

import argparse, os, time, re, shutil

def get_args():
    parser = argparse.ArgumentParser(description='''A simple tool to find out what directories have been modified by Maven(mvn) when new dependencies been added.''',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-r','--root_dir', default='.', help='''The dir(usually the mvn repo) under which to find the modified directories.''')
    parser.add_argument('-t','--from_time',type=int, default=3600, help="How long time will be look backward to check if modified")
    parser.add_argument('-d','--destination', help="Where to store the found directories. If not present, the result will only be printed out.")
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    if not os.path.exists(args.root_dir):
        print 'file not exists: "%s"' % args.ar_file ; sys.exit(1)
    nt = time.time()
    modirs = [root for root, dirs, files in os.walk(args.root_dir)
        if not dirs and any(f for f in files if re.match(r'^.+\.pom$',f))
            and (nt-os.path.getmtime(root))<args.from_time]
    for d in modirs:
        if args.destination:
            destp = os.path.join(args.destination, d[len(args.root_dir)+1:])
            if not os.path.exists(destp):
                os.makedirs(destp)
            for f in os.listdir(d):
                shutil.copy(os.path.join(d, f), destp)
        print d
    print '%d dirs modified in last %d seconds' %(len(modirs), args.from_time)