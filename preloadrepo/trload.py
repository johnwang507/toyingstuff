#!/usr/bin/env python
# -*- coding: utf-8 -*- 
#
# A simple tool to trigger a Maven Repository Manager(MRM), e.g., Artifactory,to
# preload libraries.
#
# It implements the function by reading from an "Archetype Records (AR)" file and
# invoking the system command "mvn archetype:generate" to request the MRM to load
# libraries from the proxied remote repositories(e.g., http://repo1.maven.org/maven2/).
# 
# Every line in the AR file is a record of a archetype, which is compose of the 
# archetype group id and the artifact id seperated by a colon (:). Example:
#    "com.github.searls:jasmine-archetype"
# It can be generated by invoke "mvn archetype:generate" in interaction mode and 
# redirected the output to a file, and then modify it to meet the need.
#
# The intention of creating this tool is to prepare a MRM meant to run in an environment
# without internet access.
#
# John Wang(john.wang507@gmail.com)
# 15/July/2013

import argparse, sys, os, shutil, subprocess as sbp

_mvn='mvn'
_farch_group = 'delme'
_farch_id = _farch_group
_arch_file = 'archfile'
_cmd = '''%s
        archetype:generate
        -q
        -DgroupId=%s
        -DartifactId=%s
        -DarchetypeGroupId=%s
        -DarchetypeArtifactId=%s
        -DinteractiveMode=false
        '''

def get_args():
    parser = argparse.ArgumentParser(description='''A simple tool to trigger a Maven Repository Manager(MRM), e.g., Artifactory,to preload libraries. It is implemented by reading from an "Archetype Records (AR)" file and invoking the Maven command "mvn archetype:generate". by John Wang(john.wang.wjq@gmail.com)''',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f','--ar_file', default='archfile', help='''The "Archetype Records (AR)" file with records of archetypes. Every line in the AR file is a record of a archetype, which is compose of the 
 archetype group id and the artifact id seperated by a semicolon (:). E.g., "com.github.searls:jasmine-archetype"''')
    parser.add_argument('-b','--begin',type=int, default=0, help="The idx of the record in the AR file from which to start(inclusive).")
    parser.add_argument('-e','--end',type=int, default=sys.maxint, help="The idx of the record in the AR file with which to stop(exclusive).")
    parser.add_argument('idxes', metavar='idx', type=int, nargs='*', help='The indexes specified individually to load. The "-b" and "-e" option will be ignored by this presence.')
    return parser.parse_args()

def _clear():
    os.chdir('..')
    shutil.rmtree(_farch_id)

def load(idx, line):
    sys.stdout.flush()
    atype_g, atype_a = map(lambda x:x.strip(),line.split(':'))
    cmd = _cmd % (_mvn, _farch_group, _farch_id, atype_g, atype_a)
    print idx, ':', atype_g, ':', atype_a
    if sbp.call(cmd.split(), shell=True):
        print 'Failed init archetype'
        return 1
    os.chdir(_farch_id)
    if sbp.call([_mvn, '-q', 'dependency:resolve'], shell=True):
        print 'Failed resolving dependencies.'
        _clear()
        return 1
    if sbp.call([_mvn, '-q', 'dependency:sources'], shell=True):
        print 'Cannot retrieve sources.'
    _clear()
    return 0

if __name__ == '__main__':
    args = get_args()
    if not os.path.exists(args.ar_file):
        print 'file not exists: "%s"' % args.ar_file
        sys.exit(1)
    with open(args.ar_file, 'r') as f:
        cond = lambda x: (x in args.idxes) if args.idxes else lambda x:(x >= args.begin and x < args.end)
        rts = [(i,load(i, l)) for i,l in enumerate(f) if cond(i)]
        failed = reduce(lambda x,y:x+y[1], rts, 0)
        print 'Done. Total %d, %d failed.' % (len(rts), failed)
        if failed:
            print 'Failed:', zip(*filter(lambda x:x[1]>0, rts))[0]
