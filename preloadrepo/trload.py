#!/usr/bin/env python
# -*- coding: utf-8 -*- 
#
# A simple tool to trigger a Maven Repository Manager(MRM), e.g., Artifactory,to
# preload libraries. --John Wang(john.wang507@gmail.com)
#
# It implements the function by reading from a file listing all Maven archetypes and
# invoke the system command "mvn archetype:generate" to request the MRM to load
# libraries from the proxied remote repositories(e.g., http://repo1.maven.org/maven2/).
#
# The intention of doing so is to prepare a MRM meant to run in an environment
# without internet access.

import sys, os, shutil, subprocess as sbp

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

def _clear():
    os.chdir('..')
    shutil.rmtree(_farch_id)

def load(line):
    atype_g, atype_a = map(lambda x:x.strip(),line.split(':'))
    cmd = _cmd % (_mvn, _farch_group, _farch_id, atype_g, atype_a)
    print 'Init archetype:', atype_g, atype_a
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
    if len(sys.argv)>1:
        arch_file = sys.argv[1]
    if not os.path.exists(arch_file):
        print 'file not exists: "%s"' % arch_file
        sys.exit(1)
    with open(arch_file, 'r') as f:
        rts = [load(line) for line in f]
        print 'Done. Total %d, %d failed.' %(len(rts), sum(rts))