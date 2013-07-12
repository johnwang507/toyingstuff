#!/usr/bin/env python

import sys, os, subprocess as sbp

arch_file = 'archfile'
cmd = '''D:/tools/maven-3.0.5/bin/mvn
        archetype:generate
        -q
        -DgroupId=delme
        -DartifactId=delme
        -DarchetypeGroupId=%s
        -DarchetypeArtifactId=%s
        -DinteractiveMode=false
        '''



# d:/tools/vn archetype:generate \
# -DgroupId=org.sonatype.mavenbook \
# -DartifactId=quickstart \
# -Dversion=1.0-SNAPSHOT \

# -DpackageName=org.sonatype.mavenbook \
# -DarchetypeGroupId=org.apache.maven.archetypes \
# -DarchetypeArtifactId=maven-archetype-quickstart \
# -DarchetypeVersion=1.0 \
# -DinteractiveMode=false




def load(line):
    cmd = (cmd % line.split(':')).split()
    print 'Running cmd:' cmd
    sbp.call()


if __name__ == '__main__':
    if len(sys.argv)>1:
        arch_file = sys.argv[1]
    if not os.path.exists(arch_file):
        print 'file not exists: "%s"' % arch_file
        sys.exit(1)
    with open(file,'r') as f:
        rts = [load(line) for line in f]
