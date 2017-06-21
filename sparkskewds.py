#!/usr/bin/env python
#
# While learning spark, I need a sample dataset that I can twiddle with
# to explore all kinds of feature of Spark. Then here it is.
#
# This script generate a dataset with 3 columns,i.e., id, category, fact.
# The feature is you can set distribute scheme on the category column
# to create highly skewed partitions if you aggregate on the category column.
#                -- John Q Wang

import argparse
import random
import gzip

# The *odx* is a seq spliting the *rgmx* into segments of the number of its length,
# with each segment hosting that many random number of the seq element.
# i.e., genRand([3,1,2], 0) will get [0, 1, 2, 5, 7, 8],
# while [0, 1, 2] is the 1st segment, [5] is the 2nd one, etc ..
def genRand(skew_sch, caterng):
    step = caterng / len(skew_sch)
    num = dict([i,k] for i,k in enumerate(skew_sch))
    vals = sum(
        [[random.choice(range(i, i+step))
                for _ in range(num[i/step])]
            for i in range(0, caterng, step)],
        [])
    return random.sample(vals, len(vals))

def genr(skewschm, caterng, fact_cands):
    row_num = sum(skewschm)
    col_keys = range(row_num)
    col_cates = genRand(skewschm, caterng)
    col_facts = (random.choice(range(fact_cands)) for _ in range(row_num))
    return zip(col_keys, col_cates, col_facts)

def splitr(rows, partnums):
    start = 0
    for num in partnums[:-1]:
        end = start+num
        yield rows[start:end]
        start = end
    yield rows[start:]

# def outf(rows, outpath, fweights, gz_out):
def rowparts(rows, fweights):
    tweight,totalnum = sum(fweights), len(rows)
    partnums = [int(1.0*totalnum * w/tweight+i%2) for i,w in enumerate(fweights)]
    return splitr(rows, partnums)

def get_args():
    parser = argparse.ArgumentParser(description='''
generate a dataset with 3 columns, i.e., "id", "category", "fact". You can set skewing scheme on the category column to create highly skewed partitions if you aggregate on the category column.''')
    parser.add_argument('-o', '--outpath', dest='outpath',
        default='crazyskew',
        help="The path(prefix) of output file(s). default: %(default)s.")
    parser.add_argument('-d', '--skew_schema', dest='skew_schema',
        default='2222,111,888,1111,333,6666',
        help="Comma seperated int standing for the category distribution schema. The array size must be dividable by the argument *cate_range*. default: %(default)s .")
    parser.add_argument('-z', '--gz_out', dest='gz_out',
        action='store_true',
        help="Compress output with gzip.")
    parser.add_argument('-r', '--cate_range', dest='cate_range',
        type=int, default=60,
        help="The value range of categories. default: %(default)s .")
    parser.add_argument('-c', '--fact_range', dest='fact_range',
        type=int, default=10,
        help="The fact value range. default: %(default)s .")
    parser.add_argument('-w', '--file_weights', dest='file_weights',
        default="5,1,2,1,1",
        help="Comma seperated ints as weight of size for each out files. default: %(default)s .")
    args = parser.parse_args()
    args.skew_schema = [int(i.strip()) for i in args.skew_schema.split(',')]
    args.file_weights = [int(i.strip()) for i in args.file_weights.split(',')]
    return args

def genfobj(path, idx, gz):
    if gz:
        return gzip.open('%s_%s.%s'%(path, idx, 'gz'), 'wb')
    else:
        return open('%s_%s'%(path, idx), 'w')

if __name__ == '__main__':
    args = get_args()
    rows = genr(args.skew_schema, args.cate_range, args.fact_range)
    for idx, filerows in enumerate(rowparts(rows, args.file_weights)):
        fobj = genfobj(args.outpath, idx, args.gz_out)
        content = '\n'.join(','.join(str(c) for c in r) for r in filerows)
        fobj.write(content)
        fobj.close()

