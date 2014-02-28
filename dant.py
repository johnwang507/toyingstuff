#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A vanilla crawler which collect short text from search engine. by John Wang(john.wang.wjq@gmail) in Mar/2014

import os, argparse, bs4, requests, urllib, codecs

SUPPORTED_SITES = (
    ('google', ("www.google.com/search?", 'q',  'parse_google', 'roll_google')),
    ('baidu',  ("www.baidu.com/#",        'wd', 'parse_baidu',  'roll_baidu')),
    ('bing',   ("cn.bing.com/search?",    'q',  'parse_bing',   'roll_bing')),  )

def get_args():
    parser = argparse.ArgumentParser(description='A vanilla crawler which collect short text from search engine',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    def _exit_w_info(info):
        print '\n%s\n' % info
        parser.print_help()
        sys.exit(0)
    def _validate(args):
        return args #todo check if args is validate
    parser.add_argument('-s','--site', choices=dict(SUPPORTED_SITES).keys(), default='google', help="The site to search")
    parser.add_argument('-m','--max_pages',type=int, default=2, help="The max result pages to load for one search.")
    parser.add_argument('-o','--output_to', help="To where the results goes.")
    parser.add_argument('keywords', metavar='W', nargs='+',
                   help='The keywords to search')
    args = parser.parse_args()
    return _validate(args)

def output_file(doc):
    file_name = gen_fname()
    with codecs.open(os.path.join(args.output_to, file_name), 'w', args.encoding) as f:
        f.write(doc)
    return file_name

def parse_google(soup):
    return [] #todo parse search result pages
def roll_google(soup):
    return [] #todo parse search result pages

if __name__ == '__main__':
    global args
    args= get_args()
    site_url, search_key, parser =  dict(SUPPORTED_SITES)[args.site]
    url = 'http://%s%s=%s' % (site_url, search_key, urllib.quote(' '.join(args.keywords))) #todo roll load pages
    soup = bs4.BeautifulSoup(requests.get(url).text)
    out_fn = args.output_to and output_file or (lambda x:print x)
    [out_fn(doc) for doc in eval(parser)(soup)]

    