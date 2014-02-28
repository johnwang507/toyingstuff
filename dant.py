#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A vanilla crawler which collect short text from search engine. by John Wang(john.wang.wjq@gmail) in Mar/2014

import os, argparse, bs4, requests, urllib, codecs

SUPPORTED_SITES = (
    ('google', ("www.google.com.hk/search?", 'q',  'parse_google', 'roll_google')),
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
    parser.add_argument('-d','--depth',type=int, default=2, help="How deep to go following the links.")
    parser.add_argument('-o','--output_to', help="To where the results goes.")
    parser.add_argument('keywords', metavar='W', nargs='+',
                   help='The keywords to search')
    args = parser.parse_args()
    return _validate(args)

def output_file(doc):
    file_name = gen_fname()
    with codecs.open(os.path.join(args.output_to, file_name), 'w', argS.encoding) as f:
        f.write(doc)
    return file_name

def parse_google(soup):
    def psect(sect):
        anchor = sect.find('h3').find('a')
        return anchor['href'], anchor.text
    result = soup.find('ol', id='rso')
    sections = result and result.find_all('li')
    return [psect(sect) for sect in sections]

def roll_google(soup, curr_pidx):
    return soup.find('table', id='nav').find('td', 'cur').next_sibling.a['href']

def ensure(link):
    return link and (link.upper().startswith('HTTP') and link or site_rooT+link)

def follow(link, level=0): #todo impl
    bs4.BeautifulSoup(requests.get(ensure(link)).text)

def roll_search(soup, parserfn, rollfn, curr_pidx=1):
    output = argS.output_to and output_file or (lambda x:print x)
    for link, doc in parserfn(soup):
        output(doc)
        follow(link)
    if curr_pidx < argS.max_pages:
        next_link = ensure(rollfn(soup, curr_pidx))
        next_page = next_link and bs4.BeautifulSoup(requests.get(next_link).text)
        return next_page and roll_search(next_page, parserfn, rollfn, curr_pidx+1)

if __name__ == '__main__':
    global argS, site_rooT
    argS= get_args()
    site_url, search_key, parser, roller =  dict(SUPPORTED_SITES)[argS.site]
    site_rooT = "http://%s" % site_url.split('/')[0]
    url = 'http://%s%s=%s' % (site_url, search_key, urllib.quote(' '.join(argS.keywords))) #todo roll load pages
    start_soup = bs4.BeautifulSoup(requests.get(url).text)
    return roll_search(start_soup, parser, roller)

    