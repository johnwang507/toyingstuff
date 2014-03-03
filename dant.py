#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A vanilla crawler which collect short text from search engine. by John Wang(john.wang.wjq@gmail) in Mar/2014
# Require Python >= 2.7

import sys, os, signal, argparse, bs4, requests, urllib, codecs, traceback

SITES_SHORTCUTS = (
    ('g', ('g_parser', 'g_filter', 'g_roller', "http://www.google.com.hk", "/search?q=" )), # Google
    ('b', ('b_parser', 'b_filter', 'b_roller', "http://cn.bing.com",       "/search?q=" )), # Bing
    ('d', ('d_parser', 'd_filter', 'd_roller', "http://www.baidu.com",     "/s?wd="      )), # Baidu
    )

PICKED_DOM_ELE = (('h', ('html', lambda x:x)), ('b', ('body', lambda x:x.body)), ('t', ('all text', lambda x:x.text)))

MEDIA_SUFFIX = ['js', 'css', 'jpeg', 'jpg', 'png', 'bmp', 'gif', 'mp3', 'mp4', 'zip', 'tgz', 'gz', 'rar', ]

# Small utility functions
listlen = lambda x:len(x) if x else 0

def get_args():
    parser = argparse.ArgumentParser(description='A vanilla crawler which collect short text from search engine',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    def _exit_w_info(info):
        print '\n%s\n' % info
        parser.print_help()
        sys.exit(0)
    def _validate(args):
        if len(args.site)<3:
            if args.site not in zip(*SITES_SHORTCUTS)[0]:
                return _exit_w_info('site shortcut "%s" not found.' % args.site)
            if not args.keywords:
                return _exit_w_info('Searching keywords not specified.')
        args.timeout = args.timeout / 1000.0
        if args.output_to:
            if os.path.exists(args.output_to):
                if not os.path.isdir(args.output_to):return _exit_w_info('"%s" is not a valid folder' % args.output_to)
            else :
                os.mkdir(args.output_to)
        return args
    crw_opts = parser.add_argument_group('Crawling Options')
    crw_opts.add_argument('-s','--site', default=SITES_SHORTCUTS[0][0],
        help=('The URL into which we dive. Shortcut can be used: %s' % ', '.join(['"%s" for "%s"' % (sc, cfg[2]) for sc, cfg in SITES_SHORTCUTS])))
    crw_opts.add_argument('-t','--timeout', type=int, default=4444, help="How many milliseconds before we give up waiting for response on a request.")
    crw_opts.add_argument('-d','--depth',type=int, default=1, help="How deep to follow the links.")
    crw_opts.add_argument('-r','--roll_times',type=int, default=2, help="How many result pages to roll on search engine site(e.g., Google).")
    crw_opts.add_argument('keywords', metavar='W', nargs='*', help='The keywords for a search engine site')
    output_opts = parser.add_argument_group('Output Options')
    output_opts.add_argument('-o','--output_to', help="The folder where the output files go. If absent, the output will be print on screen.")
    output_opts.add_argument('-v','--verbose', action='store_true', help="Print out log info while running.")
    output_opts.add_argument('-c','--encoding', default='UTF-8', help="The encoding used for output and url params.")
    cnt_opts = parser.add_argument_group('Content Options')
    cnt_opts.add_argument('-e','--picked_element', choices=zip(*PICKED_DOM_ELE)[0], default=PICKED_DOM_ELE[0][0], 
        help="Which part of the html document to pick out. %s" % ', '.join(['"%s" for "%s"' % (dm, dmf[0]) for dm,dmf in PICKED_DOM_ELE]))
    args = parser.parse_args()
    return _validate(args)

def output(doc):
    _data = unicode(doc)
    CTX.file_idx = CTX.file_idx+1
    CTX.all_bytes = CTX.all_bytes + len(_data)
    if not CTX.args.output_to:
        print _data.encode('unicode_escape')
    else:
        with codecs.open(os.path.join(CTX.args.output_to, str(CTX.file_idx)), 'w', CTX.args.encoding) as f:
            f.write(_data)

def mk_soup(link): # todo: should return real url(redirected if any) and soup
    if not link:return None,None
    trk, _, tail = link.rpartition('.')
    if trk and (tail.lower() in MEDIA_SUFFIX): # Ignore media type
        if CTX.args.verbose:print 'Ignore media link', link
        return None,None
    try:
        if CTX.args.verbose: print 'loading page:', link
        heads = requests.head(link, timeout=CTX.args.timeout)
        ctype = heads.headers['content-type'].split(';')[0]
        if ctype.lower() in ('text/html', 'text/plain'): # Some links look not like rich media, but still we need to check.
            response = requests.get(link, timeout=CTX.args.timeout)
            soup = None if (response.status_code != requests.codes.ok) else bs4.BeautifulSoup(response.text)
            return soup, response.url
        else:
            if CTX.args.verbose: print ctype, 'content ignored.'
            if trk: MEDIA_SUFFIX.append(tail.lower()) # Add more trailling suffix to filter out the media links for following process.
    except requests.exceptions.Timeout:
        if CTX.args.verbose: print 'Request timeout on', link
    except:
        if CTX.args.verbose: traceback.print_exc()
    return None,None

# Google
def g_parser(soup):
    return soup.h3.a.get('href', None), soup.h3.next_sibling.find('span','st').text
def g_filter(soup):
    return soup.find_all('li','g')
def g_roller(soup, curr_pidx):
    return soup.find('table', {"id": "nav"}).tr.contents[-1].a['href']

 # Bing
def b_parser(soup):
    return soup.h3.a.get('href', None), soup.find('div', 'sa_mc').p.text
def b_filter(soup):
    return soup.find('div',{'id':'results'}).find_all('li', 'sa_wr')
def b_roller(soup, curr_pidx):
    return soup.find('div','sb_pag').select('li > a.sb_pagN')

 # Baidu
def d_parser(soup):
    return soup.h3.a.get('href', None), soup.h3.next_sibling.text
def d_filter(soup):
    return soup.find_all('table','result')
def d_roller(soup, curr_pidx):
    return soup.find('p', {"id": "page"}).find('a','n')['href']

def parse_se_page(soup, sect_parser, sect_filter):
    def psect(sect):
        try:
            return sect_parser(sect) 
        except:
            if CTX.args.verbose:print traceback.print_exc()
            return None
    sections = sect_filter(soup)
    if CTX.args.verbose: print listlen(sections), 'search results found.'
    sects = sections and [psect(sc) for sc in sections]
    rt = sects and [sect for sect in sects if sect]
    if CTX.args.verbose: print listlen(rt), 'search result parsed.'
    return rt

def roll_se_page(soup, roller, curr_pidx):
    try:
        return roller(soup, curr_pidx)
    except Exception as exp:
        if CTX.args.verbose: print 'Roll to last page or there is an error: ', exp

def mk_flink(link, plink):
    if not link or link.startswith('#'):return None
    return link if link.lower().startswith('http://') else ('http://' + plink.split('/')[2] + link)

def follow(link, level=0):
    soup, realink = mk_soup(link)
    if not soup:return
    output(dict(PICKED_DOM_ELE)[CTX.args.picked_element][1](soup))
    if level >= CTX.args.depth: return
    [follow(mk_flink(anch.get('href', None), realink), level+1) for anch in soup.find_all('a')]

def roll_search(url, sparser, sfilter, roller, curr_pidx=1):
    soup, realink = mk_soup(url)
    if not soup:return
    for link, doc in parse_se_page(soup, sparser, sfilter):
        output(doc)
        if link and CTX.args.depth > 0:
            follow(mk_flink(link, realink), 1)
    if curr_pidx < CTX.args.roll_times:
        next_link = roll_se_page(soup, roller, curr_pidx)
        return next_link and roll_search(mk_flink(next_link, realink), sparser, sfilter, roller, curr_pidx+1)

def _main():
    if len(CTX.args.site) < 3: # Start from searching
        sparser, sfilter, roller, site_url, spath = dict(SITES_SHORTCUTS)[CTX.args.site]
        url = ''.join([site_url, spath, urllib.quote(' '.join(CTX.args.keywords))])
        return roll_search(url, eval(sparser),eval(sfilter), eval(roller))
    else: # General crawler here.
        pass

if __name__ == '__main__':
    global CTX
    class CTX:
        args, all_bytes, file_idx = get_args(), 0, 0
    signal.signal(signal.SIGINT, lambda x,y:sys.exit(0))
    _main()
    print 'Done. Total %s files, %s bytes' % (CTX.file_idx, '{:,}'.format(CTX.all_bytes))

    