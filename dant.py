#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A vanilla crawler which collect short text from search engine. by John Wang(john.wang.wjq@gmail) in Mar/2014
# Require Python >= 2.7

import sys, os, re, signal, argparse, bs4, requests, urllib, codecs, traceback

SITES_SHORTCUTS = (
    ('g', ('g_parser', 'g_filter', 'g_roller', "http://www.google.com.hk", "/search?q=" )), # Google
    ('b', ('b_parser', 'b_filter', 'b_roller', "http://cn.bing.com",       "/search?q=" )), # Bing
    ('d', ('d_parser', 'd_filter', 'd_roller', "http://www.baidu.com",     "/s?wd="      )), # Baidu
    )

MEDIA_SUFFIX = ['bat','dll','so', 'exe','class','o','lo','la','a','bin','sh','dat',
                'js', 'css', 'jpeg', 'jpg', 'png', 'bmp', 'gif',
                'mp3', 'mp4', 'mkv', 'avi','3gp','mpeg', 
                'zip', 'tgz', 'gz', 'rar', 'bz2','jar']
                
def pick_text(x):
    if not x.body:return None
    for sc in x.body.find_all('script'):sc.decompose()
    eles = [el.string.strip() for el in x.body.descendants if isinstance(el, bs4.element.NavigableString)]
    return '\n'.join([el for el in eles if el])

PICKED_DOM_ELE = (('h', ('html', lambda x:x)), ('b', ('body', lambda x:x.body)), ('t', ('all text', pick_text) ))

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
        if args.output_dir:
            if os.path.exists(args.output_dir):
                if not os.path.isdir(args.output_dir):return _exit_w_info('"%s" is not a valid folder' % args.output_dir)
            else :
                os.mkdir(args.output_dir)
        return args
    crw_opts = parser.add_argument_group('Crawling Options')
    crw_opts.add_argument('-s','--site', default=SITES_SHORTCUTS[0][0],
        help=('The URL into which we dive. Shortcut can be used: %s' % ', '.join(['"%s" for "%s"' % (sc, cfg[3]) for sc, cfg in SITES_SHORTCUTS])))
    crw_opts.add_argument('-t','--timeout', type=int, default=4444, help="How many milliseconds before we give up waiting for response on a request.")
    crw_opts.add_argument('-d','--depth',type=int, default=1, help="How deep to follow the links.")
    crw_opts.add_argument('-p','--paginate_times',type=int, default=2, help="How many result pages to load on search engine site(e.g., Google).")
    crw_opts.add_argument('keywords', metavar='W', nargs='*', help='The keywords for a search engine site')
    output_opts = parser.add_argument_group('Output Options')
    output_opts.add_argument('-o','--output_dir', help="The output folder. If absent, the output will go to screen.")
    output_opts.add_argument('-l','--dir_limit', type=int, default=3000, help="If the number of files in the output folder exceed this value, new output folder will be created with incremental number suffix in the name. ")
    output_opts.add_argument('-v','--verbose', action='store_true', help="Print out log info while running.")
    output_opts.add_argument('-c','--encoding', default='UTF-8', help="The encoding used for output and url params.")
    cnt_opts = parser.add_argument_group('Content Options')
    cnt_opts.add_argument('-e','--picked_element', choices=zip(*PICKED_DOM_ELE)[0], default=PICKED_DOM_ELE[0][0], 
        help="Which part of the html document to pick out. %s" % ', '.join(['"%s" for "%s"' % (dm, dmf[0]) for dm,dmf in PICKED_DOM_ELE]))
    args = parser.parse_args()
    return _validate(args)

def output(doc):
    CTX.file_idx = CTX.file_idx+1
    CTX.all_bytes = CTX.all_bytes + len(doc)
    if not CTX.args.output_dir:
        print doc.encode('unicode_escape')
    else:
        dir_idx = int(CTX.file_idx // CTX.args.dir_limit)           
        outdir = os.path.abspath(CTX.args.output_dir)
        if dir_idx > 0:
            outdir = outdir + '_' + str(dir_idx)
            if not os.path.exists(outdir):
                os.mkdir(outdir)
        with open(os.path.join(outdir, CTX.args.site[0] + str(CTX.file_idx)), 'w') as f:
            f.write(doc.encode(CTX.args.encoding))

def suck_soup(link):
    if not link:return None,None
    trk, _, tail = link.rpartition('.')
    if trk and (tail and (tail.lower() in MEDIA_SUFFIX)): # Ignore media type
        if CTX.args.verbose:print 'Ignore media link', link
        return None,None
    try:
        if CTX.args.verbose: print 'loading page:', link
        response = requests.get(link, timeout=CTX.args.timeout)
        soup = None if (response.status_code != requests.codes.ok) else bs4.BeautifulSoup(response.content)
        return soup, response.url
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
    nextlinks = soup.find('div','sb_pag').select('li > a.sb_pagN')
    return nextlinks[0]['href'] if nextlinks else None

 # Baidu
def d_parser(soup):
    return soup.h3.a.get('href', None), soup.h3.next_sibling.text
def d_filter(soup):
    return soup.find_all('div', 'c-container')
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
    if not link or link.startswith('#'): return None
    return link if link.lower().startswith('http://') else ('http://' + plink.split('/')[2] + link)

def follow(link, level=0):
    soup, realink = suck_soup(link)
    if not soup:return
    doc = dict(PICKED_DOM_ELE)[CTX.args.picked_element][1](soup)
    if doc: output(doc)
    if level >= CTX.args.depth: return
    [follow(mk_flink(anch.get('href', None), realink), level+1) for anch in soup.find_all('a')]

def roll_search(url, sparser, sfilter, roller, curr_pidx=1):
    soup, realink = suck_soup(url)
    if not soup:return
    for link, doc in parse_se_page(soup, sparser, sfilter):
        if doc: output(doc)
        if link and CTX.args.depth > 0:
            follow(mk_flink(link, realink), 1)
    if curr_pidx < CTX.args.paginate_times:
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