#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A vanilla crawler which collect short text from search engine. by John Wang(john.wang.wjq@gmail) in Mar/2014
# Require Python >= 2.7

import sys, os, signal, argparse, bs4, requests, urllib, codecs, traceback

SITES_SHORTCUTS = (
    ('g', ('g_parser', 'g_filter', 'g_roller', "http://www.google.com.hk", "/search?q=" )), # Google
    ('b', ('b_parser', 'b_filter', 'b_roller', "http://cn.bing.com",       "/search?q=" )), # Bing
    ('d', ('d_parser', 'd_filter', 'd_roller', "http://www.baidu.com",     "/#wd="      )), # Baidu
    )

PICKED_DOM_ELE = (('h', ('html', lambda x:x)), ('b', ('body', lambda x:x.body)), ('t', ('all text', lambda x:x.text)))

MEDIA_SUFFIX = ['js', 'css', 'jpeg', 'jpg', 'png', 'bmp', 'gif', 'mp3', 'mp4', 'zip', 'tgz', 'gz', 'rar', ]

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
    CTX.FILE_IDX = CTX.FILE_IDX+1
    CTX.BYTES = CTX.BYTES + len(_data)
    if not CTX.ARGS.output_to:
        print _data
    else:
        with codecs.open(os.path.join(CTX.ARGS.output_to, CTX.FILE_IDX), 'w', CTX.ARGS.encoding) as f:
            f.write(_data)

def mk_soup(link): # todo: should return real url(redirected if any) and soup
    if not link:return None,None
    trk, _, tail = link.rpartition('.')
    if trk and (tail.lower() in MEDIA_SUFFIX): # Ignore media type
        if CTX.ARGS.verbose:print 'Ignore media link', link
        return None,None
    try:
        if CTX.ARGS.verbose: print 'loading page:', link
        ctype = requests.head(link, timeout=CTX.ARGS.timeout).headers['content-type'].split(';')[0]
        if ctype.lower() in ('text/html', 'text/plain'): # Some links look not like rich media, but still we need to check.
            response = requests.get(link, timeout=CTX.ARGS.timeout)
            soup = None if (response.status_code != requests.codes.ok) else bs4.BeautifulSoup(response.text)
            return soup, response.url
        else:
            if CTX.ARGS.verbose: print ctype, 'content ignored.'
            if trk: MEDIA_SUFFIX.append(tail.lower()) # Add more trailling suffix to filter out the media links for following process.
    except requests.exceptions.Timeout:
        if CTX.ARGS.verbose: print 'Request timeout on', link
    except:
        if CTX.ARGS.verbose: traceback.print_exc()
    return None,None

def g_parser(soup):
    return sect.h3.a.get('href', None), sect.h3.next_sibling.find('span','st').text
def g_filter(soup):
    return soup.find_all('li','g')
def g_roller(soup, curr_pidx):
    return soup.find('table', {"id": "nav"}).tr.contents[-1].a['href']

def b_parser(soup):
    raise Exception('Not implemented yet.')
def b_filter(soup):
    raise Exception('Not implemented yet.')
def b_roller(soup, curr_pidx):
    raise Exception('Not implemented yet.')

def d_parser(soup):
    return sect.h3.a.get('href', None), sect.h3.next_sibling.text
def d_filter(soup):
    return soup.find_all('table','result')
def d_roller(soup, curr_pidx):
    return soup.find('p', {"id": "page"}).find('a','n')['href']

def parse_se_page(soup, sect_parser, sect_filter):
    import pdb;pdb.set_trace()
    def psect(sect):
        try:
            return sect_parser(sect) 
        except:
            return None
    sections = sect_filter(soup)
    sects = sections and [psect(sc) for sc in sections]
    return sects and [sect for sect in sects if sect]

def roll_se_page(soup, roller, curr_pidx):
    try:
        return roller(soup, curr_pidx)
    except Exception as exp:
        if CTX.ARGS.verbose: print 'Roll to last page or there is an error: ', exp

def mk_flink(link, plink):
    if not link or link.startswith('#'):return None
    return link if link.lower().startswith('http://') else ('http://' + plink.split('/')[2] + link)

def follow(link, level=0):
    soup, realink = mk_soup(link)
    if not soup:return
    output(dict(PICKED_DOM_ELE)[CTX.ARGS.picked_element][1](soup))
    if level >= CTX.ARGS.depth: return
    [follow(mk_flink(anch.get('href', None), realink), level+1) for anch in soup.find_all('a')]

def roll_search(url, sparser, sfilter, roller, curr_pidx=1):
    soup, realink = mk_soup(url)
    if not soup:return
    for link, doc in parse_se_page(soup, sparser, sfilter):
        output(doc)
        if link and CTX.ARGS.depth > 0:
            follow(mk_flink(link, realink), 1)
    if curr_pidx < CTX.ARGS.roll_times:
        next_link = roll_se_page(soup, roller, curr_pidx)
        return next_link and roll_search(mk_flink(next_link, realink), sparser, roller, curr_pidx+1)

def _main():
    if len(CTX.ARGS.site) < 3: # Start from searching
        sparser, sfilter, roller, site_url, spath = dict(SITES_SHORTCUTS)[CTX.ARGS.site]
        url = ''.join([site_url, spath, urllib.quote(' '.join(CTX.ARGS.keywords))])
        return roll_search(url, eval(sparser),eval(sfilter), eval(roller))
    else: # General crawler here.
        pass

if __name__ == '__main__':
    global CTX
    class CTX:
        ARGS, BYTES, FILE_IDX = get_args(), 0, 0
    signal.signal(signal.SIGINT, lambda x,y:sys.exit(0))
    _main()
    print 'Done. Total %s files, %s bytes' % (CTX.FILE_IDX, '{:,}'.format(CTX.BYTES))

    