#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A vanilla crawler which collect short text from search engine. by John Wang(john.wang.wjq@gmail) in Mar/2014
# Require Python >= 2.7

import os, argparse, bs4, requests, urllib, codecs, traceback

SITES_SHORTCUTS = (
    ('g', ('parse_google', 'roll_google', "http://www.google.com.hk", "/search?q=" )),
    ('b', ('parse_bing',   'roll_bing',   "http://cn.bing.com",       "/search?q=" )),
    ('d', ('parse_baidu',  'roll_baidu',  "http://www.baidu.com",     "/#wd="      )),
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
        if os.path.exists(args.output_to):
            if not os.path.isdir(args.output_to):return _exit_w_info('"%s" is not a valid folder' % args.output_to)
        else :
            os.mkdir(args.output_to)
        return args
    crw_opts = parser.add_argument_group('Crawling Options')
    crw_opts.add_argument('-s','--site', default=SITES_SHORTCUTS[0][0],
        help=('The URL into which we dive. Shortcut can be used: %s' % ', '.join(['"%s" for "%s"' % (sc, cfg[2]) for sc, cfg in SITES_SHORTCUTS])))
    crw_opts.add_argument('-t','--timeout', type=int, default=99, help="How many milliseconds before we give up waiting for response on a request.")
    crw_opts.add_argument('-d','--depth',type=int, default=1, help="How deep to follow the links.")
    crw_opts.add_argument('-r','--roll_times',type=int, default=2, help="How many result pages to roll on search engine site(e.g., Google).")
    crw_opts.add_argument('keywords', metavar='W', nargs='*', help='The keywords for a search engine site')
    output_opts = parser.add_argument_group('Output Options')
    output_opts.add_argument('-o','--output_to', help="The folder to where the output files go. If absent, the output will be print on screen.")
    output_opts.add_argument('-v','--verbose', action='store_true', help="Print out log info while running.")
    output_opts.add_argument('-c','--encoding', default='UTF-8', help="The encoding used for output and url params.")
    cnt_opts = parser.add_argument_group('Content Options')
    cnt_opts.add_argument('-e','--picked_element', choices=zip(*PICKED_DOM_ELE)[0], default=PICKED_DOM_ELE[0][0], 
        help="Which part of the html document to pick out. %s" % ', '.join(['"%s" for "%s"' % (dm, dmf[0]) for dm,dmf in PICKED_DOM_ELE]))
    args = parser.parse_args()
    return _validate(args)

def output(doc):
    _data = unicode(doc)
    FILE_IDX = FILE_IDX+1
    BYTES = BYTES + len(doc)
    if not ARGS.output_to:
        print _data
    else:
        with codecs.open(os.path.join(ARGS.output_to, FILE_IDX), 'w', ARGS.encoding) as f:
            f.write(_data)

def mk_soup(link):
    trk, _, tail = link.rpartition('.')
    if trk and tail.lower() in MEDIA_SUFFIX: # Ignore media type
        if ARGS.verbose:print 'Ignore media link', link
        return None
    try:
        if ARGS.verbose: print 'loading page:', link
        ctype = requests.head(link, timeout=ARGS.timeout).headers['content-type'].split(';')[0]
        if ctype.lower() in ('text/html', 'text/plain'): # Some links look not like rich media, but still we need to check.
            return bs4.BeautifulSoup(requests.get(link, timeout=ARGS.timeout).text)
        else:
            if ARGS.verbose: print ctype, 'content ignored.'
            if trk: MEDIA_SUFFIX.append(tail.lower()) # Add more trailling suffix to filter out the media links for following process.
    except requests.exceptions.Timeout:
        if ARGS.verbose: print 'Request timeout on', link
    except:
        if ARGS.verbose: traceback.print_exc()

def parse_google(soup):
    def psect(sect):
        try:
            anchor = sect.h3.a
            return anchor['href'], anchor.text
        except:
            return None
    result = soup.find('ol', id='rso')
    sections = result and result.find_all('li')
    sects = [psect(sect) for sect in sections]
    return [sect for sect in sects if sect]

def roll_google(soup, curr_pidx):
    try:
        return soup.find('table', id='nav').find('td', 'cur').next_sibling.a['href']
    except Exception as exp:
        if ARGS.verbose: print 'Roll to last page or there is an error: ', exp

def mk_flink(link, plink):
    return link and (link if link.lower().startswith('http://') else (''.join(plink.split('/')[:3]) + link))

def follow(link, level=0):
    soup = link and mk_soup(link)
    if not soup: return
    output(dict(PICKED_DOM_ELE)[ARGS.picked_element][1](soup))
    if level >= ARGS.depth: return
    [follow(mk_flink(anch['href'], link), level+1) for anch in soup.find_all('a')]

def roll_search(url, parser, roller, curr_pidx=1):
    soup = (url and mk_soup(url))
    if not soup:return
    for link, doc in parser(soup):
        output(doc)
        if link and ARGS.depth > 0:
            follow(mk_flink(link, url))
    if curr_pidx < ARGS.roll_times:
        next_link = roller(soup, curr_pidx)
        return next_link and roll_search(mk_flink(next_link, url), parser, roller, curr_pidx+1)

def _main():
    if ARGS.site < 3: # Start from searching
        parser, roller, site_url, spath = dict(SITES_SHORTCUTS)[ARGS.site]
        url = ''.join(site_url, spath, urllib.quote(' '.join(ARGS.keywords)))
        return roll_search(url, parser, roller)
    else: # General crawler here.
        pass

if __name__ == '__main__':
    global ARGS, BYTES, FILE_IDX
    ARGS, BYTES, FILE_IDX = get_args(), 0, 0
    _main()
    print 'Done. Total %s files, %s bytes' % (FILE_IDX, '{:,}'.format(BYTES))

    