#!/usr/bin/env python
from __future__ import division

"""
A module to query arxiv and match to ADS for citation statistics.
"""

from arxivoai2 import arxivoai2

ipblockinfo = """
Thanks for the email.  The ip block is triggered automatically by "too
many in too short a time period" where, understandably, we don't want
to advertise that rate.  If you put 30 seconds between queries, you'll
probably avoid the block.  You can also spread out your searches over
multiple mirrors which will allow you to get results back quicker...

Last, if you do trigger the block, just let us know (ads@cfa.harvard.edu
is fine for correspondence), and we'll unblock you.
"""


_exampleurl = "http://adsabs.harvard.edu/cgi-bin/nph-abs_connect?db_key=ALL&warnings=YES&version=1&bibcode=arXiv%3A1302.1193%0D%0AarXiv%3A1302.1160&nr_to_return=100&start_nr=1"
_parsesto = """
{'bibcode': ['arXiv:1302.1193\r\narXiv:1302.1160'],
 'http://adsabs.harvard.edu/cgi-bin/nph-abs_connect?db_key': ['ALL'],
 'nr_to_return': ['100'],
 'start_nr': ['1'],
 'version': ['1'],
 'warnings': ['YES']}
 """

_betterexample = 'http://adsabs.harvard.edu/cgi-bin/nph-abs_connect?db_key=ALL&warnings=YES&version=1&bibcode=arXiv%3Aastro-ph/0309704%0D%0AarXiv%3A1302.1160&data_type=Custom&format=%25c&nr_to_return=100&start_nr=1'
_parsesto = """
{'bibcode': ['arXiv:astro-ph/0309704\r\narXiv:1302.1160'],
 'data_type': ['Custom'],
 'format': ['%c'],
 'http://adsabs.harvard.edu/cgi-bin/nph-abs_connect?db_key': ['ALL'],
 'nr_to_return': ['100'],
 'start_nr': ['1'],
 'version': ['1'],
 'warnings': ['YES']}
"""

"""
Custom "%Q %c" yields:
Query Results from the ADS Database


Retrieved 3 abstracts, starting with number 1.  Total number selected: 3.

eprint arXiv:1302.1160 0

eprint arXiv:1112.1067 0

eprint arXiv:astro-ph/0309704 0
"""

def query_ads_for_citations_from_arxiv_ids(ids, adsurl='http://adsabs.harvard.edu', nperquery=100, waittime=30):
    """
    Gets the citation counts from ADS from a list of arxiv ids.

    Parameters
    ----------
    ids : list of str
        The arxiv ids
    adsurl : str
        The base url for the ADS mirror to query
    nperquery : int
        Number of records to get per query
    waittime : number
        Seconds to wait between queries

    Returns
    -------
    citations : list of ints
        The citation count for the articles in `ids`
    """
    from urllib import urlencode
    from urllib2 import urlopen

    if len(ids) <= nperquery:
        bibcodes = 'arXiv:' + '\r\narXiv:'.join(ids)

        params = [('db_key', 'all'),
                  ('version', '1'),
                  ('bibcode', bibcodes),
                  ('data_type', 'Custom'),
                  ('format', '%Q %c'),
                  ('nr_to_return', str(len(ids))),
                  ('start_nr', '1')]

        url = adsurl + '/cgi-bin/nph-abs_connect?' + urlencode(params)

        u = urlopen(url)
        try:
            res = u.read()
        finally:
            u.close()

        adsids = []
        citecounts = []

        for l in res.split('\n'):
            if l[:13] == 'eprint arXiv:':
                adsid, citecount = l[13:].split()
                adsids.append(adsid)
                citecounts.append(int(citecount))
        raise NotImplementedError('Reorider citecounts')
    else:
        n = len(ids)
        i = 0
        citecounts = []
        while (i * nperquery) < n:
            i1 = i * nperquery
            i2 = (i + 1) * nperquery
            citecounts.extend(query_ads_for_citations_from_arxiv_ids(ids[i1:i2],
                adsurl, nperquery, waittime)

    return citecounts


def get_arxiv_ids(recprefix='arXiv_oai/reclist', sessionnum=''):
    """
    Returns a list of the arxiv IDS from the OAI2 session
    """
    from glob import glob
    from xml.etree import ElementTree

    fns = glob(recprefix + str(sessionnum) + '*')

    ids = []
    for fn in fns:
        et = ElementTree(fn)
        ids.extend([e.text for e in et.findall('.//{http://arxiv.org/OAI/arXivRaw/}id')])

    return ids


def do_arxiv_session(incremental=False):
    harvkwargs = dict(incremental=False, basewritename='arXiv_oai/reclist',
        startdate=None, format='arXivRaw', recordset='physics:astro-ph',
        baseurl='http://export.arxiv.org/oai2', recnumpadding=4)

    print 'Running OAI2Harvester with', harvkwargs

    return arxivoai2.run_session(**harvkwargs)
