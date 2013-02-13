#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

_absexample = "http://adsabs.harvard.edu/cgi-bin/bib_query?arXiv:1206.2619&data_type=SHORT_XML"
_yields = """
<?xml version="1.0"?>
<records xmlns="http://ads.harvard.edu/schema/abs/1.1/references" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://ads.harvard.edu/schema/abs/1.1/references http://ads.harvard.edu/schema/abs/1.1/references.xsd" retrieved="1" start="1" selected="1">
<record>
<bibcode>2012MNRAS.423.3134F</bibcode>
<title>A spectroscopic survey of Andromeda's Western Shelf</title>
<author>Fardal, Mark A.</author>
<author>Guhathakurta, Puragra</author>
<author>Gilbert, Karoline M.</author>
<author>Tollerud, Erik J.</author>
<author>Kalirai, Jason S.</author>
<author>Tanaka, Mikito</author>
<author>Beaton, Rachael</author>
<author>Chiba, Masashi</author>
<author>Komiyama, Yutaka</author>
<author>Iye, Masanori</author>
<journal>Monthly Notices of the Royal Astronomical Society, Volume 423, Issue 4, pp. 3134-3147.</journal>
<pubdate>Jul 2012</pubdate>
<link type="ABSTRACT">
  <name>Abstract</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=ABSTRACT</url>
</link>
<link type="EJOURNAL">
  <name>Electronic On-line Article (HTML)</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=EJOURNAL</url>
</link>
<link type="ARTICLE">
  <name>Full Printable Article (PDF/Postscript)</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=ARTICLE</url>
</link>
<link type="PREPRINT" access="open">
  <name>arXiv e-print</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=PREPRINT</url>
</link>
<link type="REFERENCES">
  <name>References in the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=REFERENCES</url>
  <count>49</count>
</link>
<link type="CITATIONS">
  <name>Citations to the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=CITATIONS</url>
  <count>3</count>
</link>
<link type="REFCIT">
  <name>Refereed Citations to the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=REFCIT</url>
</link>
<link type="SIMBAD">
  <name>SIMBAD Objects</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=SIMBAD</url>
  <count>1</count>
</link>
<link type="AR">
  <name>Also-Read Articles</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=AR</url>
</link>
<link type="OPENURL">
  <name>Library Link Server</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012MNRAS.423.3134F&#38;link_type=OPENURL</url>
</link>
<score>1.000</score>
<citations>3</citations>
<DOI>10.1111/j.1365-2966.2012.21094.x</DOI>
<eprintid>arXiv:1206.2619</eprintid>
</record>

</records>

"""

_cite_query = 'http://adsabs.harvard.edu/cgi-bin/nph-ref_query?bibcode=2012MNRAS.423.3134F&amp;refs=CITATIONS&amp;db_key=AST&data_type=SHORT_XML'
_result="""
<?xml version="1.0"?>
<records xmlns="http://ads.harvard.edu/schema/abs/1.1/references" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://ads.harvard.edu/schema/abs/1.1/references http://ads.harvard.edu/schema/abs/1.1/references.xsd" retrieved="3" start="1" selected="3">
<record>
<bibcode>2013ApJ...763....4L</bibcode>
<title>PAndAS in the Mist: The Stellar and Gaseous Mass within the Halos of M31 and M33</title>
<author>Lewis, Geraint F.</author>
<author>Braun, Robert</author>
<author>McConnachie, Alan W.</author>
<author>Irwin, Michael J.</author>
<author>Ibata, Rodrigo A.</author>
<author>Chapman, Scott C.</author>
<author>Ferguson, Annette M. N.</author>
<author>Martin, Nicolas F.</author>
<author>Fardal, Mark</author>
<author>Dubinski, John</author>
<author>Widrow, Larry</author>
<author>Dougal Mackey, A.</author>
<author>Babul, Arif</author>
<author>Tanvir, Nial R.</author>
<author>Rich, Michael</author>
<journal>The Astrophysical Journal, Volume 763, Issue 1, article id. 4, 10 pp. (2013).</journal>
<pubdate>Jan 2013</pubdate>
<link type="ABSTRACT">
  <name>Abstract</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=ABSTRACT</url>
</link>
<link type="EJOURNAL">
  <name>Electronic On-line Article (HTML)</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=EJOURNAL</url>
</link>
<link type="ARTICLE">
  <name>Full Printable Article (PDF/Postscript)</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=ARTICLE</url>
</link>
<link type="PREPRINT" access="open">
  <name>arXiv e-print</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=PREPRINT</url>
</link>
<link type="REFERENCES">
  <name>References in the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=REFERENCES</url>
  <count>74</count>
</link>
<link type="CITATIONS">
  <name>Citations to the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=CITATIONS</url>
  <count>1</count>
</link>
<link type="REFCIT">
  <name>Refereed Citations to the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=REFCIT</url>
</link>
<link type="SIMBAD">
  <name>SIMBAD Objects</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=SIMBAD</url>
  <count>2</count>
</link>
<link type="AR">
  <name>Also-Read Articles</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=AR</url>
</link>
<link type="OPENURL">
  <name>Library Link Server</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2013ApJ...763....4L&#38;link_type=OPENURL</url>
</link>
<score>1.000</score>
<citations>1</citations>
<DOI>10.1088/0004-637X/763/1/4</DOI>
<eprintid>arXiv:1211.4059</eprintid>
</record>

<record>
<bibcode>2012arXiv1211.4522S</bibcode>
<title>An analytical phase-space model for tidal caustics</title>
<author>Sanderson, Robyn E.</author>
<author>Helmi, Amina</author>
<journal>eprint arXiv:1211.4522</journal>
<pubdate>Nov 2012</pubdate>
<link type="ABSTRACT">
  <name>Abstract</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012arXiv1211.4522S&#38;link_type=ABSTRACT</url>
</link>
<link type="PREPRINT" access="open">
  <name>arXiv e-print</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012arXiv1211.4522S&#38;link_type=PREPRINT</url>
</link>
<link type="REFERENCES">
  <name>References in the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012arXiv1211.4522S&#38;link_type=REFERENCES</url>
  <count>33</count>
</link>
<link type="AR">
  <name>Also-Read Articles</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012arXiv1211.4522S&#38;link_type=AR</url>
</link>
<score>1.000</score>
<eprintid>arXiv:1211.4522</eprintid>
</record>

<record>
<bibcode>2012A&amp;A...545A..33E</bibcode>
<title>Quadruple-peaked spectral line profiles as a tool to constrain gravitational potential of shell galaxies</title>
<author>Ebrová, I.</author>
<author>Jílková, L.</author>
<author>Jungwiert, B.</author>
<author>Křížek, M.</author>
<author>Bílek, M.</author>
<author>Bartošková, K.</author>
<author>Skalická, T.</author>
<author>Stoklasová, I.</author>
<journal>Astronomy &#38; Astrophysics, Volume 545, id.A33, 15 pp.</journal>
<pubdate>Sep 2012</pubdate>
<link type="ABSTRACT">
  <name>Abstract</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=ABSTRACT</url>
</link>
<link type="EJOURNAL">
  <name>Electronic On-line Article (HTML)</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=EJOURNAL</url>
</link>
<link type="ARTICLE">
  <name>Full Printable Article (PDF/Postscript)</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=ARTICLE</url>
</link>
<link type="PREPRINT" access="open">
  <name>arXiv e-print</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=PREPRINT</url>
</link>
<link type="REFERENCES">
  <name>References in the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=REFERENCES</url>
  <count>63</count>
</link>
<link type="CITATIONS">
  <name>Citations to the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=CITATIONS</url>
  <count>1</count>
</link>
<link type="REFCIT">
  <name>Refereed Citations to the Article</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=REFCIT</url>
</link>
<link type="SIMBAD">
  <name>SIMBAD Objects</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=SIMBAD</url>
  <count>6</count>
</link>
<link type="AR">
  <name>Also-Read Articles</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=AR</url>
</link>
<link type="OPENURL">
  <name>Library Link Server</name>
  <url>http://adsabs.harvard.edu/cgi-bin/nph-data_query?bibcode=2012A%26A...545A..33E&#38;link_type=OPENURL</url>
</link>
<score>1.000</score>
<citations>1</citations>
<DOI>10.1051/0004-6361/201219940</DOI>
<eprintid>arXiv:1207.0642</eprintid>
</record>

</records>
"""


mirrors = [
('Harvard-Smithsonian Center for Astrophysics, Cambridge, USA',
  'http://adsabs.harvard.edu'),
 ('Centre de Donnes astronomiques de Strasbourg, France',
  'http://cdsads.u-strasbg.fr'),
 ('University of Nottingham, United Kingdom', 'http://ukads.nottingham.ac.uk'),
 ('European Southern Observatory, Garching, Germany', 'http://esoads.eso.org'),
 ('Astronomisches Rechen-Institut, Heidelberg, Germany',
  'http://ads.ari.uni-heidelberg.de'),
 ('Institute of Astronomy of the Russian Academy of Sciences, Moscow, Russia',
  'http://ads.inasan.ru'),
 ('Main Astronomical Observatory, Kiev, Ukraine', 'http://ads.mao.kiev.ua'),
 ('Pontificia Universidad Catolica, Santiago, Chile',
  'http://ads.astro.puc.cl'),
 ('National Astronomical Observatory, Tokyo, Japan', 'http://ads.nao.ac.jp'),
 ('National Astronomical Observatory, Chinese Academy of Science, Beijing, China',
  'http://ads.bao.ac.cn'),
 ('Inter-University Centre for Astronomy and Astrophysics, Pune, India',
  'http://ads.iucaa.ernet.in'),
 ('Indonesian Institute of Sciences, Jakarta, Indonesia',
  'http://ads.arsip.lipi.go.id'),
 ('South African Astronomical Observatory', 'http://saaoads.chpc.ac.za'),
 ('Observatorio Nacional, Rio de Janeiro, Brazil', 'http://ads.on.br')
 ]


def query_ads_for_citations_from_arxiv_ids(ids, adsurl='http://adsabs.harvard.edu', nperquery=100, waittime=30):
    """
    Gets the citation counts from ADS from a list of arxiv ids.

    WARNING: this only gets cites to the *arXiv* version, not the refereed one.
    So this is probably useless.

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

        citecountd = {}

        for l in res.split('\n'):
            if l[:13] == 'eprint arXiv:':
                adsid, citecount = l[13:].split()
                citecountd[adsid] = int(citecount)

        citecounts = [citecountd[adsid] for adsid in ids]
    else:
        n = len(ids)
        i = 0
        citecounts = []
        while (i * nperquery) < n:
            i1 = i * nperquery
            i2 = (i + 1) * nperquery
            citecounts.extend(query_ads_for_citations_from_arxiv_ids(ids[i1:i2],
                adsurl, nperquery, waittime))

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
        print 'Processing', fn
        et = ElementTree.parse(fn)
        ids.extend([e.text for e in et.findall('.//{http://arxiv.org/OAI/arXivRaw/}id')])

    return ids


def do_arxiv_session(incremental=False):
    harvkwargs = dict(incremental=False, basewritename='arXiv_oai/reclist',
        startdate=None, format='arXivRaw', recordset='physics:astro-ph',
        baseurl='http://export.arxiv.org/oai2', recnumpadding=4)

    print 'Running OAI2Harvester with', harvkwargs

    return arxivoai2.run_session(**harvkwargs)


def start_mongodb(dbdir='db', port=None, waitforstartsecs=1, multimongo=False):
    """
    Starts mongodb in a backbground process
    """
    from time import sleep
    from subprocess import Popen, PIPE, STDOUT

    if not multimongo and len(find_mongo_pids()) > 0:
        raise ValueError('Mongo process(s) already open!')

    p = Popen(['mongod', '--dbpath', dbdir, '--logpath', dbdir + '/mongod.log', '--logappend'], stdout=PIPE, stderr=STDOUT)
    sleep(waitforstartsecs)
    res = p.poll()
    if res is not None:
        raise ValueError("Mongod ended immediately after starting - it probably errored")
    return p


def find_mongo_pids(printpsline=False):
    """
    Lists all the mongodb processes
    """
    from subprocess import Popen, PIPE

    proc = Popen(['ps', '-A'], stdout=PIPE)
    stdout = proc.communicate()[0]
    if proc.returncode != 0:
        raise OSError('ps -A failed!')

    pids = []
    for l in stdout.split('\n'):
        if 'mongod' in l and not '(mongod)' in l:
            if printpsline:
                print l
            pids.append(int(l.split()[0]))

    return pids


def kill_mongodbs():
    import os
    import signal

    pids = find_mongo_pids(True)
    if len(pids) == 0:
        raise ValueError('no mongodbs found!')

    for pid in pids:
        os.kill(pid, signal.SIGTERM)



def populate_mongodb_from_arxiv_reclists(reclistfns, dbname='citestats',
    collname='astroph', verbose=True):
    from datetime import datetime
    from xml.etree import cElementTree
    from glob import glob
    from pymongo import MongoClient

    monthstrtonum = {'Jan': 1,
                     'Feb': 2,
                     'Mar': 3,
                     'Apr': 4,
                     'May': 5,
                     'Jun': 6,
                     'Jul': 7,
                     'Aug': 8,
                     'Sep': 9,
                     'Oct': 10,
                     'Nov': 11,
                     'Dec': 12}

    if isinstance(reclistfns, basestring):
        reclistfns = glob(reclistfns)

    conn = MongoClient()
    try:
        coll = conn[dbname][collname]

        for fn in reclistfns:
            if verbose:
                print 'Populating db for file', fn

            et = cElementTree.parse(fn)
            for e in et.getroot().getchildren():
                if e.tag == '{http://www.openarchives.org/OAI/2.0/}ListRecords':
                    for record in e.getchildren():
                        if record.tag == '{http://www.openarchives.org/OAI/2.0/}record':
                            meta = record.find('{http://www.openarchives.org/OAI/2.0/}metadata')

                            idstr = ''.join(meta.find('.//{http://arxiv.org/OAI/arXivRaw/}id').itertext())
                            vers = meta.findall('.//{http://arxiv.org/OAI/arXivRaw/}version')
                            for v in vers:
                                if v.get('version') == 'v1':
                                    assert v[0].tag == '{http://arxiv.org/OAI/arXivRaw/}date'
                                    datestr = ''.join(v[0].itertext())
                                    break
                            else:
                                raise ValueError('No v1 found in record for id {0} in file {1}'.format(idstr, fn))

                            #covert datestr to a datetime
                            day, dt = datestr.split(',')

                            day, monthstr, year, tme = dt.split()[:-1]  # last is "GMT"
                            hr, mn, sec = [int(s) for s in tme.split(':')]
                            dt = datetime(int(year), monthstrtonum[monthstr], int(day), hr, mn, sec)

                            #insert into the db
                            coll.insert({'arxiv_id': idstr, 'arxiv_date': dt, 'arxiv_day': day})

                    break
            else:
                raise ValueError('Could not find ListRecords elemnt in reclist file ' + fn)
    finally:
        conn.close()


def get_data_from_ads(adsurl, arxivid):
    raise NotImplementedError


def update_record_from_ads(coll, aid, data):
    return coll.update({'arxiv_id': aid}, {'$set': data})


def run_ads_queries(dbname='citestats',
    collname='astroph', verbose=True):
    from pymongo import MongoClient

    conn = MongoClient()
    try:
        coll = conn[dbname][collname]

        raise NotImplementedError
    finally:
        conn.close()
