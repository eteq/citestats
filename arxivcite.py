#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division

"""
A module to query arxiv and match to ADS for citation statistics.
"""

from arxivoai2 import arxivoai2

mirrors = [
 ('Harvard-Smithsonian Center for Astrophysics, Cambridge, USA', 'http://adsabs.harvard.edu'),
 ('Centre de Donnes astronomiques de Strasbourg, France', 'http://cdsads.u-strasbg.fr'),
 ('University of Nottingham, United Kingdom', 'http://ukads.nottingham.ac.uk'),
 ('European Southern Observatory, Garching, Germany', 'http://esoads.eso.org'),
 ('Astronomisches Rechen-Institut, Heidelberg, Germany', 'http://ads.ari.uni-heidelberg.de'),
 ('Institute of Astronomy of the Russian Academy of Sciences, Moscow, Russia', 'http://ads.inasan.ru'),
 ('Main Astronomical Observatory, Kiev, Ukraine', 'http://ads.mao.kiev.ua'),
 ('Pontificia Universidad Catolica, Santiago, Chile', 'http://ads.astro.puc.cl'),
 ('National Astronomical Observatory, Tokyo, Japan', 'http://ads.nao.ac.jp'),
 ('National Astronomical Observatory, Chinese Academy of Science, Beijing, China', 'http://ads.bao.ac.cn'),
 #('Inter-University Centre for Astronomy and Astrophysics, Pune, India', 'http://ads.iucaa.ernet.in'),
 ('Indonesian Institute of Sciences, Jakarta, Indonesia', ' http://ads.arsip.lipi.go.id'),
 ('South African Astronomical Observatory', 'http://saaoads.chpc.ac.za'),
 ('Observatorio Nacional, Rio de Janeiro, Brazil', 'http://ads.on.br')
 ]

weekdaynumtostr = {0: 'Mon',
                   1: 'Tue',
                   2: 'Wed',
                   3: 'Thu',
                   4: 'Fri',
                   5: 'Sat',
                   6: 'Sun'
                  }


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


def get_cite_count_data_from_ads(arxivid, adsurl, urltimeout=5):
    """
    This gets run from process_data_from_ads
    """
    from urllib2 import urlopen
    from xml.etree import cElementTree

    url = '{adsurl}/cgi-bin/bib_query?{id}&data_type=SHORT_XML'.format(adsurl=adsurl, id=arxivid)
    urlobj = None

    try:
        urlobj = urlopen(url, timeout=urltimeout)
        et = cElementTree.parse(urlobj)
    finally:
        if hasattr(urlobj, 'close'):
            urlobj.close()

    data = {}
    citeelem = et.find('.//{http://ads.harvard.edu/schema/abs/1.1/references}citations')
    if citeelem is not None:
        data['ncites'] = int(citeelem.itertext().next())
    bibcodeelem = et.find('.//{http://ads.harvard.edu/schema/abs/1.1/references}bibcode')
    if bibcodeelem is not None:
        data['bibcode'] = bibcodeelem.itertext().next()
    titleelem = et.find('.//{http://ads.harvard.edu/schema/abs/1.1/references}title')
    if titleelem is not None:
        data['title'] = titleelem.itertext().next()
    authorelem = et.find('.//{http://ads.harvard.edu/schema/abs/1.1/references}author')
    if authorelem is not None:
        data['fauthor'] = authorelem.itertext().next()
    pubdateelem = et.find('.//{http://ads.harvard.edu/schema/abs/1.1/references}pubdate')
    if pubdateelem is not None:
        data['pubdate'] = pubdateelem.itertext().next()

    journalelem = et.find('.//{http://ads.harvard.edu/schema/abs/1.1/references}journal')
    if journalelem is not None:
        data['journal'] = jstr = journalelem.itertext().next()
        data['onlyarxiv'] = jstr.startswith('eprint arXiv')

    return data


def cite_count_proc(arxivid, adsurl, dbname, collname, waittime, laststarttime, outqueue):
    """
    This is run by ADSMirror as a subprocess
    """
    from pymongo import MongoClient
    import time
    import traceback

    try:
        dtime = time.time() - laststarttime
        if dtime < waittime:
            time.sleep(waittime - dtime)
    except BaseException as e:
        outqueue.put('error (while sleeping) before ' + arxivid)
        outqueue.put(laststarttime)
        outqueue.put(e)
        outqueue.put(traceback.format_exc())
        return

    qstarttime = time.time()
    try:
        data = get_cite_count_data_from_ads(arxivid, adsurl)
    except BaseException as e:
        outqueue.put('error (url) while getting ' + arxivid)
        outqueue.put(qstarttime)
        outqueue.put(e)
        outqueue.put(traceback.format_exc())
        return

    conn = None
    try:
        conn = MongoClient()
        coll = conn[dbname][collname]

        coll.update({'arxiv_id': arxivid}, {'$set': data})
    except Exception as e:
        BaseException.put('error (mongo) while setting ' + arxivid)
        outqueue.put(qstarttime)
        outqueue.put(e)
        outqueue.put(traceback.format_exc())
        return
    finally:
        if conn is not None:
            conn.close()
    endtime = time.time()

    outqueue.put('success at doing ' + arxivid)
    outqueue.put(qstarttime)
    outqueue.put(endtime - qstarttime)


class ADSMirror(object):
    def __init__(self, mirrorurl, mirrorname=''):
        self.url = mirrorurl
        self.name = mirrorname

        self.proc = self.queue = None

        self.currarxivid = None
        self.prevqtime = -float('inf')  # the time the last query finished
        self.qprocessingtime = []
        self.qtimestamp = []

        self.error = None
        self.errornoted = False
        self.timeoutcount = 0

    @property
    def readablename(self):
        return self.url if self.name == '' else self.name

    def __repr__(self):
        return '<ADSMirror: "{0}">'.format(self.readablename)

    def clear_error(self):
        self.error = None
        self.errornoted = False

    def set_error(self, error):
        self.error = error
        self.errornoted = False

    def spawn_arxiv_proc(self, arxivid, dbname, collname, waittime):
        """
        Does the work for this mirror, including waiting until the given
        `waittime` has passed.

        If it errors, will set self.error to whatever the error was
        """
        from multiprocessing import Process, Queue

        if not self.check_ready():
            raise ValueError('Cannot spawn arxiv process if not ready')

        self.currarxivid = arxivid

        self.queue = Queue()
        self.proc = Process(target=cite_count_proc, args=(arxivid, self.url, dbname, collname, waittime, self.prevqtime, self.queue))
        self.proc.start()

    def check_ready(self):
        """
        returns True if the process is ready, False otherwise.
        Also retrieves queue info and joins
        """
        import datetime

        if self.proc is not None:
            if self.proc.is_alive():
                return False
            else:
                self.proc.join()
                msg = self.queue.get_nowait()
                self.prevqtime = self.queue.get_nowait()
                if msg.startswith('success'):
                    self.currarxivid = None
                    self.qtimestamp.append(datetime.datetime.now())
                    self.qprocessingtime.append(self.queue.get_nowait())
                    self.timeoutcount = 0
                if msg.startswith('error'):
                    error = self.queue.get_nowait()
                    tb = self.queue.get_nowait()
                    self.error = (msg, error, tb)
                    # if a timeout error, increment the count
                    if self.timed_out():
                        self.timeoutcount += 1
                self.proc = None
        return self.error is None  # ready if proc is None and there is no error

    def timed_out(self):
        """
        Returns True if there is currently an error caused by a timeout, False
        otherwise
        """
        import socket
        from urllib2 import URLError

        issockto = isinstance(self.error[1], socket.timeout)
        urlwithsockto = isinstance(self.error[1], URLError) and isinstance(self.error[1].args[0], socket.timeout)

        return issockto or urlwithsockto

    def terminate_proc(self):
        if self.proc is not None:
            try:
                rdy = self.check_ready()
            except:
                #probably a queue problem
                rdy = None
            if not rdy:
                self.proc.terminate()
                self.proc.join()  # de-zombify
            self.proc = None

    def time_stats(self):
        from numpy import array

        ptarr = array(self.qprocessingtime)
        tsarr = array(self.qtimestamp, dtype='datetime64')

        return ptarr.mean(), ptarr.std(), ptarr, tsarr


class ADSQuerier(object):
    def __init__(self, dbname='citestats', collname='astroph',
                 mirrorurls=mirrors, querywaittime=30, overwritedb=False,
                 mainloopsleeptime=1, statuslinewaittime=120,
                 timeoutwaittime=120, timeoutlimit=5):

        self.dbname = dbname
        self.collname = collname
        self.querywaittime = querywaittime
        self.overwritedb = overwritedb
        self.mainloopsleeptime = mainloopsleeptime
        self.statuslinewaittime = statuslinewaittime
        self.timeoutwaittime = timeoutwaittime
        self.timeoutlimit = timeoutlimit

        self.mirrors = []
        for m in mirrorurls:
            if isinstance(m, basestring):  # just URL
                self.mirrors.append(ADSMirror(m))
            else:  # (name, URL) tuple
                self.mirrors.append(ADSMirror(m[1], m[0]))

    def get_arxiv_ids(self, overwrite=False):
        from pymongo import MongoClient

        conn = MongoClient()
        try:
            coll = conn[self.dbname][self.collname]

            if overwrite:
                return [doc['arxiv_id'] for doc in coll.find()]
            else:
                return [doc['arxiv_id'] for doc in coll.find() if not 'bibcode' in doc]

        finally:
            conn.close()

    def main_loop(self, launchspread=0):
        import time

        aidstoquery = self.get_arxiv_ids(self.overwritedb)

        nstart = len(aidstoquery)
        print '# of IDs to start with:', nstart

        laststatustime = -float('inf')
        sttime = time.time()
        launched = False
        while len(aidstoquery) > 0 or any([m.currarxivid is not None for m in self.mirrors]):
            #check if each mirror is available, try to give a job, if not check for errors
            allerrored = True
            for m in self.mirrors:
                if m.check_ready():
                    if not launched:
                        time.sleep(launchspread)
                    aid = aidstoquery.pop()
                    m.spawn_arxiv_proc(aid, self.dbname, self.collname, self.querywaittime)
                    allerrored = False
                elif m.error is None:
                    allerrored = False
                else:  # error is not None

                    if m.currarxivid is not None:
                        aidstoquery.append(m.currarxivid)
                        m.currarxivid = None
                    if m.timed_out():
                        if m.timeoutcount < self.timeoutlimit:
                            print 'Resetting timeout error, but waiting', self.timeoutwaittime, 'sec'
                            m.errornoted = True
                            m.clear_error()
                            # this tricks the mirror into thinking it has to wait `timeoutwaittime` from now
                            m.prevqtime = time.time() + self.timeoutwaittime - self.querywaittime
                        elif m.timeoutcount == self.timeoutlimit:
                            print 'Timed out', self.timeoutlimit, 'times - DEACTIVATING mirror', m.readablename
                            m.errornoted = True
                            m.timeoutcount += 1  # silences future visits
                    elif not m.errornoted:
                        print 'Error for mirror', m
                        print 'Error name:', m.error[0]
                        print 'Error object:', m.error[1]
                        print 'Error tb:', m.error[2]
                        m.errornoted = True
            if allerrored:
                print 'All mirrors in error state!  Dropping out of main loop'
                return

            launched = True

            if (time.time() - laststatustime) >= self.statuslinewaittime:
                elapsedhr = (time.time() - sttime) / 3600.
                hrperquery = elapsedhr / (nstart - len(aidstoquery))
                remhr = hrperquery * len(aidstoquery)
                msg = 'STATUS: {0} remaining IDs, {1} hr elapsed, ~{2} hr remaining.  {3} (of {4}) mirrors active.'
                print msg.format(len(aidstoquery), elapsedhr, remhr,
                                 sum([m.error is None for m in self.mirrors]),
                                 len(self.mirrors))
                laststatustime = time.time()

            time.sleep(self.mainloopsleeptime)

    def clear_keyboard_interrupts(self):
        for m in self.mirrors:
            m.check_ready()
            if m.error is not None:
                if isinstance(m.error[1], KeyboardInterrupt):
                    m.clear_error()

    def clear_all_errors(self):
        for m in self.mirrors:
            m.check_ready()
            m.clear_error()

    def mirror_time_stats(self):
        d = {}
        for m in self.mirrors:
            d[m.readablename] = m.time_stats()[:2]
        return d











# Below is a bunch of info text
#--------------------------------

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
_result = """
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
