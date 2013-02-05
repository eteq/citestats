#!/usr/bin/env python
from __future__ import division

"""
A module to query arxiv and match to ADS for citation statistics.
"""

ipblockinfo = """
Thanks for the email.  The ip block is triggered automatically by "too
many in too short a time period" where, understandably, we don't want
to advertise that rate.  If you put 30 seconds between queries, you'll
probably avoid the block.  You can also spread out your searches over
multiple mirrors which will allow you to get results back quicker...

Last, if you do trigger the block, just let us know (ads@cfa.harvard.edu
is fine for correspondence), and we'll unblock you.
"""


class ArXivOAI2Harvester(object):
    """
    A harvester following the OAI2 specification.
    """
    def __init__(self, basename='arXiv_oai/reclist', startdate=None,
                 format='arXivRaw', recordset='physics:astro-ph',
                 baseurl='http://export.arxiv.org/oai2'):

        self.basename = basename
        self.startdate = startdate
        self.format = format
        self.recordset = recordset
        self.baseurl = baseurl

        self.sessionnum = None
        self.i = None

    def reset_session(self):
        from glob import glob

        fns = [fn[len(self.basename):] for fn in glob(self.basename + '*')]

        sessionnums = [int(fn.split('_')[0]) for fn in fns]
        self.sessionnum = 1 if len(sessionnums) == 0 else (max(sessionnums) + 1)
        self.i = 0

        return self.sessionnum

    def clear_session_files(self, sessionnum):
        from os import unlink
        from glob import glob

        fns = glob(self.basename + str(sessionnum) + '_*')
        for fn in fns:
            unlink(fn)

        return fns

    def construct_start_url(self):
        from urllib import urlencode

        params = [('verb', 'ListRecords'),
                  ('set', self.recordset),
                  ('metadataPrefix', self.format)]
        if self.startdate is not None:
            params.insert(1, ('from', self.startdate))

        return self.baseurl + '?' + urlencode(params)

    def construct_resume_url(self, token):
        from urllib import urlencode

        params = [('verb', 'ListRecords'),
                  ('resumptionToken', token)]

        return self.baseurl + '?' + urlencode(params)

    def extract_resume_info(self, reqtext):
        """
        returns False if info missing, else (token, listsize, cursor)
        """
        from xml.etree import ElementTree

        reqtext = reqtext[-1000:]  # this should always be enough to find the token?

        for l in reqtext.split('\n'):
            if l.startswith('<resumptionToken'):
                e = ElementTree.fromstring(l)
                cursor = int(e.attrib['cursor'])
                listsize = int(e.attrib['completeListSize'])
                token = e.text
                break
        else:
            return False

        return token, listsize, cursor

    @property
    def writefn(self):
        return self.basename + str(self.sessionnum) + "_" + str(self.i)

    def do_request(self, url):
        import requests
        from time import sleep

        req = requests.get(url)

        while (not req.ok):
            if req.status_code == 503:
                waittime = float(req.headers['retry-after'])
                print '503: asked to wait', waittime, 'sec'
                sleep(waittime)
                req = requests.get(url)
            else:
                msg = 'Request failed w/status code {code}. Contents:\n{text}'
                raise ValueError(msg.format(code=req.status_code, text=req.text), req)

        return req

    def start_session(self):
        if self.sessionnum is None:
            self.reset_session()

        req = self.do_request(self.construct_start_url())

        print 'Writing request to', self.writefn
        with open(self.writefn, 'w') as f:
            f.write(req.text)

        res = self.extract_resume_info(req.text)
        if res is False:
            print 'Completed request in one go, no resumption info'
            self.sessionnum = None
            self.i = None
            return False
        else:
            token, listsize, cursor = res
            print 'First request completed, nrecords:', listsize
            self.i += 1

        return token

    def continue_session(self, token):

        if self.sessionnum is None:
            raise ValueError("Can't continue a session that's not started!")

        req = self.do_request(self.construct_resume_url(token))

        print 'Writing request to', self.writefn
        with open(self.writefn, 'w') as f:
            f.write(req.text)

        res = self.extract_resume_info(req.text)
        if res is False:
            print 'Completed request for session', self.sessionnum
            self.sessionnum = None
            self.i = None
            return False
        else:
            token, listsize, cursor = res
            print 'Request completed. cursor at ', cursor, 'of', listsize
            self.i += 1

        return token


if __name__ == '__main__':
    a = ArXivOAI2Harvester()
    print 'Running ArXivOAI2Harvester with', a.__dict__

    res = a.start_session()
    while res is not False:
        print 'Token: "{0}"'.format(res)
        res = a.continue_session(res)
