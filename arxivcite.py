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


class OAI2Harvester(object):
    """
    A harvester following the OAI2 specification.
    """
    def __init__(self, basename='arXiv_oai/reclist', startdate=None,
                 format='arXivRaw', recordset='physics:astro-ph',
                 baseurl='http://export.arxiv.org/oai2', recnumpadding=4):

        self.basename = basename
        self.startdate = startdate
        self.format = format
        self.recordset = recordset
        self.baseurl = baseurl

        self.recnumpadding = recnumpadding

        self.reset_session()  # initializes session-related vars

    def reset_session(self):
        """
        Sets the state for a new session
        """
        self.sessionnum = None
        self.i = None
        self.currentreq = None

    def _get_last_session_info(self):
        from glob import glob

        fns = [fn[len(self.basename):] for fn in glob(self.basename + '*')]

        sessionnums = [int(fn.split('_')[0]) for fn in fns]
        lastsessionnum = 0 if len(sessionnums) == 0 else max(sessionnums)

        lastsessionfns = [fn for fn in fns if int(fn.split('_')[0]) == lastsessionnum]
        inums = [int(fn.split('_')[1]) for fn in lastsessionfns]
        mininum = min(inums)
        minfns = [fn for inum, fn in zip(inums, fns) if mininum]
        assert len(minfns) == 1
        firstfn = self.basename + minfns[0]

        return lastsessionnum, firstfn



    def clear_session_files(self, sessionnum):
        """
        Deletes the files associated with the given session number

        Returns a list of the deleted files' names
        """
        from os import unlink
        from glob import glob

        fns = glob(self.basename + str(sessionnum) + '_*')
        for fn in fns:
            unlink(fn)

        return fns

    def construct_start_url(self):
        """
        Returns the URL for the first request of a session
        """
        from urllib import urlencode

        params = [('verb', 'ListRecords'),
                  ('set', self.recordset),
                  ('metadataPrefix', self.format)]
        if self.startdate is not None:
            params.insert(1, ('from', self.startdate))

        return self.baseurl + '?' + urlencode(params)

    def construct_resume_url(self, token):
        """
        Returns the URL for a continuing request of a session
        """
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
        templ = '{0}{1}_{2}'
        if self.recnumpadding:
            templ = templ[:-1] + ':0' + str(int(self.recnumpadding)) + '}'
        return templ.format(self.basename, self.sessionnum, self.i + 1)

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

        self.currentreq = req
        return req

    def setup_incremental_session(self, prevsessionnum=None):
        """
        Sets up for a session that's an incremental update of the `precsessionnum`.
        If `prevsiessionnum` is None, uses the latest
        """
        from xml.etree import ElementTree

        if self.sessionnum is not None:
            raise ValueError('Already in a session.  Call reset_session() before doing this again.')

        if prevsessionnum is None:
            prevsessionnum, firstfn = self._get_last_session_info()
        else:
            firstistr = (('0' * (self.recnumpadding - 1)) if self.recnumpadding else '') + '1'
            firstfn = self.basename + str(prevsessionnum) + '_' + firstistr

        if prevsessionnum < 1:
            raise ValueError("No previous session to update from!")

        with open(firstfn) as f:
            gotdate = gotreq = False

            for event, elem in ElementTree.iterparse(f):
                if elem.tag == '{http://www.openarchives.org/OAI/2.0/}responseDate':
                    datestr = elem.text
                    gotdate = True
                elif elem.tag == '{http://www.openarchives.org/OAI/2.0/}request':
                    if elem.attrib['verb'] != 'ListRecords':
                        raise ValueError('Verb for most recent session is {0}, but should be ListRecords!'.format(elem.attrib['verb']))
                    format = elem.attrib['metadataPrefix']
                    recset = elem.attrib['set']

                if gotdate and gotreq:
                    break
            else:
                if not gotdate and not gotreq:
                    raise ValueError('Could not find responseDate or request!')
                elif not gotdate:
                    raise ValueError('Could not find responseDate!')
                elif not gotreq:
                    raise ValueError('Could not find request!')
                else:
                    # should be unreachable
                    raise RuntimeError('unreachable')

            self.startdate = datestr
            self.format = format
            self.recordset = recset

    def start_session(self):
        """
        Do the initial request for a session

        Returns the continuation token or False if the session is finished.
        """
        if self.sessionnum is None:
            self.sessionnum = self._get_last_session_info()[0] + 1
            self.i = 0
        else:
            raise ValueError('Already in a session.  Call reset_session() before doing this again.')

        req = self.do_request(self.construct_start_url())

        print 'Writing request to', self.writefn
        with open(self.writefn, 'w') as f:
            f.write(req.text)

        res = self.extract_resume_info(req.text)
        if res is False:
            print 'Completed request in one go, no resumption info'
            self.reset_session()
            return False
        else:
            token, listsize, cursor = res
            print 'First request completed, nrecords:', listsize
            self.i += 1

        return token

    def continue_session(self, token):
        """
        Do the next request for a session

        Returns the continuation token, or False if the session completed
        """

        if self.sessionnum is None:
            raise ValueError("Can't continue a session that's not started!")

        req = self.do_request(self.construct_resume_url(token))

        print 'Writing request to', self.writefn
        with open(self.writefn, 'w') as f:
            f.write(req.text)

        res = self.extract_resume_info(req.text)
        if res is False:
            print "Couldn't find resumptionToken - possibly the request failed?"
            print "(Leaving session state alone - need to reset_session() to do anything more)"
            return False
        else:
            token, listsize, cursor = res
            if token == '':
                #blank token means this was the last request of the session
                print 'Completed request for session', self.sessionnum
                self.reset_session()
                return False
            else:
                print 'Request completed. cursor at ', cursor, 'of', listsize
                self.i += 1

        return token


if __name__ == '__main__':
    o = OAI2Harvester()
    print 'Running OAI2Harvester with', o.__dict__

    res = o.start_session()
    while res is not False:
        print 'Token: "{0}"'.format(res)
        res = o.continue_session(res)
