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

_exampleurl="http://adsabs.harvard.edu/cgi-bin/nph-abs_connect?db_key=ALL&warnings=YES&version=1&bibcode=arXiv%3A1302.1193%0D%0AarXiv%3A1302.1160&nr_to_return=100&start_nr=1"
_parsesto="""
{'bibcode': ['arXiv:1302.1193\r\narXiv:1302.1160'],
 'http://adsabs.harvard.edu/cgi-bin/nph-abs_connect?db_key': ['ALL'],
 'nr_to_return': ['100'],
 'start_nr': ['1'],
 'version': ['1'],
 'warnings': ['YES']}
 """

if __name__ == '__main__':
    harvkwargs = dict(basewritename='arXiv_oai/reclist', startdate=None,
                 format='arXivRaw', recordset='physics:astro-ph',
                 baseurl='http://export.arxiv.org/oai2', recnumpadding=4)
    print 'Running OAI2Harvester with', harvkwargs

    arxivoai2.run_session(incremental=False, **harvkwargs)
