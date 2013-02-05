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

import requests
