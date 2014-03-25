#!/usr/bin/env python
from __future__ import division

from urllib import urlencode
from urllib2 import urlopen
from contextlib import closing
from xml import sax




class AdsFromArxiv(sax.handler.ContentHandler):
    def __init__(self,adsurl='http://adsabs.harvard.edu'):
        self.inarxivcode = ''
        self.outarxivcode = ''
        self.citations = 0

        self.curr = None
        self.currname = ''

        if adsurl.startswith('http://'):
            self.adsurl = adsurl
        else:
            self.adsurl = 'http://' + adsurl

    def reset(self):
        self.__init__()

    def startElement(self, name, attrs):
        if name=='eprintid' or name=='citations':
            self.currname = name
            self.curr = []

    def endElement(self, name):
        if self.curr is not None:
            s = ''.join(self.curr)

            if self.currname == 'eprintid':
                if s.startswith('arXiv:'):
                    self.outarxivcode = s[6:]
            elif self.currname == 'citations':
                self.citations = int(s)

            self.curr = None
            self.currname = ''

    def characters(self, chrs):
        if self.curr is not None:
            self.curr.append(chrs)

    _ads_query = '%s/cgi-bin/bib_query?arXiv:%s&data_type=SHORT_XML'
    def query_ads(self,arxivcode):
        if '.' not in arxivcode:
            arxivcode = 'astro-ph/'+arxivcode
        url = AdsFromArxiv._ads_query%(self.adsurl,arxivcode)
        with closing(urlopen(url)) as w:
            s = w.read()

        sax.parseString(s,self)
        self.inarxivcode = arxivcode
        return url



class ArxivSearcher(sax.handler.ContentHandler):
    def __init__(self):
        self.arxivids = []
        self.pubdates = []
        self.totabs = -1

        self.curr = None
        self.currid = None
        self.currpub = None
        self.currname = ''
        self.inentry = False



    def reset(self):
        self.__init__()

    def startElement(self, name, attrs):
        if name=='opensearch:totalResults' or name=='published' or name=='id':
            self.currname = name
            self.curr = []
        elif name == 'entry':
            self.inentry = True

    def endElement(self, name):
        if name == 'entry':
            self.inentry = False
            self.arxivids.append(self.currid)
            self.pubdates.append(self.currpub)
            self.currid = None
            self.currpub = None

        elif self.curr is not None:
            s = ''.join(self.curr)

            if self.currname == 'opensearch:totalResults':
                self.totabs = int(s)
            elif self.currname == 'id' and self.currid is None:
                self.currid = s.split('/')[-1][:-2]
            elif self.currname == 'published' and self.currpub is None:
                self.currpub = s

            self.curr = None
            self.currname = ''

    def characters(self, chrs):
        if self.curr is not None:
            self.curr.append(chrs)
    _arxivquery='http://export.arxiv.org/api/query?search_query=submittedDate:[{syr}+TO+{eyr}]+AND+cat:astro-ph*&sortBy=submittedDate&sortO%20rder=ascending&start={st}&max_results={mx}'
    def query_arxiv(self,start=0,mx=10,syr=1992,eyr=2012):
        url = ArxivSearcher._arxivquery.format(st=start,mx=mx,syr=syr,eyr=eyr)
        with closing(urlopen(url)) as w:
            s = w.read()

        sax.parseString(s,self)


class Searcher(object):
    def __init__(self,pfn=None,nperarxiv=1000,pickleperads=50,startyr=1992):
        self.ids = []
        self.dates = []
        self.currids = []
        self.currdates = []
        self.citations = []
        self.peerrev = []
        self.startarxivtotal = 0
        self.startedyr = None
        self.picklefn = pfn
        self.nskipped = 0
        self.nperarxiv = nperarxiv
        self.pickleperads = pickleperads
        self.yearsdone = []
        self.startyr = startyr

    def arxiv_search(self,waittime=3):
        import time,cPickle,datetime

        nper = self.nperarxiv

        #figure out the years to search - arxiv astro-ph starts in 1992
        yrs = [yr+self.startyr for yr in range(datetime.datetime.now().year+1-self.startyr)]
        print 'yrs',yrs

        for yr in yrs:
            if yr in self.yearsdone:
                continue
            print 'Starting search for year',yr

            sr = ArxivSearcher()
            sr.query_arxiv(len(self.currids)+self.startarxivtotal,nper,syr=yr,eyr=yr+1)
            if self.startedyr is None:
                self.startedyr = datetime.datetime.now()
                self.startarxivtotal = sr.totabs
            target = self.startarxivtotal
            print 'Year',yr,'Has',target,'in total'

            while len(self.currids)<target:
                t0 = time.time()
                self.currids.extend(sr.arxivids)
                self.currdates.extend(sr.pubdates)
                startoffset = sr.totabs - self.startarxivtotal

                nleft = target - len(self.currids)
                print 'Have',len(self.currids),'Abstracts',nleft,'Remaining in year',yr,'(Skipped %i)'%startoffset
                if self.picklefn is not None:
                    print 'Pickling to',self.picklefn
                    with open(self.picklefn,'w') as f:
                        cPickle.dump(self,f,-1)

                t1 = time.time()
                stime = waittime - (t1 - t0)
                if stime > 0:
                    print 'Sleeping',stime,'s'
                    time.sleep(stime)
                sr.reset()
                sr.query_arxiv(len(self.currids)+startoffset,nper,syr=yr,eyr=yr+1)

            print 'Search of year',yr,'Complete','(Got %i,Skipped %i)'%(len(self.currids),sr.totabs-self.startarxivtotal)
            self.resetyr(yr)
        if self.picklefn is not None:
            print 'Pickling to',self.picklefn
            with open(self.picklefn,'w') as f:
                cPickle.dump(self,f,-1)
        print 'Completed Search, found',len(self.ids),'Abstracts'


    def resetyr(self,yr):
        if yr is not None:
            self.yearsdone.append(yr)
        self.ids.extend(self.currids)
        self.dates.extend(self.currdates)
        self.currids = []
        self.currdates = []
        self.startedyr = None
        self.startarxivtotal = 0

    def get_cite_count(self,waittime=.5,adsurl='http://adsabs.harvard.edu'):
        import time,cPickle,urllib2

        pickleper = self.pickleperads
        aa = AdsFromArxiv(adsurl=adsurl)
        timeouts = 0

        while len(self.citations)<len(self.ids):
            t0 = time.time()
            try:
                lasturl = aa.query_ads(self.ids[len(self.citations)])
                if aa.outarxivcode == '':
                    self.citations.append(aa.citations)
                    self.peerrev.append(False)
                else:
                    incode = aa.inarxivcode
                    if incode.endswith('v'):
                        incode = incode[:-1]
                    elif incode[-2] == 'v':
                        incode = incode[:-2]

                    if incode == aa.outarxivcode:
                        self.citations.append(aa.citations)
                        self.peerrev.append(True)
                    else:
                        raise ValueError('arxiv codes do not match: %s , %s from url %s'%(aa.inarxivcode,aa.outarxivcode,lasturl))
            except urllib2.HTTPError,e1:
                try:
                    float(self.ids[len(self.citations)])
                except ValueError,e2:
                    print 'Invalid arxiv id',self.ids[len(self.citations)],'skipping'
                    self.nskipped += 1
                    del self.ids[len(self.citations)]
                    del self.dates[len(self.citations)]
                if e1.code == 404:
                    print 'URL',e1.geturl(),'not found on ADS, skipping'
                    self.nskipped += 1
                    del self.ids[len(self.citations)]
                    del self.dates[len(self.citations)]
                elif e1.code == 110:
                    timeouts += 1
                    waitmin = min(3*timeouts,60)
                    print 'URL',e1.geturl(),'Timed out - trying again in',waitmin,'min'
                    time.sleep(60*waitmin)
                    continue
                else:
                    print 'HTTP Error at URL',e1.geturl()
                    raise

            timeouts = 0
            print 'Got Citations for #',len(self.citations),',',len(self.ids)-len(self.citations),'Remaining'

            if self.picklefn is not None and len(self.citations)%pickleper==0:
                print 'Pickling to',self.picklefn
                with open(self.picklefn,'w') as f:
                    cPickle.dump(self,f,-1)


            aa.reset()

            t1 = time.time()
            stime = waittime - (t1 - t0)
            if stime > 0:
                print 'Sleeping',stime,'s'
                time.sleep(stime)

        if self.picklefn is not None:
            print 'Final Pickling to',self.picklefn
            with open(self.picklefn,'w') as f:
                cPickle.dump(self,f,-1)


    def cite_array(self):
        from numpy import array

        return array(self.citations)

    def wd_array(self,skipweekends=True):
        """
        wd at which the paper appears on the listing.
        Monday is 0 and Sunday is 6
        If `skipweekends` is true, Sat/Sun are pushed to Mon
        """
        from numpy import array
        import datetime
        from pytz import timezone,UTC

        wd = []
        for d in self.dates:
            if d is None:
                wd.append(-1)
            else:
                da,ti = d.split('T')
                dati = []
                dati.extend(da.split('-'))
                dati.extend(ti.replace('Z','').split(':'))
                dati = [int(e) for e in dati]
                edt = datetime.datetime(*dati,**dict(tzinfo=UTC)).astimezone(timezone('US/Eastern'))
                if edt.hour<16:
                    wd.append((edt.weekday()+1)%7)
                else:
                    wd.append((edt.weekday()+2)%7)
        res = array(wd)
        if skipweekends:
            res[res>4]=0
        return res

    def rank_in_day_array(self,reversedrank=False):
        """
        `reversedrank` means -1 for last, -2 for second-to-last, etc.

        returns rank,mask,
        """
        from pytz import timezone,UTC
        from datetime import datetime,timedelta
        from numpy import array,zeros,argsort
        from collections import defaultdict
        from time import strptime

        ranks = zeros(len(self.ids))
        ords = []
        msk = []

        fmt = '%Y-%m-%dT%H:%M:%SZ'
        tzeastern = timezone('US/Eastern')
        dt1 = timedelta(1)
        for dstr in self.dates:
            if dstr is not None:
#                edt = datetime(*(strptime(dstr, fmt)[0:6]),**dict(tzinfo=UTC))
#                if edt.hour<16:
#                    date = (edt + dt1).date()
#                else:
#                    date = (edt + dt1 + dt1).date()
                date = datetime(*(strptime(dstr, fmt)[0:6]),**dict(tzinfo=UTC))
                edate = date.astimezone(tzeastern)
                if edate.hour<16:
                    ords.append(edate.toordinal())
                else:
                    ords.append(edate.toordinal()+1)
                msk.append(True)
            else:
                ords.append(-1)
                msk.append(False)
        ords = array(ords)
        msk = array(msk)

        dis = defaultdict(list)
        dids = defaultdict(list)
        for i,(o,id) in enumerate(zip(ords,self.ids)):
            try:
                dids[o].append(float(id.replace('v','')))
                dis[o].append(i)
            except ValueError:
                pass
                #print 'Nday failed for',i,o,'Due to id of',id

        for o in dis.keys():
            sorti = argsort(dids[o])
            for i,rank in zip(dis[o],sorti):
                if reversedrank:
                    ranks[i] = rank - len(sorti)
                else:
                    ranks[i] = rank+1

        msk[ranks==0] = False
        return ranks,msk

    def papers_over_time(self):
        from pytz import timezone,UTC
        from datetime import datetime,timedelta
        from numpy import array,zeros,argsort
        from collections import defaultdict
        from time import strptime

        ranks = zeros(len(self.ids))
        ords = []
        msk = []

        fmt = '%Y-%m-%dT%H:%M:%SZ'
        tzeastern = timezone('US/Eastern')
        dt1 = timedelta(1)
        for dstr in self.dates:
            if dstr is not None:
#                edt = datetime(*(strptime(dstr, fmt)[0:6]),**dict(tzinfo=UTC))
#                if edt.hour<16:
#                    date = (edt + dt1).date()
#                else:
#                    date = (edt + dt1 + dt1).date()
                date = datetime(*(strptime(dstr, fmt)[0:6]),**dict(tzinfo=UTC))
                edate = date.astimezone(tzeastern)
                if edate.hour<16:
                    ords.append(edate.toordinal())
                else:
                    ords.append(edate.toordinal()+1)
                msk.append(True)
            else:
                ords.append(-1)
                msk.append(False)
        ords = array(ords)
        msk = array(msk)

        dis = defaultdict(list)
        dids = defaultdict(list)
        for i,(o,id) in enumerate(zip(ords,self.ids)):
            try:
                dids[o].append(float(id.replace('v','')))
                dis[o].append(i)
            except ValueError:
                print 'Nday failed for',i,o,'Due to id of',id
                dis[o]

        os = []
        ns = []
        for o in dis.keys():
            os.append(o)
            ns.append(len(dids[o]))
        os,ns = arrary(os),array(ns)

        return os[os>0],ns[os>0]

    def cite_by_wd(self,filter0=True,skipweekends=True):
        wd = self.wd_array(skipweekends)
        ndays = 5 if skipweekends else 7
        cs = self.cite_array()
        if wd.size!=cs.size:
            wd = wd[:cs.size]

        if filter0:
            cspd = [cs[(i==wd)&(0!=cs)] for i in range(ndays)]
        else:
            cspd = [cs[i==wd] for i in range(ndays)]

        return cspd

    def zipf_day_plots(self,filter0=True,skipweekends=True):
        import numpy as np
        from matplotlib import pyplot as plt

        cbw = self.cite_by_wd(filter0,skipweekends)

        if skipweekends:
            days = ['Sa/Su/M','Tu','W','Th','F']
        else:
            days = ['M','Tu','W','Th','F','Sa','Su']

        for c,d in zip(cbw,days):
            x = (np.arange(c.size)+1)/c.size
            y = c[np.argsort(c)][::-1]

            plt.loglog(x,y,label=d)
        plt.legend(loc=0)
        plt.xlabel('$r/N$')
        plt.ylabel('Citations')

    def zipf_rank_plots(self,filter0=True):
        import numpy as np
        from matplotlib import pyplot as plt

        r,mr = self.rank_in_day_array()
        print 'Between ranks'
        rr,mr = self.rank_in_day_array(True)
        cs = self.cite_array()

        if r.size!=cs.size:
            r = r[:cs.size]
        if rr.size!=cs.size:
            rr = rr[:cs.size]

        labels = ['1','2','3','4','5','>5','last']
        msks = [r==1,r==2,r==3,r==4,r==5,r>5,rr==-1]

        for msk,l in zip(msks,labels):
            c = cs[msk]
            x = (np.arange(c.size)+1)/c.size
            y = c[np.argsort(c)][::-1]

            plt.loglog(x,y,label=l)
        plt.legend(loc=0)
        plt.xlabel('$r/N$')
        plt.ylabel('Citations')

    def compare_zipf_rand(self,filter0=True,skipweekends=True):
        import numpy as np
        from numpy import random
        from matplotlib import pyplot as plt

        #real data
        cbw = self.cite_by_wd(filter0,skipweekends)
        if skipweekends:
            days = ['Sa/Su/M','Tu','W','Th','F']
        else:
            days = ['M','Tu','W','Th','F','Sa','Su']

        realys = [np.sort(c)[::-1] for c in cbw]
        realsz = np.min([r.size for r in realys])
        realxs = np.array([(np.arange(c.size)+1)[-realsz:]/c.size for c in cbw])
        realys = np.array([r[-realsz:] for r in realys])

        #sim data
        all = np.concatenate(self.cite_by_wd())
        ps = random.permutation(all.size)

        #equal size slices
        pse = [np.sort(all[ps[int(i*all.size/5):int((i+1)*all.size/5)]])[::-1] for i in range(5)]
        psesz = np.min([p.size for p in pse])
        psex = np.array([(np.arange(p.size)+1)[-psesz:]/p.size for p in pse])
        psey = np.array([p[-psesz:] for p in pse])

        #slices matching real
        psm = []
        i = 0
        for c in cbw:
            psm.append(np.sort(all[ps[i:(i+c.size)]])[::-1])
            i += c.size
        psmsz = np.min([p.size for p in psm])
        psmx = np.array([(np.arange(p.size)+1)[-psmsz:]/p.size for p in psm])
        psmy = np.array([p[-psmsz:] for p in psm])

        minx = np.min((np.min(psmx),np.min(psex),np.min(realxs)))
        x = np.logspace(-4,0,5000)
        nms = ['Equal-Space','Matched','Real']
        xs,ys = [psex,psmx,realxs],[psey,psmy,realys]
        for nm,xi,yi in zip(nms,xs,ys):
            print xi.shape,yi.shape
            y = np.std([np.interp(x,xi[i],yi[i]) for i in range(5)],axis=0)/np.mean([np.interp(x,xi[i],yi[i]) for i in range(5)],axis=0)
            plt.semilogx(x,y,label=nm)
        plt.legend(loc=0)
        plt.xlabel('$r/N$')
        plt.ylabel(r'$\sigma_{\rm cite}/\mu_{\rm cite}$')

    def ks_array(self):
        from numpy import empty
        from scipy.stats import ks_2samp

        cites = self.cite_by_wd()
        ksD = empty((7,7))
        ksp = empty((7,7))

        for i,ai in enumerate(cites):
            for j,aj in enumerate(cites):
                Dij,pij = ks_2samp(ai,aj)
                ksD[i,j] = Dij
                ksp[i,j] = pij

        return ksD,ksp

    def ks_plot(self,clf=True,stat='p',cut=None):
        import numpy as np
        from matplotlib import pyplot as plt
        from matplotlib import rcParams,cm

        ksD,ksp = self.ks_array()

        if clf:
            plt.clf()
        if stat=='p':
            kss = ksp
        elif stat=='D':
            kss = ksD
        else:
            raise ValueError('invalid stat '+str(stat))

        if clf:
            plt.clf()

        cmap = getattr(cm,rcParams['image.cmap'])
        if cut:
            newsdata = dict(cmap._segmentdata)
            for cn in ('red','green','blue'):
                cl = list(newsdata[cn])
                endy0,endy1 = cl[0][1],cl[0][2]
                del cl[0]
                cl.insert(0,(cut,endy0,endy1))
                cl.insert(0,(cut,0,0))
                cl.insert(0,(0,0,0))
                newsdata[cn] = tuple(cl)

            cmap = cmap.__class__(name=cmap.name+'-cut',segmentdata=newsdata)

        plt.imshow(kss,interpolation='nearest',origin='lower',cmap=cmap)
        plt.xticks(np.arange(7),['M','Tu','W','Th','F','Sa','Su'])
        plt.yticks(np.arange(7),['M','Tu','W','Th','F','Sa','Su'])
        plt.colorbar()

def funpickle(fileorname,number=0,usecPickle=True):
    """
    Unpickle a pickled object from a specified file and return the contents.

    :param fileorname: The file from which to unpickle objects
    :type fileorname: a file name string or a :class:`file` object
    :param number:
        The number of objects to unpickle - if <1, returns a single object.
    :type number: int
    :param usecPickle:
        If True, the :mod:`cPickle` module is to be used in place of
        :mod:`pickle` (cPickle is faster).
    :type usecPickle: bool

    :returns: A list of length given by `number` or a single object if number<1

    """
    if usecPickle:
        import cPickle as pickle
    else:
        import pickle

    if isinstance(fileorname,basestring):
        f = open(fileorname,'r')
        close = True
    else:
        f = fileorname
        close = False

    try:
        if number > 0:
            res = []
            for i in range(number):
                res.append(pickle.load(f))
        elif number < 0:
            res = []
            eof = False
            while not eof:
                try:
                    res.append(pickle.load(f))
                except EOFError:
                    eof = True
        else: #number==0
            res = pickle.load(f)
    finally:
        if close:
            f.close()

    return res

if __name__=='__main__':
    import os,cPickle,sys

    if os.path.exists('arxivads.pickle'):
        with open('arxivads.pickle') as f:
            sr = cPickle.load(f)
    else:
        sr = Searcher(pfn='arxivads.pickle')

    if len(sys.argv)<2:
        sys.argv.append('-s')
        sys.argv.append('-m')

    if '-s' in sys.argv:
        print 'Starting Search'
        sr.arxiv_search()
    if '-m' in sys.argv:
        i = sys.argv.index('-m')
        print 'Starting Match'
        if len(sys.argv)>(i+1):
            adsurl = sys.argv[i+1]
            sr.get_cite_count(adsurl=adsurl)
        else:
            sr.get_cite_count()

    if '-d' in sys.argv:
        from matplotlib import pyplot as plt
        plt.figure(1)
        sr.zipf_day_plots()
        plt.figure(2)
        sr.compare_zipf_rand()
        plt.show()
