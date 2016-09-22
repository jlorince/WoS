import sys
import os
import zipfile, gzip, zlib, io
import pandas as pd
from lxml import etree
import multiprocessing as mp
#sys.path.append(os.path.expanduser('~')+'/pathos/')
#from pathos.helpers import mp
import numpy as np
import time
import glob
import datetime


import logging
logger = logging.getLogger('WoS processing')
hdlr = logging.FileHandler('log')
formatter = logging.Formatter('%(asctime)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

#z = zipfile.ZipFile("{}{}_DSSHU.zip".format(basedir,year))


years = np.arange(1950,2016,1).astype(str)

#basedir = 'Z:/DSSHU_ANNUAL_1950-2015/'
basedir = '/webofscience/diego/WoS_XML/xdata/data/'


def feed(queue, records):
    for rec in records:
        queue.put(rec)
    queue.put(None)

def calc(queueIn, queueOut):
    while True:
        rec = queueIn.get(block=True)
        if rec is None:
            queueOut.put('__DONE__')
            break

        result = process(rec)
        queueOut.put(result)


def write(queue, file_handle):
    records_logged = 0
    while True:
        result = queue.get()
        if result == '__DONE__':
            logger.info("{} --> ALL records logged ({})".format(file_handle.name,records_logged))
            break
        elif result is not None:
            file_handle.write(result+'\n')
            file_handle.flush()
            records_logged +=1
            if records_logged % 1000 == 0:
                logger.info("{} --> {} records complete".format(file_handle.name,records_logged))

def reader(files):
    n=0
    for fname in files:
        chunk = ''
        with gzip.open(fname, "r") as f:
            for line in f:
                line = line.strip()
                if '<REC' in line or ('<REC' in chunk and line != '</REC>'):
                    chunk += line
                    continue
                if line == '</REC>':
                    chunk += line
                    n += 1
                    yield chunk
                    chunk = ''

def process(chunk,fields='all'):
    paper = etree.fromstring(chunk)

    uid = paper.findall(".//UID")[0].text
    return uid



nThreads = 24
if __name__ == '__main__':
    #for year in years:
    year = '1950'
    year_start = time.time()
    filelist = [f for f in glob.glob(basedir+'*') if f[f.rfind('/'):][4:8]==year]
    chunks = reader(filelist)

    handle = open(year+'.txt','w')

    workerQueue = mp.Queue()
    writerQueue = mp.Queue()
    feedProc = mp.Process(target = feed , args = (workerQueue, chunks))
    calcProc = [mp.Process(target = calc, args = (workerQueue, writerQueue)) for i in range(nThreads)]
    writProc = mp.Process(target = write, args = (writerQueue, handle))


    feedProc.start()
    for p in calcProc:
        p.start()
    writProc.start()

    feedProc.join()
    for p in calcProc:
        p.join()

    writProc.join()

    feedProc.terminate()
    writProc.terminate()
    for p in calcProc:
        p.terminate()

    workerQueue.close()
    writerQueue.close()

    year_finish = time.time()

    logger.info("Year complete: {} ({})".format(year,str(datetime.timedelta(seconds=year_finish-year_start))))
