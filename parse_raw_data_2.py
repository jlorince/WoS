import sys
import os
import zipfile,io,gzip
import pandas as pd
from lxml import etree
import multiprocessing as mp
#from pathos import multiprocessing as mp
#from pathos.multiprocessing import ProcessingPool as Pool
#from pathos.multiprocessing import cpu_count
import numpy as np
import time
import glob
import datetime
import logging
from functools import partial

years = np.arange(1950,2016,1).astype(str)
basedir = 'Z:/DSSHU_ANNUAL_1950-2015/'
output_dir = 'P:/Projects/WoS/WoS/parsed/'
do_logging = False
#basedir = '/webofscience/diego/WoS_XML/xdata/data/'

allowed_filetypes = ['metadata','references','authors','subjects']
filetypes = ['metadata','references','subjects']



def reader(files):
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
                    yield chunk
                    chunk = ''

def zipreader(year):
    with zipfile.ZipFile("{}{}_DSSHU.zip".format(basedir,year), 'r') as archive:
        for name in archive.namelist():
            if name.endswith('.xml.gz'):
                bfn = archive.read(name)
                bfi = io.BytesIO(bfn)
                f = gzip.GzipFile(fileobj=bfi,mode='rb')
                chunk = ''
                for line in f:
                    line = line.strip()
                    if '<REC' in line or ('<REC' in chunk and line != '</REC>'):
                        chunk += line
                        continue
                    if line == '</REC>':
                        chunk += line
                        yield chunk
                        chunk = ''


def find_text(query):
    if query is not None:
        if query.text is not None:
            return query.text
    return ''

def process(record,handles):

    paper = etree.fromstring(record)

    uid = paper.find(".//UID").text

    if 'metadata' in handles:

        pub_info = paper.find('.//pub_info')
        basic_data = pub_info.attrib
        date = basic_data.get('sortdate','')
        pubtype = basic_data.get('pubtype','')
        volume = basic_data.get('volume','')
        issue = basic_data.get('issue','')
        pages = find_text(pub_info.find('page'))

        doctype = '|'.join([find_text(doctype) for doctype in paper.find('.//doctypes')])

        source_title = find_text(paper.find(".//title[@type='source']"))
        paper_title = find_text(paper.find(".//title[@type='item']"))


        #### NEED CONFERENCE / JOURNAL / PUBLISHER INFO


        result = '\t'.join([uid,date,pubtype,volume,issue,pages,paper_title,source_title,doctype])+'\n'
        handles['metadata'].write(result.encode('utf8'))

    if 'authors' in handles:

        for author in paper.findall('.//summary/names/name'):
            basic_data = author.attrib
            dais = basic_data.get('dais_id','')
            role = basic_data.get('role','')
            addr_no = basic_data.get('addr_no',None)

            fullname = author.find('full_name').text

        for address in paper.findall('.//addresses/address_name/address_spec'):
            pass


    if 'subjects' in handles:

        heading = find_text(paper.find('.//heading'))
        subheading = find_text(paper.find('.//subheading'))

        categories = '|'.join([cat.text for cat in paper.findall(".//subject[@ascatype='extended']")])
        handles['subjects'].write("{}\t{}\t{}\t{}\n".format(uid,heading,subheading,categories).encode('utf8'))

    if 'references' in handles:
        references = []
        no_uid = 0
        for ref in  paper.find(".//references"):
            ref_uid = ref.find('.//uid')
            if ref_uid is not None:
                references.append(ref_uid.text)
            else:
                no_uid += 1
        handles['references'].write("{}\t{}\t{}\t{}\n".format(uid,len(references),'|'.join(references),no_uid).encode('utf8'))


def go(year,fromzip = True):
    year_start = time.time()
    if fromzip:
        records = zipreader(year)
    else:
        filelist = [f for f in glob.glob(basedir+'*') if f[f.rfind('/'):][4:8]==year]
        records = reader(filelist)
    records_logged = 0
    files = ['{}{}/{}.txt.gz'.format(output_dir,kind,year) for kind in filetypes]
    handles = dict(zip(filetypes,[gzip.open(f,'wb') for f in files]))
    for record in records:
        result = process(record,handles)
        records_logged += 1
        #if records_logged % 10000 == 0:
        #    log_handler("{} --> {} records complete".format(year,records_logged))
    for handle in handles.values():
        handle.close()
    td = str(datetime.timedelta(seconds=time.time()-year_start))
    log_handler("{} --> ALL {} records logged in {}".format(year,records_logged,td))
    return records_logged

def log_handler(s):
    if do_logging:
        log_handler(s)
    else:
        print s



N = mp.cpu_count()
if __name__ == '__main__':

    overall_start = time.time()

    # if do_logging:

    #     logpath = sys.argv[1]
    #     logger = logging.getLogger('WoS processing')
    #     hdlr = logging.FileHandler(logpath)
    #     formatter = logging.Formatter('%(asctime)s %(message)s')
    #     hdlr.setFormatter(formatter)
    #     logger.addHandler(hdlr)
    #     logger.setLevel(logging.INFO)

    #     filetypes = sys.argv[2:]

    # else:

    #     filetypes = sys.argv[1:]

    # for f in filetypes:
    #     if f not in allowed_filetypes:
    #         raise("Not a valid filetype")
    #     dname = 'parsed/{}'.format(f)
    #     if not os.path.exists(dname):
    #         os.mkdir(dname)

    pool = mp.Pool(N)
    #func_partial = partial(go,filetypes=filetypes,fromzip=True)
    record_count = pool.map(go,years)
    pool.close()
    td = str(datetime.timedelta(seconds=time.time()-overall_start))
    log_handler("Parsing complete: {} total records processed in {}".format(sum(record_count),td))


