# -*- coding: utf-8 -*-
"""
Created on Mon Jun 02 16:33:35 2014

@author: andrew
"""

import urllib
import urlparse
import time, datetime
import pyodbc
import os.path
from bs4 import BeautifulSoup
from apscheduler.scheduler import Scheduler
import ConfigParser
import os
import json
import logging
import logging.config
import fnmatch




def setup_logging(
    default_path='logging_config.json', 
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:            
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

# Initialise logging
logger = logging.getLogger(__name__)
setup_logging()
logger.info("NEMWeb Downloader starting")

# Configuration
logger.info("Loading configuration files")
config = ConfigParser.RawConfigParser()
config.read('nemweb.cfg')
db_connection_string = config.get('Database Connection','odbcconnectionstring')

# Start the scheduler
logger.info("Starting the job scheduler")
sched = Scheduler()
sched.start()
logger.info("Job scheduler started")

 
def nemwebdownload(f_id,url,dest_folder,filename_pattern):
    
    # to do: add logging  
    logger.debug("Getting file list from %s", url)
    # database connection
    conn = pyodbc.connect(db_connection_string)
    curs = conn.cursor()
    
    # read url and determine files to download
    try:
        u = urllib.urlopen(url)
        soup = BeautifulSoup(u.read())
    except IOError:
        logger.error('IO Error downloading file list %s', url, exc_info=1)
        
    for link in soup.find_all('a'):      # first link is to parent directory
        file_url = urlparse.urljoin(url, link.get('href'))
        file_name = os.path.split(file_url)[1]
        if fnmatch.fnmatch(file_name,filename_pattern):
            
            # check if file has already been downloaded
            rec_count = curs.execute("select count(id) from nemwebfiles where file_name = ?", file_name).fetchone()[0]        
            if rec_count==0:
                logger.debug("About to download %s", file_url)
                dest_url = os.path.join(dest_folder.strip(), file_name)
                try:
                    urllib.urlretrieve(file_url,dest_url)
                    curs.execute("insert into nemwebfiles(folder_id, file_name, source_url) values (?, ?, ?)", f_id, file_name, file_url)
                    conn.commit()                
                except IOError:
                    logger.error('IO Error retrieving file',exc_info=1)
    
    curs.close()
    conn.close()


    
def urldownload(f_id,url,dest_folder,filename_pattern):
    
    # to do: add logging  
    logger.debug("Getting file list from ftp location %s", url)
    # database connection
    conn = pyodbc.connect(db_connection_string)
    curs = conn.cursor()
    
    # tokens for use in naming files - base on current time (AEST) 
    timeAEST = datetime.datetime.utcnow()+datetime.timedelta(hours=10)    
        
    file_name = filename_pattern.format(year=timeAEST.strftime('%Y'), month=timeAEST.strftime('%m'), day=timeAEST.strftime('%d'), hour=timeAEST.strftime('%H'), minute=timeAEST.strftime('%M'))
        
    logger.debug("About to download %s", url)
    dest_url = os.path.join(dest_folder.strip(), file_name)
    try:
        urllib.urlretrieve(url,dest_url)
        curs.execute("insert into nemwebfiles(folder_id, file_name, source_url) values (?, ?, ?)", f_id, file_name, url)
        conn.commit()                
    except IOError:
        logger.error('IO Error retrieving file',exc_info=1)
    
    curs.close()
    conn.close()    
    
    
    
    
def bomdownload(f_id,url,dest_folder,filename_pattern):
        
    logger.debug("Downloading from location %s", url)
    # database connection
    conn = pyodbc.connect(db_connection_string)
    curs = conn.cursor()
    
    # read url and determine files to download
    try:
        u = urllib.urlopen(url)
        s = u.read()
        l = s.split('\r\n')[0:-1]
        fnames = map(lambda x: x.split()[-1], l)
        fnames = fnmatch.filter(fnames, filename_pattern)
    except IOError:
        logger.error('Error downloading FTP file list %s', url, exc_info=1)
    
            
    for fname in fnames:
        file_url = urlparse.urljoin(url, fname)
        file_name = os.path.split(file_url)[1]
        
        # check if file has already been downloaded
        rec_count = curs.execute("select count(id) from nemwebfiles where file_name = ?", file_name).fetchone()[0]        
        if rec_count==0:
            logger.debug("About to download %s", file_url)
            dest_url = os.path.join(dest_folder.strip(), file_name)
            try:
                urllib.urlretrieve(file_url,dest_url)
                curs.execute("insert into nemwebfiles(folder_id, file_name, source_url) values (?, ?, ?)", f_id, file_name, file_url)
                conn.commit()                
            except IOError:
                logger.error('IO Error retrieving file',exc_info=1)
    
    curs.close()
    conn.close()    
    
# db_connection_string = 'Driver={SQL Server Native Client 11.0};Server=MEL-LT-001\SQLEXPRESS;Database=MarketData;Trusted_Connection=yes;'


def main():
    
    logger.info("Establishing database connection with connection string %s", db_connection_string)
    cnxn = pyodbc.connect(db_connection_string)
    cursor = cnxn.cursor()
    
    logger.info("Fetching folders and scheduling configurations")
    cursor.execute("""
    SELECT ID,URL,interval_minutes,start_time,active_flag,destination_folder,retrieval_type,filename_pattern      
    FROM [MarketData].[dbo].[NEMWebFolders]
    WHERE active_flag = 1
    """)
    
    jobs = []
    for nwf in cursor.fetchall():
        # nemwebdownload(nwf[0], nwf[1], nwf[5])
        logger.info("Scheduling download of URL (%s): %s", nwf[6], nwf[1])
        if nwf[6] == 'LINKS':
            job = sched.add_interval_job(nemwebdownload, minutes=nwf[2], start_date=time.strftime("%Y-%m-%d")+' '+nwf[3].strftime('%H:%M:%S'), max_instances=1, coalesce=1, args=[nwf[0], nwf[1],nwf[5],nwf[7]])
        elif nwf[6] == 'FTP_FILES':
            job = sched.add_interval_job(bomdownload, minutes=nwf[2], start_date=time.strftime("%Y-%m-%d")+' '+nwf[3].strftime('%H:%M:%S'), max_instances=1, coalesce=1, args=[nwf[0], nwf[1],nwf[5],nwf[7]])
        elif nwf[6] == 'DIRECT':
            job = sched.add_interval_job(urldownload, minutes=nwf[2], start_date=time.strftime("%Y-%m-%d")+' '+nwf[3].strftime('%H:%M:%S'), max_instances=1, coalesce=1, args=[nwf[0], nwf[1],nwf[5],nwf[7]])
        else:
            logger.error('Bad retrieval_type encountered: %s. URL %s download not scheduled', nwf[6], nwf[1])
        jobs.append(job)

    logger.info("Closing database connections")
    cursor.close()
    cnxn.close()
    
    logger.info("Loop until user terminates the application")
    try:
        while True:
            time.sleep(1)
    except:
        logger.info("Application terminated. Scheduler shutting down")
        sched.shutdown()
        logger.info("Exiting.")
        

# Schedule nemwebdownload to be called at regular intervals
#sched.add_interval_job(job_function, minutes=nwf[2], start_date=time.strftime("%Y-%m-%d")+nwf[2].strftime('%H:%M:%S'), max_instances=1, coalesce=1, args=['Hello world'])
#sched.add_interval_job(job_function, seconds=60, start_date=time.strftime("%Y-%m-%d")+' 00:00:02', max_instances=1, coalesce=1, args=['Hello 2'])

if __name__ == "__main__":
    main()