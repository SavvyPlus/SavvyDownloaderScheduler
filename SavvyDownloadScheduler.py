# -*- coding: utf-8 -*-
"""


@author: oz
"""

import ConfigParser
import datetime
import fnmatch
import json
import logging
import logging.config
import os
import os.path
import time
import urllib
import urlparse
from logging import Logger

import pyodbc
from bs4 import BeautifulSoup
from apscheduler.scheduler import Scheduler


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
logger = logging.getLogger(__name__)  # type: Logger
setup_logging()
logger.info("NEMWeb Downloader starting")

# Configuration
logger.info("Loading configuration files")
config = ConfigParser.RawConfigParser()
config.read('SavvyDownloadScheduler.cfg')
db_connection_string = config.get('Database Connection','odbcconnectionstring')



def download_ftp(f_id,url,dest_folder,filename_pattern,run_time):

    # to do: add logging
    logger.debug("Downloading from ftp location %s", url)
    # database connection
    conn = pyodbc.connect(db_connection_string)
    curs = conn.cursor()

    file_name = filename_pattern

    logger.debug("About to download %s", url)
    dest_url = os.path.join(dest_folder.strip(), file_name)

    try:
        urllib.urlretrieve(url,dest_url)
        curs.execute("insert into nemwebfiles(folder_id, file_name, source_url) values (?, ?, ?)", f_id, file_name, url)
        conn.commit()
    except IOError:
        logger.error('IO Error retrieving file',exc_info=1)
        urllib.urlcleanup()

    curs.close()
    conn.close()


def download_ftp_files(f_id,url,dest_folder,filename_pattern,run_time):

    logger.debug("Getting file list from ftp location %s", url)
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

# db_connection_string = 'Driver={SQL Server};Server=aws2-svr-db-002;Database=InvoiceLoader;Trusted_Connection=yes;'
def download(jobdetails, run_time=None):
    # unpack job details tuple
    jobid = jobdetails[0]
    url = jobdetails[1]
    dest_folder = jobdetails[5]
    retrieval_type = jobdetails[6]
    filename_pattern = jobdetails[7]
    utc_offset_hours = jobdetails[8]


    # determine run date/time
    if run_time is None:
        run_time = datetime.datetime.utcnow()+datetime.timedelta(hours=utc_offset_hours)

    # format the url, dest_folder, filename_pattern to include date fields
    url = url.format(year=run_time.strftime('%Y'), month=run_time.strftime('%m'), day=run_time.strftime('%d'), hour=run_time.strftime('%H'), minute=run_time.strftime('%M'))
    dest_folder = dest_folder.format(year=run_time.strftime('%Y'), month=run_time.strftime('%m'), day=run_time.strftime('%d'), hour=run_time.strftime('%H'), minute=run_time.strftime('%M'))
    filename_pattern = filename_pattern.format(year=run_time.strftime('%Y'), month=run_time.strftime('%m'), day=run_time.strftime('%d'), hour=run_time.strftime('%H'), minute=run_time.strftime('%M'))

    logger.info("[JobID=%d]. Commencing %s download from location %s", jobid, retrieval_type, url)

    if retrieval_type == 'LINKS':
        download_links(jobid, url, dest_folder, filename_pattern, run_time)
    elif retrieval_type == 'LINKS_OVERWRITE':
        download_links(jobid, url, dest_folder, filename_pattern, run_time, redownload=True)
    elif retrieval_type == 'FTP_FILES':
        download_ftp_files(jobid, url, dest_folder, filename_pattern, run_time)
    elif retrieval_type == 'DIRECT':
        download_url(jobid, url, dest_folder, filename_pattern, run_time)
    elif retrieval_type == 'DIRECT_FTP':
        download_ftp(jobid, url, dest_folder, filename_pattern, run_time)
    else:
        logger.error('[JobID=%d]. Bad retrieval_type encountered: %s. URL %s download not started', jobid, retrieval_type, url)

def run_as_at(jobid, datelist):
    # example:  run_as_at(64, [datetime(2009,1,1)])
    # example2: run_as_at(64, pd.date_range(start = datetime.datetime(2009,1,1), periods=60, freq='m'))
    conn = pyodbc.connect(db_connection_string)
    curs = conn.cursor()

    curs.execute("""
    SELECT ID,URL,interval_minutes,start_time,active_flag,destination_folder,retrieval_type,filename_pattern,utc_offset_hours      
    FROM [MarketData].[dbo].[NEMWebFolders]
    WHERE ID = ?
    """, jobid)

    jobdetails = curs.fetchall()[0]
    curs.close()
    conn.close()


    for dttm in datelist:
        download(jobdetails, dttm)


def main():

    # Start the scheduler
    logger.info("Starting the job scheduler")
    sched = Scheduler()
    sched.start()
    logger.info("Job scheduler started")

    # Database connection
    logger.info("Establishing database connection with connection string %s", db_connection_string)
    cnxn = pyodbc.connect(db_connection_string)
    cursor = cnxn.cursor()

    # Fetch scheduled jobs
    logger.info("Fetching folders and scheduling configurations")
    cursor.execute("""
    SELECT ID,URL,interval_minutes,start_time,active_flag,destination_folder,retrieval_type,filename_pattern,utc_offset_hours      
    FROM [MarketData].[dbo].[NEMWebFolders]
    WHERE active_flag = 1
    """)


    # Schedule jobs
    jobs = []
    for nwf in cursor.fetchall():
  # nemwebdownload(nwf[0], nwf[1], nwf[5])
        logger.info("[JobID=%d] Scheduling download of URL (%s): %s", nwf[0], nwf[6], nwf[1])
        job = sched.add_interval_job(download, minutes=nwf[2],
                                     start_date=datetime.datetime.strftime("%Y-%m-%d")+' '+nwf[ 3 ].strftime('%H:%M:%S'),
                                     max_instances=1, coalesce=1, args=[ nwf ])


        jobs.append(job)

     # Close database connections
        logger.info("Closing database connections")
        cursor.close()
        cnxn.close()

     # Wait for user keyboard interrupt <Crtl-C>
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
