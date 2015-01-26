#!/usr/bin/env python

import multiprocessing as mp
import logging
from threading import Timer
import time
import sys
import os





def f(name,sleeptime):
    pid = os.getpid()
    logging.info("PID: {}".format(pid))
    logging.info("running f")
    logging.info ('hello {}'.format(name))
    logging.info ('sleeping {} seconds'.format(sleeptime))
    time.sleep(sleeptime)
    logging.info ('goodbye {}'.format(name))
    return sleeptime

def fCompletecb(result):
    # This is a callback, which is invoked when a spawned process completes.
    global numprocs
    numprocs -= 1
    logging.info ("fCompletecb called")
    print result

def Timeout():
    logging.info('timer expired, shutting down.')
    global numprocs
    numprocs = 0
    time.sleep(10)
    sys.exit()



# Main

if __name__ == "__main__":

    # Globals
    numprocs = 0


    logging.basicConfig(level=logging.DEBUG)

    # Put the names on the queue
    names = ('None', 'Bob', 'Ted', 'Carol', 'Alice', 'Fred', 'Wilma', 'Barney', 'Mrs Barney', 'Hawkeye', 'Trapper', 'BJ')

    jobs = mp.cpu_count()
    results = []
    mp.log_to_stderr(logging.DEBUG)
    logging.info("starting {} processes in a pool".format(jobs))
    pool = mp.Pool(processes=jobs)
    for i in range(len(names)):
        print 'submitting job:',i
        r = pool.apply_async(f,args=(names[i],i*10), callback = fCompletecb)
        numprocs += 1

    # Wait for maxtime or for all processes to complete.
    # which ever comes first.
    maxtime = 10
    t = Timer(maxtime,Timeout)
    t.start()
    while (numprocs > 0):
        print 'numprocs =', numprocs
        time.sleep(2)

    print "complete"