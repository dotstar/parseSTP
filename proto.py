#!/usr/bin/env python

import multiprocessing as mp
import logging
from threading import Timer
import time
import sys
import re
import os

def mkoutdir(datadir,list):
    # pick a name for the output, and create the directory
    print "max list " ,max(list)
    ndir = int(max(list))+1
    outdir = datadir+"/output"+str(ndir)
    return outdir

def ccat(f,srcdir,tgtdir):
    # Copy file to the tgtdirectory
    # If it already exists in the target, skip the headers and concatenate
    src = srcdir + "/" + str(f)
    tgt = tgtdir + "/" + str(f)
    rc = True


    if (os.path.isfile(tgt)):
        logging.debug('skipping to end of file {}'.format(tgt))
        t = open(tgt,"r+")
        t.seek(0,2)   # Seek to end of the output file
        skip = 1      # Skip a line of the input file
    else:
        # Create a new file
        logging.debug('creating output file {}'.format(tgt))
        t = open(tgt,"w")
        skip = 0

    with open(src,"r") as s:
        for line in s:
            if skip == 0:
                t.write(line)
            else:
                # The first time through, is skip > 0
                # do not output the line.
                skip -= 1
    s.close()
    t.close()
    return rc

def copyfiles(datadir):
    dirs = sorted(os.listdir(datadir))
    dlist = []   # Directories to consolidate
    for dir in dirs:
        if re.match("[0-9]{1,4}$",dir) and os.path.isdir("data"+"/"+dir):
            # Any directory which is all numbers is one we want to process
            dlist.extend(dir)
            # Build a list of files in the first directory
            # and Join it with the files from the other directories
    print "input list: {}".format(dlist)

    filelist = sorted(os.listdir(datadir+"/"+str(min(dlist))))

    outdir = mkoutdir(datadir,dlist)
    print "creating outdir {}".format(outdir)
    if  os.path.exists(outdir):
        logging.error("output directory already exists {}.That is not expected".format(outdir))
    else:
        os.makedirs(outdir)

    for dir in dlist:
        for f in filelist:
            fqn = datadir+'/'+dir+"/"+str(f)
            if os.path.isfile(fqn):
                ccat(f,(datadir+"/"+dir),outdir)
            else:
                logging.error("input file is missing {}".format(fqn))

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    datadir = "./data"
    copyfiles(datadir)

