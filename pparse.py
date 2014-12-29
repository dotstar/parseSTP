#!/usr/bin/env python
#
# Parse EMC Symmetrix STP File
# which reflects performance data for Symmetrix/VMAX
# Data is ASCII or Compressed ASCII
# File has sections which begins with ^<word:  and end with ^<END>
# top of file describes the device
# <VERSION> <Symmetrix> <TIMEZONE> <TIME INTERVAL> <CONFIGURATION>
# Then a series of sections describing the tables
# <METRIC: ____>
# Then a bunch of tables, separated by timestamps
# <TIMESTAMP: 20141107, 080003>
# <DATA: System>
# <END>
# <DATA: Devices, 8421>
# <END>
# <DATA: Devices TP Pool 1, 5954>
# <END>
# <DATA: Devices TP Pool 2, 5954>

# Top part of file describes the device and begins

import os
import gzip
from glob import iglob
import io
import re
import numpy as np
import multiprocessing as mp
import time


def parse(ifile, outdir, sequencenum):
    print 'infile:', ifile
    print 'outfile:', outdir
    print 'sequencenum:', sequencenum
    print "PID:", os.getpid()

    def gettable(stpfile, start, stop, never):
        # Find the top of the table
        # returns the lines from the STP file between start and stop
        # returns "None" on EOF
        print 'gettable called: (', stpfile, ",", "start", ",", "stop", ",", never, ')'
        debug = 0
        if never is None:
            never = 'xyzzy123____5551212'
        startmatch = re.compile(start)
        stopmatch = re.compile(stop)
        diematch = re.compile(never)

        (patternfound, rc) = (skipto(stpfile, startmatch, diematch))
        if rc:
            returnbuffer = io.StringIO()
            rc = False
            if debug > 14:
                print 'patternfound:', patternfound
            if patternfound is not None:
                # Process to the bottom of the table ...
                # for stpbuffer in stpfile:
                while True:
                    stpbuffer = stpfile.readline()
                    if stpbuffer == '':
                        break

                    if stopmatch.match(stpbuffer):
                        # print 'found the terminating regExp'
                        rc = True
                        break
                    elif diematch.match(stpbuffer):
                        returnbuffer = stpbuffer
                        rc = False
                        break
                    elif stpbuffer is None:
                        returnbuffer = None
                    else:
                        returnbuffer.write(unicode(stpbuffer))
            else:
                returnbuffer = None
                patternfound = ''
                rc = False
        else:
            patternfound = None
            returnbuffer = None
            rc = False
        return patternfound, returnbuffer, rc

    def skipto(stpfile, firstregx, dieregx):
        print "skipto(", ",", stpfile, ",", 'firstRE', ",", dieregx, ")"
        # Look forward into the file
        # searching for RE or dieRE
        # return rc = True if matching RE
        # return rc = False if matching dieRE
        found = False
        debug = 20
        i = 0
        while not found:
            fileptr = stpfile.tell()  # Where are we before the read?
            stpline = stpfile.readline()
            if stpline == '':
                return '', False  # EOF?
            i += 1
            if firstregx.match(stpline):
                if debug > 19:
                    print 'matched firstRE', stpline  # print 'skipped',i,'lines'
                return stpline, True
            elif dieregx.match(stpline):
                if debug > 19:
                    print 'matched dieRE', stpline
                stpfile.seek(fileptr)  # roll the file back
                print 'skipped', i, 'lines'
                return stpline, False

    def tsdecode(ts):
        # Input - a time stamp from STP formated like: <TIMESTAMP: 20141107, 080003>
        # Returns - ctime long integer

        sts = ts.split()
        year = int((sts[1])[0:4])
        month = int((sts[1])[4:6])
        day = int((sts[1][6:8]))
        hour = int((sts[2][0:2])) - 1
        minute = int((sts[2][2:4]))
        second = int((sts[2][4:6]))
        t = (year, month, day, hour, minute, second, 0, 0, -1)
        # print ts, year, month, day, hour, minute, second
        retval = int(time.mktime(t))
        return retval

    def setval(trackingvar, table, i, rateflag):
        # Tag column i of table as a rate field
        # This is probably very un-pythonesque.
        # if the table hasn't been seen before, create a row
        try:
            trackingvar[table][i] = rateflag
            rc = True
        except KeyError:
            row = [None] * maxrow
            trackingvar[table] = row
            trackingvar[table][i] = rateflag
            rc = True
        return rc

    def getval(trackingvar, table, i):
        # Given a table name and column
        # Returns True if this is
        # rate column
        if not trackingvar[table][i]:
            rc = None
        else:
            rc = trackingvar[table][i]
        return rc

    def printdb(trackingvar):
        # Convenience routine for debugging
        for key in trackingvar:
            print key,
            i = 0
            while trackingvar[key][i] is not None:
                print trackingvar[key][i],
                i += 1
            print

    def processheaders(fp):
        # Is there a clean way to do this without reading the whole file ?
        # Does it matter if the net effect is that the file will be in buffer cache for the 2nd pass ?
        print "Processing Headers and Metrics"
        debug = 10
        while True:
            (firstline, tabletext, rc) = gettable(fp, '^<METRIC:', '^<END>', '^<TIMESTAMP: ')
            # if tabletext is None or rc == False:
            if tabletext is None:
                print 'out of metrics?'
                break  # We either ran out of Metrics in the file or didn't see an END.
            else:
                tablename = firstline[9:-3]
                # For unknown reasons, 4 variables are named RDF-* in the metric section and Rdf-* in the Data section
                # Patch around this for now.
                if tablename[0:4] == 'RDF-':
                    tablename = re.sub("RDF-", "Rdf-", tablename)
                if debug > 9:
                    print "Metric Name: " + tablename
                mybuffer = tabletext.getvalue().split("\n")
                columns = []
                colnum = 0
                for line in mybuffer:
                    # For each column there is a CSV with a name and some attributes.
                    # Similar to:
                    # symid,long,Key,ArchiveLast
                    # ios per sec,longlong,ConvertToRate,ArchiveStats,sortDescending
                    # reads per sec,longlong,ConvertToRate,ArchiveStats

                    if line != '':
                        linevalues = line.split(',')
                        columnname = linevalues[0]
                        columntype = linevalues[1]
                        setval(typetable, tablename, colnum, columntype)
                        columnaction = linevalues[2]
                        if columnaction == "Derived":
                            setval(ratetable, tablename, colnum, False)
                        elif columnaction == "ConvertToRate":
                            setval(ratetable, tablename, colnum, True)
                        else:
                            setval(ratetable, tablename, colnum, False)
                        setval(headertable, tablename, colnum, columnname)
                        columns.append(columnname)
                    colnum += 1
                headers[tablename] = columns
        # Build header string in global variable, one for each table.

        return headers, True

    def openoutputfile(filename, directoryname, headers):
        # Convenience routine
        # Check to see if the file exists.  If not, create it and place headers
        # else open for append
        fullyqualifiedname = directoryname + '/' + filename

        if not os.path.isfile(fullyqualifiedname):
            # Here when the file doesn't already exist
            f1 = open(fullyqualifiedname, 'a')
            f1.write(headers)
        else:
            f1 = open(fullyqualifiedname, 'a')
        return f1

    def openinputfile(name, mode):
        # print 'file is:', name
        # print 'directory is:',os.getcwd()
        m = re.search('.*.gz$', name)
        if m:
            f1 = gzip.open(name, mode)
        else:
            f1 = open(name, mode)
        return f1

    def closefile(f):
        f.close()

    # Global table which tracks which variables are rates and need additional processing
    # True if the column is a Rate
    # Key is tableName_Column
    lastt = 0
    deltat = 0
    ratetable = {}
    typetable = {}
    headertable = {}
    headers = {}

    directory = os.getcwd() + '/' + 'data' + '/' + str(sequencenum)  # Output directory, mkdir if needed.
    if not os.path.exists(directory):
        os.makedirs(directory)  # Here to walk through the rest of the file and collect the data into tables ...

    f = openinputfile(ifile, "r")
    (headers, rc) = processheaders(f)
    if not rc:
        exit('processHeaders failed')
    print "Rewinding file to process data elements."
    f.close()
    if debug > 2:
        print 'rates:'
        printdb(ratetable)
        print 'types:'
        printdb(typetable)
        print 'headers:'
        printdb(headertable)
    f = openinputfile(ifile, "r")
    linebuffer = ""
    priorrow = dict()  # Used to calculate rates.
    firstsample = dict()  # Can't calculate a rate on first time stamp ... (need two points)
    while True:
        # The first time in we need to find the 1st TIMESTAMP
        # After that we may already have a TIMESTAMP in linebuffer
        if linebuffer and re.match("^<TIMESTAMP:", linebuffer):
            ts = linebuffer
        else:
            (ts, rc) = skipto(f, re.compile("^<TIMESTAMP:.*>"), re.compile('xyzzy'))
        if not ts:  # When we're out of TIMESTAMPS, we are out of data.  File is complete.
            break
        else:
            t = str(tsdecode(ts))
            print "time is: ", t, ts
            if lastt > 0:
                deltat = long(t) - long(lastt)
                if deltat < 0:
                    deltat = - deltat
                print 'elapsed time is', deltat, 'seconds'
            while True:  # collect tables until we see another timestamp
                (linebuffer, tabletext, rc) = gettable(f, "^<DATA:", "^<END", "^<TIMESTAMP: ")
                if not rc:
                    break  # end while
                # Samples of what the linebuffer looks like:  need to parse out the variable name
                # <DATA: System>
                # <DATA: Devices, 11059>
                # <DATA: Devices TP Pool 1, 7606>
                table = linebuffer
                table = re.sub('^<DATA: ', '', table)
                table = re.sub('>.*', '', table)
                table = re.sub(',.*', '', table)
                table = table.rstrip()
                if debug > 3:
                    print 'processing table:', table
                header = 'TimeStamp'
                i = 0
                while True:
                    h = getval(headertable, table, i)
                    if h is None:
                        break
                    header += ',' + h
                    i += 1
                header += '\n'
                outfile = openoutputfile(table, directory, header)
                mybuffer = tabletext.getvalue().split("\n")  # Process one line at a time.
                stopregx = re.compile('^<END')
                emptyre = re.compile("^$")
                for line in mybuffer:
                    line = line.rstrip()  # remove CR NL and trailing blanks
                    line = re.sub(r'\s+$', '', line)
                    line = re.sub(r',$', '', line)
                    values = line.split(",")  # The values in the record (before rate adjustment)
                    pvalues = []  # The values to be output (after rate adjustment)
                    key = table + '_' + values[0]
                    # The last line will match the Regular Expression stopRE ...
                    # Add time stamp to each line.  Drop the last line.
                    if not stopregx.match(line) and not emptyre.match(line):
                        if firstsample.get(table) is None:
                            # store all of the columns so we can process rates on the next pass
                            # print 'saving '+key+' for next pass '
                            if debug > 5:
                                print 'first time for table:', table, 'key instance:', values[0]
                            priorrow[key] = [None] * maxrow
                            j = 0
                            for v in values:
                                priorrow[key][j] = v
                                j += 1
                        else:
                            i = 0
                            for val in values:
                                newvalue = val
                                oldvalue = priorrow[key][i]
                                if (oldvalue != '') and (oldvalue != " "):
                                    if getval(ratetable, table, i):
                                        vtype = getval(typetable, table, i)
                                        if vtype != 'float':
                                            try:
                                                if False:
                                                    nv = newvalue
                                                    nv = nv.astype(np.uint64)
                                                    ov = oldvalue
                                                    ov = ov.astype(np.uint64)
                                                    dv = deltat
                                                    dv = dv.astype(np.uint64)
                                                    delta = (nv - ov) / dv
                                                else:
                                                    delta = (np.uint64(newvalue) - np.uint64(oldvalue)) / np.uint64(deltat)
                                            except RuntimeWarning:
                                                print 'RuntimeWarning'
                                                print 'newvalue:', newvalue
                                                print 'oldvalue:', oldvalue
                                                print 'deltat:', deltat
                                                delta = 0  # xxx - is this the right thing to do on overflow ?
                                        else:
                                            delta = (long(float(newvalue) - float(oldvalue))) / float(deltat)

                                        pvalues.append(delta)
                                    else:
                                        pvalues.append(newvalue)
                                i += 1
                            pbuf = t
                            for v in pvalues:
                                pbuf = pbuf + ',' + str(v)
                            pbuf += '\n'
                            outfile.write(pbuf)  # output the record
                            if debug > 9:
                                print pbuf
                            # Update the priorrow record, for next time ...
                            priorrow[key] = [None] * maxrow
                            j = 0
                            for v in values:
                                priorrow[key][j] = v
                                j += 1
                    else:
                        firstsample[table] = False
                        lastt = t

                closefile(outfile)
    closefile(f)

#
# MAIN
#
if __name__ == "__main__":
    debug = 0
    maxrow = 150
    # Build a sorted list of filenames
    inputDirectory = './'
    inputFiles = inputDirectory + 'T1*'
    ifile = iglob(inputFiles)
    results = []
    pool = mp.Pool(processes=4)
    seq = os.getpid()
    for infile in sorted(ifile):
        r = pool.apply_async(parse, args=(infile, '.', seq))
        results.append(r)
        seq += 1
        time.sleep(7)

    print "complete"