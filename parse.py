#!/usr/bin/env python
#
# Parse EMC Symmetrix STP File
# which reflects performance data for Symmetrix/VMAX
# Data is ASCII or Compressed ASCII
# File has sections which begine with ^<word:  and end with ^<END>
# top of file describes the device
# <VERSION> <Symmetrix> <TIMEZONE> <TIME INTERVAL> <CONFIGURATION>
# Then a series of sections describing the tables
# <METRIC: ____>
# Then a bunch of tables, seperated by timestamps
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
from glob import glob
import io
import re
import time
import sys




def getTable(stpfile, startRE, stopRE, dieRE):
    # Find the top of the table
    # returns a the lines in the STP file between startRE and endRE
    # returns "None" on EOF
    # print 'getTable( ',startRE,stopRE,dieRE,')'
    (patternfound,rc) = (skipTo(stpfile, startRE,dieRE))
    if rc:
        returnbuffer = io.StringIO()
        rc = False
        if patternfound != None:
            # Process to the bottom of the table ...
            for buffer in stpfile:

                if stopRE.match(buffer):
                    # print 'found the terminating regExp'
                    rc = True
                    break
                elif dieRE.match(buffer):
                        print 'found beginning of data section ... die'
                        rc = False
                        break
                elif buffer is None:
                    returnbuffer = None
                else:
                    returnbuffer.write(unicode(buffer))
        else:
            returnbuffer = None
            patternfound = None
            rc = False
    else:
        patternfound = None
        returnbuffer = None
        rc = False
    # print(returnbuffer)
    return (patternfound,returnbuffer,rc)


def skipTo(stpfile, RE, dieRE) :
    # Look forward into the file
    # searching for RE or dieRE
    # return rc = True if matching RE
    # return rc = False if matching dieRE

    firstline = re.compile(RE)
    patternfound = False
    stpline = ''

    for stpline in stpfile:
        if RE.match(stpline):
            patternfound = True
            break
        elif dieRE.match(stpline):
            patternFound = False
            break

    if patternfound:
        return (stpline,True)
    else:
        return (None,False)


def tsDecode(ts):
    ## Input - a time stamp from STP formated like: <TIMESTAMP: 20141107, 080003>
    ## Returns - ctime long integer

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
    return (retval)


def OpenFile(filename, directoryname, headers):
    # Convenience routine
    # Check to see if the file exists.  If not, create it and place headers
    #  else open for append
    fullyQualifiedName = directoryname + '/' + filename

    if not os.path.isfile(fullyQualifiedName):
        # Here when the file doesn't already exist
        f = open(fullyQualifiedName, 'a')
        f.write(headers)
    else:
        f = open(fullyQualifiedName, 'a')
    return (f)

def isRate(table,i):
    # Given a table name and column
    # Returns True if this is a rate column
    rc = True
    k = table+'_'+str(i)
    rc = rateTable[k]
    return rc

def CloseFile(f):
    f.close()
###
### MAIN
###

# Global table which tracks which variables are rates and need additional processing
# True if the column is a Rate
# Key is tableName_Column
rateTable = dict()
if __name__ == "__main__":
    tables = ("System", "Devices", "Devices TP Pool 1", "Devices TP Pool 2", "Devices TP Pool 3", "Devices TP Pool 4",
              "Devices TP Pool 5", "Devices TP Pool 6", "Directors FE", "Directors BE", "Directors RDF", "Ports FE",
              "Disks", "External Disks", "RDFAStats", "Rdf-System", "Rdf-Director", "Rdf-Device", "Rdf-Group",
              "Thin Pool Info", "Interconnect")

    headers = {}
    for infile in glob('T1*'):
        print 'file is:',infile
        m = re.search('.*.gz$', infile)
        if (m):
            compressed = True
            f = gzip.open(infile, 'r')
        else:
            compressed = False
            f = open(infile, "r")

        # First sections of the file contain the description of the columns.
        # Each table name has a metric section
        # Read these and build the headers for later.

        # Is there a clean way to do this without reading the whole file ?
        # Does it matter if the net effect is that the file will be in buffer cache for the 2nd pass ?
        print "Processing Metrics"
        while True:
            match = '^<METRIC: '
            startRE = re.compile('^<METRIC: ')
            stopRE = re.compile("^<END>")
            lastMetricRE = re.compile("^<TIMESTAMP: ")       # If we see a timestamp there are no more Metrics
            (firstline,tableText,rc) = getTable(f, startRE, stopRE, lastMetricRE)
            # if tableText is None or rc == False:
            if tableText is None:
                break                           # We either ran out of Metrics in the file or didn't see an END.
            else:
                tableName = firstline[9:-3]
                # For unknown reasons, 4 variables are named RDF-* in the metric section and Rdf-* in the Data section
                # Patch around this for now.
                if tableName[0:4] == 'RDF-':
                    tableName = re.sub("RDF-","Rdf-",tableName)
                print "Metric Name: " + tableName
                mybuffer = tableText.getvalue().split("\n")
                columns = []
                colNum = 0
                for line in mybuffer:
                    # For each column there is a CSV with a name and some attributes.
                    # Similar to:
                    #    symid,long,Key,ArchiveLast
                    #    ios per sec,longlong,ConvertToRate,ArchiveStats,sortDescending
                    #    reads per sec,longlong,ConvertToRate,ArchiveStats

                    rkey = tableName+'_'+str(colNum)
                    colNum += 1
                    if line != '':
                        linevalues = line.split(',')
                        columnName = linevalues[0]
                        columnType = linevalues[1]
                        columnaction = linevalues[2]
                        if columnaction == "Derived":
                            rateTable[rkey] = False
                        elif columnaction == "ConvertToRate":
                            rateTable[rkey] = True
                        else:
                            rateTable[rkey] = False
                        columns.insert(-0,columnName)
                headers[tableName]=columns
                # print headers
        # Now that we've determined which columns are in each table, re-open the file to process the data
        print "Rewinding file to process data elements."
        f.close()
        if compressed:
            f = gzip.open(infile, 'r')
        else:
            f = open(infile, "r")
        lastrow = dict()                    # Used to calculate rates.
        firsttimestamp = dict()             # Can't calculate a rate on first time stamp ... (need two points)
        for table in tables:
            firsttimestamp[table]=True      # Mark each table as fresh / never seen
        directory = os.getcwd()+'/'+'data'  # Output directory, mkdir if needed.
        if not os.path.exists(directory):
            os.makedirs(directory)          # Here to walk through the rest of the file and collect the data into tables ...
        while True:
            ### Get all of the lines starting at the TIMESTAMP ...
            (ts,rc) = skipTo(f, re.compile("^<TIMESTAMP:.*>"),re.compile('xyzzy'))
            if not ts:  ## When we're out of TIMESTAMPS, we are out of data.  File is complete.
                break
            else:
                t = str(tsDecode(ts))
                print "time is: ", t
                # sys.stdout.write(".")  # give the user a sense that we are working ...
                # sys.stdout.flush()
                # tables = ("Devices",)
                for table in tables:
                    header = 'TimeStamp'
                    for h in (headers[table]):
                        header += ',' + h[0]  # Prepare a string for output
                    header += '\n'
                    outfile = OpenFile(table, directory, header)
                    startRE = re.compile("^<DATA: " + table + ".*>")
                    stopRE = re.compile("^<END>")
                    dieRE = re.compile('xyzzy')
                    (firstline,tableText,rc) = getTable(f, startRE, stopRE, dieRE)
                    mybuffer = tableText.getvalue().split("\n")
                    stop = re.compile(stopRE)
                    empty = re.compile("^$")
                    for line in mybuffer:
                        values = line.split(",")
                        key = table+'_'+values[0]
                        # The last line will match the Regular Expression stopRE ...
                        # Add time stamp to each line.  Drop the last line.
                        if not stop.match(line) and not empty.match(line):
                            if firsttimestamp[table]:
                                # store all of the columns so we can process rates on the next pass
                                # print 'saving '+key+' for next pass '
                                lastrow[key] = values  # Keep a copy for next time
                            else:
                                # Adjust for Rates
                                i = 0
                                for oldvalue in (lastrow[key]):
                                    if isRate(table,i):
                                        delta = int(values[i]) - int(oldvalue)
                                        # print oldvalue,values[i],delta
                                        values[i] = delta
                                    i += 1
                                # Output the record
                                pbuf = t
                                for v in values:
                                    pbuf = pbuf+','+str(v)
                                # pbuf += '\n'
                                outfile.write(pbuf)
                                print pbuf
                                # Update the lastrow record, for next time ...
                                tmpbuf = []
                                for v in values:
                                    tmpbuf.insert(-0,v)
                                lastrow[key] = tmpbuf
                        else:
                            firsttimestamp[table] = False

                    CloseFile(outfile)
