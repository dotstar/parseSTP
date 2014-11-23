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
import io
import re
import time


def getTable(stpfile, startRE, endRE):
    # Find the top of the table
    # returns a the lines in the STP file between startRE and endRE
    # returns "None" on EOF

    patternFound = (skipTo(stpfile, startRE))
    returnBuffer = io.StringIO()
    if patternFound:
        # Process to the bottom of the table ...
        stop = re.compile(stopRE)
        for buffer in stpfile:
            if buffer is None:
                returnBuffer = None
            else:
                if stop.match(buffer):
                    break
                else:
                    returnBuffer.write(unicode(buffer))
    else:
        returnBuffer = None
        patternFound = None
    return (patternFound,returnBuffer)


def skipTo(stpfile, RE):
    FirstLine = re.compile(RE)
    patternFound = False
    stpline = ''

    for stpline in stpfile:
        if FirstLine.match(stpline):
            patternFound = True
            break

    if patternFound:
        return stpline
    else:
        return None


def tsDecode(ts):
    ## Input - a time stamp from STP formated like: <TIMESTAMP: 20141107, 080003>
    ## Returns - ctime long integer

    sts = ts.split()
    year = int((sts[1])[0:4])
    month = int((sts[1])[4:6])
    day = int((sts[1][6:8]))
    hour = int((sts[2][0:2]))
    minute = int((sts[2][2:4]))
    second = int((sts[2][4:6]))
    hour = hour - 1  ## Is this converting the hour correctly?  - GMT vs DST ARGH
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


def CloseFile(f):
    f.close()

if __name__ == "__main__":
    tables = ("System", "Devices", "Devices TP Pool 1", "Devices TP Pool 2", "Devices TP Pool 3", "Devices TP Pool 4",
              "Devices TP Pool 5", "Devices TP Pool 6", "Directors FE", "Directors BE", "Directors RDF", "Ports FE",
              "Disks", "External Disks", "RDFAStats", "Rdf-System", "Rdf-Director", "Rdf-Device", "Rdf-Group",
              "Thin Pool Info", "Interconnect")

    infile = 'T1_20141107_080003_000195700621.ttp'
    headers = {}
    f = open(infile, "r")

    # First sections of the file contain the description of the columns.
    # Each table name has a metric section
    # Read these and build the headers for later.

    # Is there a clean way to do this without reading the whole file ?
    # Does it matter if the net effect is that the file will be in buffer cache for the 2nd pass ?
    while True:
        match = '^<METRIC: '
        startRE = re.compile(match)
        stopRE = "^<END>"


        (firstline,tableText) = getTable(f, startRE, stopRE)
        if tableText is None:
            break
        else:
            tableName = firstline[9:-3]
            # For unknown reasons, 4 variables are named RDF-* in the metric section and Rdf-* in the Data section
            # Patch around this for now.
            if tableName[0:4] == 'RDF-':
                tableName = re.sub("RDF-","Rdf-",tableName)
            print "Metric Name: " + tableName
            buffer = tableText.getvalue().split("\n")
            columns = []
            for line in buffer:
                # For each column there is a CSV with a name and some attributes.
                # Similar to:
                #    symid,long,Key,ArchiveLast
                #    ios per sec,longlong,ConvertToRate,ArchiveStats,sortDescending
                #    reads per sec,longlong,ConvertToRate,ArchiveStats
                #
                # Some headers are for contain the tag ConcertToRate
                # Need to add some logic to do that.
                #
                # Others contain 'Derived' and a simple algebra for how to derive that field.
                # for now, skip the Derived headers
                if line != '':
                    linevalues = line.split(',')
                    columnName = linevalues[0]
                    columnType = linevalues[1]
                    columnaction = linevalues[2]
                    if columnaction != "Derived":
                        columnAttributes = linevalues[1:-0]
                        columns.append((columnName,columnAttributes))
                        # columns.insert(-0,(columnName,columnAttributes))
            headers[tableName]=columns
            # print headers


    f.close()
    # exit()
    f = open(infile, "r")


    # Here to walk through the rest of the file and collect the data into tables ...
    while True:
        ### Get all of the lines starting at the TIMESTAMP ...
        ts = skipTo(f, "^<TIMESTAMP:.*>")
        if not ts:  ## When we're out of TIMESTAMPS, we are out of data.  File is complete.
            break
        else:
            t = str(tsDecode(ts))
            print "time is: ", t


            ## tables = ("Devices",)
            for table in tables:
                directory = os.getcwd()+'/'+'data'
                header = 'TimeStamp'
                for h in (headers[table]):
                    if header == '':
                        header = h[0]
                    else:
                        header += ',' + h[0]
                header += '\n'
                outfile = OpenFile(table, directory, header)

                ## print "processing table", table
                startRE = "^<DATA: " + table + ".*>"
                stopRE = "^<END>"
                (firstline,tableText) = getTable(f, startRE, stopRE)
                buffer = tableText.getvalue().split("\n")

                stop = re.compile(stopRE)
                empty = re.compile("^$")
                for line in buffer:
                    # The last line will match the Regular Expression stopRE ...
                    # Add time stamp to each line.  Drop the last line.
                    if not stop.match(line) and not empty.match(line):
                        printme = t + "," + line + '\n'
                        outfile.write(printme)
                CloseFile(outfile)
