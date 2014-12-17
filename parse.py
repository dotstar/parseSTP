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
from glob import glob
import io
import re
import time




def getTable(stpfile, start, stop, never):
    # Find the top of the table
    # returns a the lines in the STP file between start and stop
    # returns "None" on EOF
    startRE = re.compile(start)
    stopRE = re.compile(stop)
    if never is None:
        dieRE = re.compile('xyzzy123___!')   # Easier to search for the improbably than change the code right now.
    else:
        dieRE = re.compile(never)

    (patternfound,rc) = (skipTo(stpfile, startRE,dieRE))
    if rc:
        returnbuffer = io.StringIO()
        rc = False
        if patternfound is not None:
            # Process to the bottom of the table ...
            for stpbuffer in stpfile:

                if stopRE.match(stpbuffer):
                    # print 'found the terminating regExp'
                    rc = True
                    break
                elif dieRE.match(stpbuffer):
                        print 'found beginning of data section ... die'
                        rc = False
                        break
                elif stpbuffer is None:
                    returnbuffer = None
                else:
                    returnbuffer.write(unicode(stpbuffer))
        else:
            returnbuffer = None
            patternfound = None
            rc = False
    else:
        patternfound = None
        returnbuffer = None
        rc = False
    # print(returnbuffer)
    return patternfound,returnbuffer,rc


def skipTo(stpfile, firstRE, dieRE) :
    # Look forward into the file
    # searching for RE or dieRE
    # return rc = True if matching RE
    # return rc = False if matching dieRE

    patternfound = False
    stpline = ''

    for stpline in stpfile:
        if firstRE.match(stpline):
            patternfound = True
            break
        elif dieRE.match(stpline):
            patternfound = False
            break

    if patternfound:
        return stpline,True
    else:
        return None,False


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
    return retval


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
    return f

def setVal(trackingvar,table,i,rateFlag):
    # Tag column i of table as a rate field
    # This is probably very un-pythonesque.
    # if the table hasn't been seen before, create a row
    maxrow = 150
    try:
        trackingvar[table][i] = rateFlag
        rc = True
    except KeyError:
        row = [None]*maxrow
        trackingvar[table] = row
        trackingvar[table][i] = rateFlag
        rc = True
    return rc

def getVal(trackingvar,table,i):
    # Given a table name and column
    # Returns True if this is
    #  rate column
    if not trackingvar[table][i]:
        rc = None
    else:
        rc = trackingvar[table][i]
    return rc

def printDB(trackingvar):
    for key in trackingvar:
        print key,
        i = 0
        while trackingvar[key][i] is not None:
            print trackingvar[key][i],
            i+= 1
        print

def CloseFile(f):
    f.close()

def processHeaders(fp):
    # Is there a clean way to do this without reading the whole file ?
    # Does it matter if the net effect is that the file will be in buffer cache for the 2nd pass ?
    print "Processing Headers and Metrics"
    while True:
        (firstline,tableText,rc) = getTable(fp, '^<METRIC:', '^<END>', '^<TIMESTAMP: ')
        # if tableText is None or rc == False:
        if tableText is None:
            break                           # We either ran out of Metrics in the file or didn't see an END.
        else:
            tableName = firstline[9:-3]
            # For unknown reasons, 4 variables are named RDF-* in the metric section and Rdf-* in the Data section
            # Patch around this for now.
            if tableName[0:4] == 'RDF-':
                tableName = re.sub("RDF-","Rdf-",tableName)
            if debug > 9:
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

                if line != '':
                    linevalues = line.split(',')
                    columnName = linevalues[0]
                    columnType = linevalues[1]
                    setVal(typeTable,tableName,colNum,columnType)
                    columnaction = linevalues[2]
                    if columnaction == "Derived":
                        setVal(rateTable,tableName,colNum,False)
                    elif columnaction == "ConvertToRate":
                        setVal(rateTable,tableName,colNum,True)
                    else:
                        setVal(rateTable,tableName,colNum,False)
                    setVal(headerTable,tableName,colNum,columnName)
                    columns.append(columnName)
                colNum += 1
            headers[tableName]=columns
    # Build header string in global variable, one for each table.


    return headers,True

def myopen(name,mode):
    print 'file is:',name
    m = re.search('.*.gz$', infile)
    if m:
        f = gzip.open(name, mode)
    else:
        f = open(name,mode)
    return f



###
### MAIN
###

# Global table which tracks which variables are rates and need additional processing
# True if the column is a Rate
# Key is tableName_Column
rateTable = {}
typeTable={}
headerTable = {}
headers = {}
debug = 5
directory = os.getcwd()+'/'+'data'  # Output directory, mkdir if needed.
if not os.path.exists(directory):
    os.makedirs(directory)          # Here to walk through the rest of the file and collect the data into tables ...

if __name__ == "__main__":
    tables = ("System", "Devices", "Devices TP Pool 1", "Devices TP Pool 2", "Devices TP Pool 3", "Devices TP Pool 4",
              "Devices TP Pool 5", "Devices TP Pool 6", "Directors FE", "Directors BE", "Directors RDF", "Ports FE",
              "Disks", "External Disks", "RDFAStats", "Rdf-System", "Rdf-Director", "Rdf-Device", "Rdf-Group",
              "Thin Pool Info", "Interconnect")
    # tables = ("Devices TP Pool 1","Rdf-Group")

    for infile in glob('T1*'):
        f = myopen(infile,"r")
        (headers,rc) = processHeaders(f)
        if not rc:
            exit('processHeaders failed')
        print "Rewinding file to process data elements."
        f.close()
        if debug > 29:
            print 'rates:'
            printDB(rateTable)
            print 'types:'
            printDB(typeTable)
            print 'headers:'
            printDB(headerTable)
        f = myopen(infile,"r")
        priorrow = dict()                   # Used to calculate rates.
        firsttimestamp = dict()             # Can't calculate a rate on first time stamp ... (need two points)
        for table in tables:
            firsttimestamp[table]=True      # Mark each table as fresh / never seen
        while True:
            ### Get all of the lines starting at the TIMESTAMP45 ...
            (ts,rc) = skipTo(f, re.compile("^<TIMESTAMP:.*>"),re.compile('xyzzy'))
            if not ts:  ## When we're out of TIMESTAMPS, we are out of data.  File is complete.
                break
            else:
                t = str(tsDecode(ts))
                print "time is: ", t
                for table in tables:
                    header = 'TimeStamp'
                    i = 0
                    while True:
                        h = getVal(headerTable,table,i)
                        if h is None:
                            break
                        header += ',' + h
                        i += 1
                    header += '\n'
                    print 'HEADER:',header
                    outfile = OpenFile(table, directory, header)

                    (firstline,tableText,rc) = getTable(f, "^<DATA:", "^<END", None)
                    mybuffer = tableText.getvalue().split("\n")     # Process one line at a time.
                    stopRE = re.compile('^<END')
                    emptyRE = re.compile("^$")
                    for line in mybuffer:
                        line = line.rstrip()                        # remove CR NL and trailing blanks
                        line=re.sub(r'\s+$','',line)
                        line=re.sub(r',$','',line)
                        values = line.split(",")                    # The values in the record (before rate adjustment)
                        pvalues = []                                # The values to be output (after rate adjustment)
                        key = table+'_'+values[0]
                        # The last line will match the Regular Expression stopRE ...
                        # Add time stamp to each line.  Drop the last line.
                        if not stopRE.match(line) and not emptyRE.match(line):
                            if firsttimestamp[table]:
                                # store all of the columns so we can process rates on the next pass
                                # print 'saving '+key+' for next pass '
                                priorrow[key] = values  # Keep a copy for next time
                            else:
                                i = 0
                                for val in values:
                                    newvalue = val
                                    oldvalue = priorrow[key][i]
                                    if debug > 19:
                                        print 'prior row has ',len(priorrow[key]),'columns'
                                        print 'table:',table,'i:',i,'keys: ',
                                        for k in priorrow[key]:
                                            print k,
                                        print '\n'
                                    if (oldvalue != '') and (oldvalue != " "):
                                        if debug > 10:
                                            print 'calling inRate,key:',key,'table:',table,'column:',i
                                        if getVal(rateTable,table,i):
                                            try:
                                                delta = long(float(newvalue) - float(oldvalue))
                                                pvalues.append(delta)
                                            except ValueError:
                                                print 'ValueError, table:',table,'key:',key,'newValue:',values[i],'oldvalue:',oldvalue
                                        else:
                                            pvalues.append(newvalue)
                                    i += 1
                                pbuf = t
                                for v in pvalues:
                                    pbuf = pbuf+','+str(v)
                                pbuf += '\n'
                                outfile.write(pbuf)                    # output the record
                                if debug > 9:
                                    print pbuf
                                # Update the priorrow record, for next time ...
                                tmpbuf = []
                                for v in values:
                                    tmpbuf.insert(-0,v)
                                priorrow[key] = tmpbuf
                        else:
                            firsttimestamp[table] = False

                    CloseFile(outfile)
