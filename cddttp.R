sysread <- function(filename){
    # Read the system information for TTP Data
    # Calculate the derived variables and add them to the data.frame
    # Returns data frame with system variables and observations
    # Dickerson - 28Feb2015
  
  s<-read.table(filename,header = T, stringsAsFactors = F,sep=',')
  s<-tbl_df(s)
  s$TimeStamp<-as.POSIXct(s$TimeStamp,origin = "1970-01-01",tz="GMT")
  # plot(s$ios.per.sec,type='l',x=s$TimeStamp)
  s$total.reads.per.sec <- s$reads.per.sec + s$seq.reads.per.sec
  s$total.ios.per.sec <- s$reads.per.sec + s$seq.reads.per.sec + s$writes.per.sec
  s$percent.reads <- 100 * (s$total.reads.per.sec / s$total.ios.per.sec)
  s$percent.writes <- 100 * (s$writes.per.sec / s$total.ios.per.sec)
  s$percent.hit <- 100 * (s$hits.per.sec/s$total.ios.per.sec)
  s$Kbytes.transferred.per.sec <- s$Kbytes.written.per.sec + s$Kbytes.read.per.sec
  s$percent.sequential.io <- 100 * (s$seq.reads.per.sec / s$total.ios.per.sec)
  plot(s$TimeStamp,s$TimeStamp,type='l')
  return(s)
}

filterTDEVS <- function (df) {
  # remove volumes with no disk I/O.
  # ?? gatekeepers, tdevs? << Am I doing this backward >>?
  n<-df %>% group_by(device.name) %>% summarise(max(DA.Kbytes.read.per.sec+DA.Kbytes.written.per.sec))
  # n2 <- n[n[2]>0,]
  # only devices with IOPS are included
  df <- df[ n[[2]] > 0, ]
  rm(n)
  return(df)  
}

calcDerivedDevs <- function (d) {
  # TTP file has a number of derived variables
  # which are calculated in this routine.
  # note that if you restrict which values are in the data frame
  # some of these derived variables are incalculable. (and thus commented out)
  # d<-tbl_df(d)
  d$TimeStamp<-as.POSIXct(d$TimeStamp,origin = "1970-01-01",tz="GMT")
  d$sampled.average.read.time.ms <- d$sampled.read.time.per.sec / d$sampled.reads.per.sec
  d$sampled.average.write.time.ms <- d$sampled.write.time.per.sec / d$sampled.writes.per.sec
#   d$sampled.average.read.miss.time.ms <- d$sampled.read.miss.time.per.sec / d$sampled.read.miss.waits.per.sec
#   d$sampled.average.WP.disconnect.time.ms <- d$sampled.WP.disconnect.time.per.sec / d$sampled.WP.disconnects.per.sec
#   d$sampled.average.RDF.write.time.ms <- d$sampled.RDF.write.time.per.sec / d$sampled.RDF.write.waits.per.sec
#   d$average.DA.req.time..ms <- d$DA.req.time.per.sec / d$DA.read.requests.per.sec
#   d$average.DA.disk.time..ms <- d$DA.disk.time.per.sec / d$DA.read.requests.per.sec
#   d$average.DA.task.time..ms <- d$DA.task.time.per.sec / d$DA.read.requests.per.sec
   d$total.reads.per.sec <- d$random.reads.per.sec + d$seq.reads.per.sec
#   d$total.read.hits.per.sec <- d$random.read.hits.per.sec + d$seq.read.hits.per.sec
#   d$total.read.misses.per.sec <- d$total.reads.per.sec - d$total.read.hits.per.sec
   d$total.writes.per.sec <- d$random.writes.per.sec + d$seq.writes.per.sec
   d$total.ios.per.sec <- d$total.reads.per.sec + d$total.writes.per.sec
#   d$total.hits.per.sec <- d$total.read.hits.per.sec + d$random.write.hits.per.sec + d$seq.write.hits.per.sec
#   d$total.misses.per.sec <- d$total.ios.per.sec - d$total.hits.per.sec
#   d$total.write.misses.per.sec <- d$total.writes.per.sec - d$random.write.hits.per.sec - d$seq.write.hits.per.sec
#   d$total.seq.ios.per.sec <- d$seq.reads.per.sec + d$seq.writes.per.sec
#   d$random.read.misses.per.sec <- d$random.reads.per.sec - d$random.read.hits.per.sec
#   d$percent.random.read.hit <- 100 * (d$random.read.hits.per.sec / d$total.ios.per.sec)
#   d$percent.random.read.miss <- 100 * (d$random.read.misses.per.sec / d$total.ios.per.sec)
#   d$percent.sequential.read <- 100 * (d$seq.reads.per.sec / d$total.ios.per.sec)
#   d$percent.write <- 100 * (d$total.writes.per.sec / d$total.ios.per.sec)
#   d$percent.read <- 100 * (d$total.reads.per.sec / d$total.ios.per.sec)
#   d$percent.hit <- 100 * (d$total.hits.per.sec / d$total.ios.per.sec)
#   d$percent.miss <- 100 - d$percent.hit
#   d$percent.read.hit <- 100 * (d$total.read.hits.per.sec / d$total.reads.per.sec)
#   d$percent.write.hit <- 100 * ((d$random.write.hits.per.sec + d$seq.write.hits.per.sec) / d$total.writes.per.sec)
#   d$percent.read.miss <- 100 * (d$total.read.misses.per.sec / d$total.reads.per.sec)
#   d$percent.write.miss <- 100 * (d$total.write.misses.per.sec / d$total.writes.per.sec)
#   d$percent.sequential.io <- 100 * (d$total.seq.ios.per.sec / d$total.ios.per.sec)
#   d$percent.sequential.writes <- 10 * (d$seq.writes.per.sec / d$total.ios.per.sec)
   d$HA.Kbytes.transferred.per.sec <- d$Kbytes.read.per.sec + d$Kbytes.written.per.sec
#   d$average.read.size.in.Kbytes <- d$Kbytes.read.per.sec / d$total.reads.per.sec
#   d$average.write.size.in.Kbytes <- d$Kbytes.written.per.sec / d$total.writes.per.sec
   d$average.io.size.in.Kbytes <- d$HA.Kbytes.transferred.per.sec / d$total.ios.per.sec
   d$DA.Kbytes.transferred.per.sec <- d$DA.Kbytes.read.per.sec + d$DA.Kbytes.written.per.sec
#   d$percent.read.hit.of.ios <- 100 * (d$total.read.hits.per.sec / d$total.ios.per.sec)
#   d$percent.read.miss.of.ios <- 100 * (d$total.read.misses.per.sec / d$total.ios.per.sec)
#   d$percent.write.hit.of.ios <- 100 * ((d$random.write.hits.per.sec + d$seq.write.hits.per.sec) / d$total.ios.per.sec)
#   d$percent.write.miss.of.ios <- 100 * (d$total.write.misses.per.sec / d$total.ios.per.sec)
#   d$device.capacity.in.MB <- (d$device.capacity/(2^20)) * d$device.block.size 
  return(d)
}


DevRead <- function(filename,rows){
  # use scan directly, don't keep columns we don't use
  # to reduce memory demands
  # uses the getcolumns() function as a helper for book keeping about
  # which to keep.
  hdrs <- getcolumns()
  cols <- as.vector(hdrs$vartyp)
  # d <-read.table(filename,header=T,sep=',',stringsAsFactors = T, colClasses = cols,nrows=rows)
  d <-fread(filename,header=T,sep=',',stringsAsFactors = T, colClasses = cols,nrows=rows)
  n <- make.names(colnames(d))
  setnames(d,n)
  print('filter TDEVs')
  d <- filterTDEVS(d)
  print('calculating derived values')
  d <- calcDerived(d)
}

getcolumns <- function() {
  # convenience routine
  # loads the headers data frame
  # which tell which columns to consider for reporting
  # and thus read into the dataframe
  # Column One is the variable name a the time this function was written
  # Column Two is the colClasses field in read.table
  # typically numeric or NULL
  
  v <- c( 
    "TimeStamp",'integer',
    "device.name",'factor',
    "random.ios.per.sec",'integer',
    "random.reads.per.sec",'integer',
    "random.writes.per.sec",'integer',
    "random.hits.per.sec",'integer',
    "random.read.hits.per.sec",'integer',
    "random.write.hits.per.sec",'integer',
    "seq.reads.per.sec",'integer',
    "seq.read.hits.per.sec",'integer',
    "seq.writes.per.sec",'integer',
    "seq.write.hits.per.sec",'integer',
    "Kbytes.read.per.sec",'integer',
    "Kbytes.written.per.sec",'integer',
    "DA.read.requests.per.sec",'NULL',
    "DA.write.requests.per.sec",'NULL',
    "DA.prefetched.tracks.per.sec",'NULL',
    "DA.prefetched.tracks.used.per.sec",'NULL',
    "DA.Kbytes.read.per.sec",'numeric',
    "DA.Kbytes.written.per.sec",'numeric',
    "DA.req.time.per.sec",'NULL',
    "DA.disk.time.per.sec",'NULL',
    "DA.task.time.per.sec",'NULL',
    "write.pending.count",'NULL',
    "max.write.pending.threshold",'NULL',
    "sampled.read.time.per.sec",'integer',
    "sampled.write.time.per.sec",'integer',
    "sampled.read.miss.time.per.sec",'NULL',
    "sampled.WP.disconnect.time.per.sec",'NULL',
    "sampled.RDF.write.time.per.sec",'NULL',
    "sampled.reads.per.sec",'integer',
    "sampled.writes.per.sec",'integer',
    "sampled.read.miss.waits.per.sec",'NULL',
    "sampled.WP.disconnects.per.sec",'NULL',
    "sampled.RDF.write.waits.per.sec",'NULL',
    "num.invalid.tracks",'NULL',
    "M1.invalid.tracks",'NULL',
    "M2.invalid.tracks",'NULL',
    "M3.invalid.tracks",'NULL',
    "M4.invalid.tracks",'NULL',
    "DA.partial.sector.write.Kbytes.per.sec",'NULL',
    "DA.optimize.write.Kbytes.per.sec",'NULL',
    "DA.xor.reads.per.sec",'NULL',
    "DA.xor.Kbytes.read.per.sec",'NULL',
    "DA.read.for.copy.per.sec",'NULL',
    "DA.Kbytes.read.for.copy.per.sec",'NULL',
    "DA.writes.for.copy.per.sec",'NULL',
    "DA.Kbytes.written.for.copy.per.sec",'NULL',
    "DA.read.for.vlun.migration.per.sec",'NULL',
    "DA.Kbytes.read.for.vlun.migration",'NULL',
    "DA.writes.for.vlun.migration.per.sec",'NULL',
    "DA.Kbytes.written.for.vlun.migration",'NULL',
    "DA.writes.for.rebuild.per.sec",'NULL',
    "DA.Kbytes.written.for.rebuild.per.sec",'NULL',
    "rdf.copy.per.sec",'NULL',
    "rdf.copy.Kbytes.per.sec",'NULL',
    "device.capacity",'NULL',
    "device.block.size",'NULL',
    "optimized.read.miss.Kbytes",'NULL',
    "optimized.read.miss",'NULL'
  )
  hdr <- as.data.frame(matrix(data=v,nrow=60,ncol=2,byrow=TRUE))
  names(hdr) <- c('varnm','vartyp')
  return(hdr)
}

parseSG <- function(infile) {
  # Read the XML file associated with symsg list -v --output XML 
  # This has details of which Devices are associated with which storage groups
  # return a data frame, suitable for mapping device name to volue serial number
  doc = xmlParse(infile)
  xpath='/SymCLI_ML/SG'
  # get the nodes for Storage Groups (SG)
  nodeset <- getNodeSet(doc,xpath)
  
  df <- data.frame()
  # for each sg node build a node data frame (ndf)
  # then append it to the function return data frame (df)
  for (node in nodeset) {
    # Name of this storage group
    sgname = xmlValue(node[['SG_Info']][['name']])
    # print (paste('processing storage group',sgname))
    symid = xmlValue(node[['SG_Info']][['symid']])
    count = xmlValue(node[['SG_Info']][['SG_group_info']][['count']])
    if ( count > 1 ) {
      # If this is a cascaded entry, don't add to the data frame
      # The cascaded elements will be added instead
      # print('cascading detected\n')
      #       names = xmlToList(node[['SG_Info']][['SG_group_info']][['SG']],simplify=TRUE )
      #       for (n in names) {
      #         print (paste('   cascaded name',n))
      #       }
    } else {
      # Device details
      ndf = xmlToDataFrame(node[['DEVS_List']])
      ndf<-cbind(sgname,symid,ndf)
      df <- rbind(df,ndf)
    }
  }
  names(df) = c( "sgname","symid","device.name","pd_name","configuration","status","megabytes")
  df$device.name <- paste('0x',df$device.name,sep='')

  return(df)
}

topLuns <- function(devs,v='total.ios.per.sec',plots=4,plotsperpage = 4, columns = 2,size = 1) {
  m <- devs[,c('TimeStamp','device.name',v,'LongLabel'),with=FALSE]
  setnames(m,c('time','device','value','label'))
  m <- filter(m, !is.na(m$value), m$value != Inf)
  m2 <- m %>% group_by(device) %>% summarise(avg = mean(value,na.rm = TRUE)) %>% arrange(desc(avg))

  pages = plots / plotsperpage
  for (page in 1:pages) {
    topvols <- as.vector(head(m2$device,plots))
    plots <-list()
    i = 1
    for (vol in topvols) {
      tdf <- filter(m,device == vol)
      
      # Determine Scale for X axis titles (http://docs.ggplot2.org/0.9.3.1/scale_datetime.html)
      g <- ggplot(tdf, aes(x = time,y = value ))
      if ( size != 1 ) {
        p <- g + geom_point(col='chocolate3',aes(size=4))         
        } else {
        p <- g + geom_point(col='blue4')         
      }
      p <- p + geom_smooth(col='darkred',method='loess')
      p50 <- as.numeric(quantile(tdf$value,probs=c(.5),na.rm=TRUE))      
      p95 <- as.numeric(quantile(tdf$value,probs=c(.95),na.rm=TRUE))
      start <- min(tdf$time) - 500
      p <- p + geom_hline(yintercept=p50,colour='green4',linetype=2)
      p <- p + annotate("text",x=start,y=p50,label=c("mean"))
      p <- p + geom_hline(yintercept=p95,color='green',linetype=4)
      p <- p + annotate("text",x=start,y=p95,label=c("95th"))
      lunName <- unique(tdf$label)
      title <- paste(v,lunName)
      print(title)
      p <- p + ggtitle (title)
      plots[[i]] <- p
      # print(p)
      i = i + 1
    }
    multiplot(plotlist = plots ,cols=columns)
  }
}

topNIOPS <- function (df,n=4) {
  meaniops <- df %>% group_by(device.name) %>% summarise(avg = mean(total.ios.per.sec)) %>% arrange(avg)
  meaniops <- meaniops[order(meaniops$avg,decreasing=TRUE),]
  top <- head(meaniops$device.name,n)
  d <- filter(df,device.name %in% top)
  g <- ggplot(d,aes(TimeStamp,total.ios.per.sec))
  p <- g + geom_point(alpha = 0.6, aes(color=LongLabel,size=average.io.size.in.Kbytes))
  p <- p + guides(colour = guide_legend(override.aes = list(alpha = 1,size=8),ncol=1,byrow=TRUE))
  p <- p + geom_smooth(method='gam')
  p <- p + theme(legend.title=element_text(face="italic"))
  p <- p + theme(legend.key.size = unit(2,"cm"))
  p <- p + scale_size_continuous(range = c(1, 25),guide_legend(title='size'))
  title <- paste(serialnumber,"top",n,"LUNs by mean IOPS")
  p <- p + ggtitle(title)
  return(p)
}

topNBW <- function (df,n=4) {
  meanbw <- df %>% group_by(device.name) %>% 
    summarise(avg = mean(HA.Kbytes.transferred.per.sec)) %>% 
    arrange(avg)
  meanbw <- meanbw[order(meanbw$avg,decreasing=TRUE),]
  top <- head(meanbw$device.name,n)
  d <- filter(df,device.name %in% top)
  g <- ggplot(d,aes(TimeStamp,HA.Kbytes.transferred.per.sec))
  p <- g + geom_point(alpha = 0.6, aes(color=LongLabel))
  p <- p + guides(colour = guide_legend(override.aes = list(alpha = 1,size=8),ncol=1,byrow=TRUE))
  p <- p + geom_smooth(method='gam')
  p <- p + theme(legend.title=element_text(face="italic"))
  p <- p + theme(legend.key.size = unit(2,"cm"))
  p <- p + scale_size_continuous(range = c(1, 25),guide_legend(title='size'))
  title <- paste(serialnumber,"top",n,"LUNs by mean Bandwidth")
  p1 <- p + ggtitle(title)
  
  
  # Now print a bar plot of the top N
  
  dat <- filter(df,device.name %in% top)
  
  g2 <- ggplot(dat,aes(x=device.name,y=HA.Kbytes.transferred.per.sec))
  p2 <- g2 + geom_boxplot(aes(col=LongLabel),outlier.shape=NA)
  
  ymin <- as.numeric(0.9 * min(dat$HA.Kbytes.transferred.per.sec))
  ymax <- as.numeric(quantile(dat$HA.Kbytes.transferred.per.sec,probs=c(.98),na.rm=TRUE))

  p2 <- p2 + coord_cartesian(ylim = c(ymin,ymax))
  multiplot(plotlist=list(p1,p2),cols=1)
  
}

feread <- function(filename,nrows=-1) {
  # open the Front End Directors file
  # and apply the derived calculations.

  fe<-read.table(filename,header=T,sep=",",stringsAsFactors=F,nrows=nrows)
  fe <- tbl_df(fe)
  fe$TimeStamp <- as.POSIXct(fe$TimeStamp,origin="1970-01-01",tz="GMT")
  fe$director.number <- as.factor(fe$director.number)
  # Derived Variables
  fe$read.hits.per.second <- fe$requests.per.sec - fe$write.reqs.per.sec - fe$read.misses.per.sec
  fe$percent.hit <- 100*(fe$hits.per.sec/fe$requests.per.sec)
  fe$percent.write <- 100*(fe$write.reqs.per.sec/fe$requests.per.sec)
  fe$percent.read <- 100*(fe$read.reqs.per.sec/fe$requests.per.sec)
  fe$percent.read.hit <- 100*(fe$read.hits.per.sec/fe$read.reqs.per.sec)
  fe$percent.idle <- 100*(fe$accumulated.director.idle.time / fe$interval.time)
  fe$percent.busy <- (100 - fe$percent.idle)
  
  fe$Avg.time.per.syscall <- fe$syscall.time.per.sec / fe$syscall.count.per.sec
  fe$average.queue.depth.range.0 <- 0
  fe$average.queue.depth.range.1 <- (fe$accumulated.queue.depth.range.1 / fe$queue.depth.count.range.1)
  fe$average.queue.depth.range.2 <- (fe$accumulated.queue.depth.range.2 / fe$queue.depth.count.range.2)
  fe$average.queue.depth.range.3 <- (fe$accumulated.queue.depth.range.3 / fe$queue.depth.count.range.3)
  fe$average.queue.depth.range.4 <- (fe$accumulated.queue.depth.range.4 / fe$queue.depth.count.range.4)
  fe$average.queue.depth.range.5 <- (fe$accumulated.queue.depth.range.5 / fe$queue.depth.count.range.5)
  fe$average.queue.depth.range.6 <- (fe$accumulated.queue.depth.range.6 / fe$queue.depth.count.range.6)
  fe$average.queue.depth.range.7 <- (fe$accumulated.queue.depth.range.7 / fe$queue.depth.count.range.7)
  fe$average.queue.depth.range.8 <- (fe$accumulated.queue.depth.range.8 / fe$queue.depth.count.range.8)
  fe$average.queue.depth.range.9 <- (fe$accumulated.queue.depth.range.9 / fe$queue.depth.count.range.9)
  
  fe$percent.read.hit.of.requests <- 100*(fe$read.hits.per.sec/fe$requests.per.sec)
  fe$average.read.response.time.range.0 <- fe$accumulated.read.response.range.0 / fe$read.response.time.count.range.0
  fe$average.read.response.time.range.1 <- fe$accumulated.read.response.range.1 / fe$read.response.time.count.range.1
  fe$average.read.response.time.range.2 <- fe$accumulated.read.response.range.2 / fe$read.response.time.count.range.2
  fe$average.read.response.time.range.3 <- fe$accumulated.read.response.range.3 / fe$read.response.time.count.range.3
  fe$average.read.response.time.range.4 <- fe$accumulated.read.response.range.4 / fe$read.response.time.count.range.4
  fe$average.read.response.time.range.5 <- fe$accumulated.read.response.range.5 / fe$read.response.time.count.range.5
  fe$average.read.response.time.range.6 <- fe$accumulated.read.response.range.6 / fe$read.response.time.count.range.6
  fe$average.read.response.time.range.7 <- fe$accumulated.read.response.range.7 / fe$read.response.time.count.range.7
  fe$average.write.response.time.range.0 <- fe$accumulated.write.response.range.0 / fe$write.response.time.count.range.0
  fe$average.write.response.time.range.1 <- fe$accumulated.write.response.range.1 / fe$write.response.time.count.range.1
  fe$average.write.response.time.range.2 <- fe$accumulated.write.response.range.2 / fe$write.response.time.count.range.2
  fe$average.write.response.time.range.3 <- fe$accumulated.write.response.range.3 / fe$write.response.time.count.range.3
  fe$average.write.response.time.range.4 <- fe$accumulated.write.response.range.4 / fe$write.response.time.count.range.4
  fe$average.write.response.time.range.5 <- fe$accumulated.write.response.range.5 / fe$write.response.time.count.range.5
  fe$average.write.response.time.range.6 <- fe$accumulated.write.response.range.6 / fe$write.response.time.count.range.6
  fe$average.write.response.time.range.7 <- fe$accumulated.write.response.range.7 / fe$write.response.time.count.range.7
  fe$average.read.time.ms <- fe$total.read.time.per.sec / (1000*fe$total.read.count.per.sec)
  fe$average.write.time.ms <- fe$total.write.time.per.sec / (1000*fe$total.write.count.per.sec)
  
  return (fe)
} # end fe_read

feboxplot <- function(df) {
  par (mfrow=c(2,1),bg='grey')
  # prepare for box plots
  t2<-select(df,TimeStamp,director.number,average.read.time.ms, average.write.time.ms)
  t2 <- t2[complete.cases(t2),]  # remove NA
  names(t2) <- c('ts','dir','read','write')  # friendly names
  from <- min(t2$ts)
  to <- max(t2$ts)
  boxplot(read~dir,data=t2,las=2,cex.axis=.4, 
          horizontal=TRUE,
          ylab='Director FE',
          xlab='Service Time (mSec)',
          col=c('green','blue','purple','red','orange'),
          main='Read Service Time by Front End Director',
          outline=F,
          sub=paste('start time: ',from,'end time: ',to))

  boxplot(write~dir,data=t2,las=2,cex.axis=.4,
          horizontal=TRUE,
          ylab='Director FE',
          xlab='Service Time (mSec)',          
          col=c('green','blue','purple','red','orange'),
          main='Write Service Time by Front End Director',
          outline=F,
          sub=paste('start time: ',from,'end time: ',to))
}

getdirs <- function(df) {
  # return a list of front end directors
  
  # Determine which directors have I/O ( and thus will be reported )
  dirs<-select(df,director.number)
  dirs<-distinct(dirs)
  cat ('checking for directors with no I/O\n')
  ndir <- c();
  # Build a list of interesting directors
  by_dir <- group_by(df,director.number)
  dirs<-summarize(by_dir,max(ios.per.sec))
  names(dirs) <- c('director.number','maxio')
  dirs <- dirs[order(dirs$maxio,decreasing=TRUE),]
  for (d in dirs$director.number) {
    # Select only directors that have I/O
    i <- sum(select(filter(df,director.number==d),ios.per.sec))
    if ( i > 0 ){
      ndir <- append(ndir,d)
    } else {
      cat('eliminated director',d,'for lack of traffic\n')
    }  
  }
  dirs <- ndir
}

fedetails <-function(df) {
  
  dirs <- getdirs(df)
  
  # Per Director Graphs
  opar <- par(no.readonly=T)
  par (mfrow=c(4,4))
  times <- select(df,TimeStamp,
                  director.number,
                  ios.per.sec,
                  average.read.time.ms,
                  average.write.time.ms,
                  port.0.read.Kbytes.per.sec,
                  port.1.read.Kbytes.per.sec,
                  port.0.write.Kbytes.per.sec,
                  port.1.write.Kbytes.per.sec)
  times <- times[complete.cases(times),] # Remove NA Values
  times <- tbl_df(times)
  

  # For debugging, give an opt out after N directors
  # Set n <- -1 for running all director plots
  # Set n <- #  to stop after # director plots
  n <- -1
  for (d in dirs) {
    if ( n >= 0 ) {
      # if we're debugging (n=-1 to turn off)
      if( n == 0) {
        break
      } else {
        n = n - 1
      }
    }
    cat ('processing graphs for director',d,'\n')
    
    # IOPS

    t1<-select(filter(times,director.number==d),TimeStamp,ios.per.sec)
    t1<- t1[complete.cases(t1),]
    x <- t1$TimeStamp
    y <- t1$ios.per.sec
    if (length(x) == 0 || length(y) ==0) {
      cat('detected empty data - skipping director',d,'\n')
      next
    }
    t <- paste('Front End ',d, 'IOPS')

    plot(x,y,ylab='I/O / Sec',pch=20,cex.axis=0.75,col='firebrick2',main=t,xlab='')
    lines(supsmu(x,y),col='black',lwd=3)
    abline(h=mean(y),lty=2)
    

    # Bandwidth - Reads

    t1<-select(filter(times,director.number==d),TimeStamp,port.0.read.Kbytes.per.sec,port.1.read.Kbytes.per.sec)
    t1<- t1[complete.cases(t1),]
    t1<-filter(t1,port.0.read.Kbytes.per.sec!=Inf)
    x <- t1$TimeStamp
    y0 <- t1$port.0.read.Kbytes.per.sec/1000
    y1 <- t1$port.1.read.Kbytes.per.sec/1000    
    t1<-filter(t1,port.1.read.Kbytes.per.sec!=Inf)
    
    plot(x,y0,col='blue',pch=19,cex=.5,xlab='',ylab='read MB/Sec')
    abline(h=mean(y0),col='blue',lty=2)
    
    points(x=t1$TimeStamp,y=y1,col='green',pch=19,cex=.5,xlab='',ylab='')
    abline(h=mean(y1),col='green',lty=2)
    # legend(0,1,c('port 0','port 1'))

    t <- paste(d,"Read Bandwidth (MB/Sec)")
    title(main=t)    
    
    
    # Bandwidth  - Writes
    t1<-select(filter(times,director.number==d),TimeStamp,
               port.0.write.Kbytes.per.sec,      
               port.1.write.Kbytes.per.sec)
    t1 <- t1[complete.cases(t1),]
    t1<-filter(t1,port.0.write.Kbytes.per.sec!=Inf)
    t1<-filter(t1,port.1.write.Kbytes.per.sec!=Inf)
    y0 <- t1$port.0.write.Kbytes.per.sec/1000
    y1 <- t1$port.1.write.Kbytes.per.sec/1000
    plot(x=t1$TimeStamp,y=y0,col='blue',pch=19,cex=.5,xlab='',ylab='read MB/Sec')
    abline(h=mean(y0),col='blue',lty=2)
    
    points(x=t1$TimeStamp,y=y1,col='green',pch=19,cex=.5,xlab=F,ylab=F)
    abline(h=mean(y1),col='green',lty=2)
    
    t <- paste(d,"Write Bandwidth (MB/Sec)")
    title(main=t)

    #  estmate queue lengths
    # Assume it is open systems for now.
    open <- c(0,5,10,20,40,80,160,320,640,1280)
    openmid <- as.vector(mode='numeric',open/2)
    
    # just work with this director
    d2 <- filter(df,director.number == d)
    q <- select(d2,queue.depth.count.range.0:queue.depth.count.range.9)
    
    setnames(q,c('r0','r1','r2','r3','r4','r5','r6','r7','r8','r9'))
    
    barplot(as.matrix(q),main=paste(d,'queue.depth.count.range buckets'))
    
    addem <- function(v1,v2) { 
      # dot product of buckets to estimate the queue length
      zed <- function (x) { 
        # zero out Na and Inf Values.
        if (is.finite(x)) {x }
        else { 0 }  
      }
      sapply(v1,zed)  # Zero out NA and Inf values
      sum(v1*v2) 
    }
    qlen <- apply(q,1,addem,openmid) # Compute Dot products, then count the IOPS, the divide to make an average ?
    iocount <- apply(q,1,sum)
    qlen <- qlen/iocount
    plot(d2$TimeStamp,qlen,pch=19,col='purple',main=paste(d,'Estimate Queue Length'),xlab='',ylab='est queue length')
    abline(h=mean(qlen),lty=2)
    
    hist(qlen,breaks=128,col='purple',main=paste(d,'Estimated Queue Length'))
    abline(v=mean(qlen),lty=2)
    
    
    
    # Read Service Time
    t1<-select(filter(times,director.number==d),TimeStamp,average.read.time.ms)
    t1 <-t1[complete.cases(t1),]
    t1 <- filter(t1,average.read.time.ms != Inf)
    x<-t1[[1]]
    rst<-t1[[2]]   # Read Service Time
    plot(x,y=rst,ylab='mSec',xlab='',col='gold',pch=20,cex.axis=0.75,main=(paste('Director:',d,'Read Service Time (mSec')))
    abline(h=mean(rst),col='blue3',lty=2,lwd=2)
    lines(supsmu(x,rst),col='grey2',lwd=1)

    
    
    hist(y,breaks=128,main=paste(d,'Read Service Time (mSec)'),ylab='Frequency',xlab='',col='gold')
    abline(v=mean(y),lty=2)

    # Write Service Time
    t1<-select(filter(times,director.number==d),TimeStamp,average.write.time.ms)
    t1 <-t1[complete.cases(t1),]
    t1 <- filter(t1,average.write.time.ms != Inf)
    x<-t1[[1]]
    wst <-t1[[2]]   # Write Service Time
    plot(x,y=wst,t=,ylab='mSec',xlab='',cex.axis=0.75,col='green4',pch=19,main=(paste('Director:',d,'Write mSec Service Time')))
    abline(h=mean(wst),col='blue3',lty=2,lwd=2)
    lines(supsmu(x,wst),col='black',lwd=3)
    
    hist(y,breaks=128,main=paste(d,'Write Service Time (mSec)'),col='green4',ylab='Frequency',xlab='')        
    abline(v=mean(y),lty=2)

      # Fill the page with empty plots.
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    
    } # End For
}

diskread <- function(filename){
  colClasses <- c('integer','factor','factor',rep('numeric',17))
  d<-read.table(filename,header = T, colClasses= colClasses , stringsAsFactors = F,sep=',')
  d<-tbl_df(d)
  d$TimeStamp<-as.POSIXct(d$TimeStamp,origin = "1970-01-01",tz="GMT")
  # plot(s$ios.per.sec,type='l',x=s$TimeStamp)
  d$average.hypers.per.seek <- d$seek.distance.per.sec/d$seeks.per.sec
  d$average.kbytes.per.read <- d$Kbytes.read.per.sec/d$read.commands.per.sec
  d$average.kbytes.per.write <- d$Kbytes.written.per.sec/(d$write.commands.per.sec + d$skip.mask.commands.per.sec + d$XOR.write.commands.per.sec + d$XOR.read.commands.per.sec)
  d$average.read.time.ms <- d$total.read.time.per.sec/d$read.commands.per.sec/1000
  d$average.write.time.ms <- d$total.write.time.per.sec/d$write.commands.per.sec/1000
  d$percent.disk.idle <- (d$disk.idle.time.per.sec/d$disk.total.time.per.sec) * 100
  d$percent.disk.busy <- 100 - d$percent.disk.idle
  d$average.queue.depth <- d$accumulated.queue.depth / d$total.SCSI.command.per.sec
  return(d)
}



