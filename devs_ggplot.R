devread <- function(filename,rows){
  cols <- c('integer','factor',rep('numeric',56))
  d<-read.table(filename,header = T, colClasses=cols, stringsAsFactors = F,sep=',',nrows=rows)
  d<-tbl_df(d)
  d$TimeStamp<-as.POSIXct(d$TimeStamp,origin = "1970-01-01",tz="GMT")
  d$sampled.average.read.time.ms <- d$sampled.read.time.per.sec / d$sampled.reads.per.sec
  d$sampled.average.write.time.ms <- d$sampled.write.time.per.sec / d$sampled.writes.per.sec
  d$sampled.average.read.miss.time.ms <- d$sampled.read.miss.time.per.sec / d$sampled.read.miss.waits.per.sec
  d$sampled.average.WP.disconnect.time.ms <- d$sampled.WP.disconnect.time.per.sec / d$sampled.WP.disconnects.per.sec
  d$sampled.average.RDF.write.time.ms <- d$sampled.RDF.write.time.per.sec / d$sampled.RDF.write.waits.per.sec
  d$average.DA.req.time..ms <- d$DA.req.time.per.sec / d$DA.read.requests.per.sec
  d$average.DA.disk.time..ms <- d$DA.disk.time.per.sec / d$DA.read.requests.per.sec
  d$average.DA.task.time..ms <- d$DA.task.time.per.sec / d$DA.read.requests.per.sec
  d$total.reads.per.sec <- d$random.reads.per.sec + d$seq.reads.per.sec
  d$total.read.hits.per.sec <- d$random.read.hits.per.sec + d$seq.read.hits.per.sec
  d$total.read.misses.per.sec <- d$total.reads.per.sec - d$total.read.hits.per.sec
  d$total.writes.per.sec <- d$random.writes.per.sec + d$seq.writes.per.sec
  d$total.ios.per.sec <- d$total.reads.per.sec + d$total.writes.per.sec
  d$total.hits.per.sec <- d$total.read.hits.per.sec + d$random.write.hits.per.sec + d$seq.write.hits.per.sec
  d$total.misses.per.sec <- d$total.ios.per.sec - d$total.hits.per.sec
  d$total.write.misses.per.sec <- d$total.writes.per.sec - d$random.write.hits.per.sec - d$seq.write.hits.per.sec
  d$total.seq.ios.per.sec <- d$seq.reads.per.sec + d$seq.writes.per.sec
  d$random.read.misses.per.sec <- d$random.reads.per.sec - d$random.read.hits.per.sec
  d$percent.random.read.hit <- 100 * (d$random.read.hits.per.sec / d$total.ios.per.sec)
  d$percent.random.read.miss <- 100 * (d$random.read.misses.per.sec / d$total.ios.per.sec)
  d$percent.sequential.read <- 100 * (d$seq.reads.per.sec / d$total.ios.per.sec)
  d$percent.write <- 100 * (d$total.writes.per.sec / d$total.ios.per.sec)
  d$percent.read <- 100 * (d$total.reads.per.sec / d$total.ios.per.sec)
  d$percent.hit <- 100 * (d$total.hits.per.sec / d$total.ios.per.sec)
  d$percent.miss <- 100 - d$percent.hit
  d$percent.read.hit <- 100 * (d$total.read.hits.per.sec / d$total.reads.per.sec)
  d$percent.write.hit <- 100 * ((d$random.write.hits.per.sec + d$seq.write.hits.per.sec) / d$total.writes.per.sec)
  d$percent.read.miss <- 100 * (d$total.read.misses.per.sec / d$total.reads.per.sec)
  d$percent.write.miss <- 100 * (d$total.write.misses.per.sec / d$total.writes.per.sec)
  d$percent.sequential.io <- 100 * (d$total.seq.ios.per.sec / d$total.ios.per.sec)
  d$percent.sequential.writes <- 10 * (d$seq.writes.per.sec / d$total.ios.per.sec)
  d$HA.Kbytes.transferred.per.sec <- d$Kbytes.read.per.sec + d$Kbytes.written.per.sec
  d$average.read.size.in.Kbytes <- d$Kbytes.read.per.sec / d$total.reads.per.sec
  d$average.write.size.in.Kbytes <- d$Kbytes.written.per.sec / d$total.writes.per.sec
  d$average.io.size.in.Kbytes <- d$HA.Kbytes.transferred.per.sec / d$total.ios.per.sec
  d$DA.Kbytes.transferred.per.sec <- d$DA.Kbytes.read.per.sec + d$DA.Kbytes.written.per.sec
  d$percent.read.hit.of.ios <- 100 * (d$total.read.hits.per.sec / d$total.ios.per.sec)
  d$percent.read.miss.of.ios <- 100 * (d$total.read.misses.per.sec / d$total.ios.per.sec)
  d$percent.write.hit.of.ios <- 100 * ((d$random.write.hits.per.sec + d$seq.write.hits.per.sec) / d$total.ios.per.sec)
  d$percent.write.miss.of.ios <- 100 * (d$total.write.misses.per.sec / d$total.ios.per.sec)
  d$device.capacity.in.MB <- (d$device.capacity/(2^20)) * d$device.block.size 
  return(d)
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


# Multiple plot function
# http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_(ggplot2)/
# ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
# - cols:   Number of columns in layout
# - layout: A matrix specifying the layout. If present, 'cols' is ignored.
#
# If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
# then plot 1 will go in the upper left, 2 will go in the upper right, and
# 3 will go all the way across the bottom.
#
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  require(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}


library(dplyr)
library(ggplot2)
library('XML')

# File with storage group information in XML format
# from symsg list -v --output XML
xdir <- '/media/cdd/Seagate Backup Plus Drive/HK192602527_VMAX'
sgfile <- paste(xdir,'sg_2527.xml',sep='/')

# Input stats
# as parsed from TTP file
datad <- paste(xdir,'output',sep='/')
fname <- paste(datad,"Devices",sep = '/')

print('reading ttp data for devices')
devs <- devread(fname,rows=-1)
print('reading Storage Group XML')
stgroups <-parseSG(sgfile)

print('joining storage group names into devices')
devs <- merge(devs,stgroups,by='device.name')
devs$LongLabel <- paste(devs$device.name,devs$sgname)
rm(stgroups)

# remove volumes with no disk I/O.
# ?? gatekeepers, tdevs? << Am I doing this backward >>?

n<-devs %>% group_by(device.name) %>% summarise(max(DA.Kbytes.transferred.per.sec))
# n2 <- n[n[2]>0,]
# only devices with IOPS are included
devs <- devs[ n[2] > 0, ]
rm(n)

# Add a factor to each LUN, based on IO Size
# Try dividing into quartiles.
sz <- select(devs,TimeStamp,device.name,average.io.size.in.Kbytes)
sz <- filter(sz,! is.na(average.io.size.in.Kbytes), average.io.size.in.Kbytes != Inf)
cutpoints <- quantile(sz$average.io.size.in.Kbytes,seq(0,1,length=5),na.rm=TRUE)
devs$sz <- cut(devs$average.io.size.in.Kbytes,cutpoints)

# TOP N by IOPS
meaniops <- devs %>% group_by(device.name) %>% summarise(avg = mean(total.ios.per.sec)) %>% arrange(avg)
# find the top n vols by total.ios.per.sec
meaniops <- meaniops[order(meaniops$avg,decreasing=TRUE),]

topvols <- head(meaniops$device.name,8)
rm(meaniops)

plots <-list()
i = 1
for (vol in topvols) {
  tdf <- filter(devs,device.name == vol)
  g <- ggplot(tdf, aes(TimeStamp,total.ios.per.sec))
  p <- g + geom_point() + geom_smooth()
  lunName <- unique(tdf$LongLabel)
  title <- paste(lunName,"IOPS")
  print(title)
  p <- p + ggtitle (title)
  plots[[i]] <- p
  # print(p)
  i = i + 1
}
multiplot(plotlist = plots ,cols=2)

stop()

top <- head(meaniops$device.name,16)
d <- filter(devs,device.name %in% top)
g <- ggplot(d,aes(TimeStamp,total.ios.per.sec))
p <- g + geom_point(alpha = 0.4, aes(color=device.name,size=sz)) + geom_smooth()

print(p)

stop()

# TOP N by Random Read IOPS
meaniops <- devs %>% group_by(device.name) %>% summarise(avg = max(random.reads.per.sec)) %>% arrange(avg)
# find the top 64 vols by random.reads.per.sec
meaniops <- meaniops[order(meaniops$avg,decreasing=TRUE),]
topvols <- head(meaniops$device.name,16)
par (mar=c(4,3,4,2),mfrow=c(4,4),oma=c(3,2,6,2))
i = 16
pagenum = 1
for (vol in topvols) {
  tdf <- filter(devs,device.name==vol)
  x <- tdf$TimeStamp
  y <- tdf$random.reads.per.sec
  plot(x,y,ylab='iops',xlab='',main=paste('volume:',vol),cex=0.5,pch=19,col='blue')
  lines(supsmu(x,y),col='chocolate2',lwd=2)
  abline(h=mean(y),lty=2)
  i = i - 1
  if ( i == 0 ) {
    i = 16
    title('LUNs with max Random Read IOPS',outer=TRUE)
    mtext(pagenum,side=1, outer = TRUE)
    pagenum = pagenum + 1
  }
}

# TOP N by total BW
meaniops <- devs %>% group_by(device.name) %>% summarise(avg = max(Kbytes.read.per.sec+Kbytes.written.per.sec)) %>% arrange(avg)
# find the top N vols by BW
meaniops <- meaniops[order(meaniops$avg,decreasing=TRUE),]
topvols <- head(meaniops$device.name,16)
par (mar=c(4,3,4,2),mfrow=c(4,4),oma=c(3,2,6,2))
i = 16
pagenum = 1
for (vol in topvols) {
  tdf <- filter(devs,device.name==vol)
  x <- tdf$TimeStamp
  y <- (tdf$Kbytes.read.per.sec + tdf$Kbytes.written.per.sec) / (2^10)
  plot(x,y,ylab='iops',xlab='',main=paste('volume:',vol),cex=0.5,pch=19,col='blue')
  lines(supsmu(x,y),col='chocolate2',lwd=2)
  abline(h=mean(y),lty=2)
  i = i - 1
  if ( i == 0 ) {
    i = 16
    title('LUNs by peak BW (MB/Sec)',outer=TRUE)
    mtext(pagenum,side=1, outer = TRUE)
    pagenum = pagenum + 1
  }
}

# top mean BW
meaniops <- devs %>% group_by(device.name) %>% summarise(avg = mean(Kbytes.read.per.sec+Kbytes.written.per.sec)) %>% arrange(avg)
# find the top N vols by BW
meaniops <- meaniops[order(meaniops$avg,decreasing=TRUE),]
topvols <- head(meaniops$device.name,16)
par (mar=c(4,3,4,2),mfrow=c(4,4),oma=c(3,2,6,2))
i = 16
pagenum = 1
for (vol in topvols) {
  tdf <- filter(devs,device.name==vol)
  x <- tdf$TimeStamp
  y <- (tdf$Kbytes.read.per.sec + tdf$Kbytes.written.per.sec) / (2^10)
  plot(x,y,ylab='iops',xlab='',main=paste('volume:',vol),cex=0.5,pch=19,col='blue')
  lines(supsmu(x,y),col='chocolate2',lwd=2)
  abline(h=mean(y),lty=2)
  i = i - 1
  if ( i == 0 ) {
    i = 16
    title('LUNs by mean BW (MB/Sec)',outer=TRUE)
    mtext(pagenum,side=1, outer = TRUE)
    pagenum = pagenum + 1
  }
}

options(digits=1)
# build a table of the stats of top iop vols
t<-devs %>% group_by(device.name) %>% summarise( 
  iop_mean=mean(total.ios.per.sec),
  iop_min=min(total.ios.per.sec),
  iop_max=max(total.ios.per.sec),
  iop_sd=sd(total.ios.per.sec),
  rdBW_mean=mean(Kbytes.read.per.sec),
  rdBW_max=max(Kbytes.read.per.sec),
  wtBW_mean=mean(Kbytes.written.per.sec),
  wtBW_max=max(Kbytes.written.per.sec),
  rdsz = mean(average.read.size.in.Kbytes),
  wtsz = mean(average.write.size.in.Kbytes,na.rm=T)
) %>% arrange(desc(wtBW_max))
head(as.data.frame(t),75)
