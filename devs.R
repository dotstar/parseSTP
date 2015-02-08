devread <- function(filename,rows){
  cols <- c('integer','factor',rep('integer',56))
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


library(dplyr)
datad <- "~/parseSTP/data/output8"
fname <- paste(datad,"Devices",sep = '/')
devs <- devread(fname,rows=-1)

# remove volumes with no disk I/O.
# ?? gatekeepers, tdevs? << Am I doing this backward >>?

n<-devs %>% group_by(device.name) %>% summarise(max(DA.Kbytes.transferred.per.sec))
# n2 <- n[n[2]>0,]
devs <- devs[ n[2] > 0, ]

meaniops <- devs %>% group_by(device.name) %>% summarise(avg = mean(total.ios.per.sec)) %>% arrange(avg)
# find the top 100 vols by total.ios.per.sec
meaniops <- meaniops[order(meaniops$avg,decreasing=TRUE),]
topvols <- head(meaniops$device.name,32)
par (mfrow=c(4,4))
for (vol in topvols) {
  tdf <- filter(devs,device.name==vol)
  x <- tdf$TimeStamp
  y <- tdf$total.ios.per.sec
  plot(x,y,ylab='iops',xlab='',main=paste('volume:',vol),cex=0.5,pch=19,col='blue')
  lines(supsmu(x,y),col='chocolate2',lwd=2)
  abline(h=mean(y),lty=2)
}
