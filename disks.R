
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




## MAIN 
library(dplyr)

serialnum <- "HK19570227"

datad <- "~/parseSTP/data/output8"
datad <- "/media/cdd/Seagate Backup Plus Drive/MC Mainframe/MasterCard 2217/output"
fname <- paste(datad,"Disks",sep = '/')
d <- diskread(fname)

# Summaries
### Top SCSCI Commands Per Second
options(digits=1)
d$device = paste(d$device.name,d$spindle.ID,sep=":")
t <- d %>% group_by(device) %>% summarize(
  max_scsi = max(total.SCSI.command.per.sec),
  mean_scsi = mean(total.SCSI.command.per.sec)
) %>% arrange(desc(max_scsi))
topN <- head(t$device,18)
par(mfrow = c(2,2))
for (n in topN) {
  local <- filter(d,device==n)
  with (local, plot(TimeStamp,total.SCSI.command.per.sec,pch=19,cex=0.4,col='blue',main=n))
  with (local, lines(supsmu(TimeStamp,total.SCSI.command.per.sec),col='black',lwd=3) )
  with(local, abline(h=mean(total.SCSI.command.per.sec)))
}

### System Wide Write BW to Disk
par(mfrow = c(1,1))
# remove Spurious values
t <- d[d$Kbytes.written.per.sec<1e9,]
sums <- t %>% group_by(TimeStamp) %>% summarise ( MBps = sum(Kbytes.written.per.sec)/2^10)
with (t, plot(sums$TimeStamp,sums$MBps,pch=19,cex=0.3,col='red',main='Frame Disk BW - MBps') )
with (t, lines(supsmu(sums$TimeStamp,sums$MBps),pch=19,cex=0.3,col='blue',main='Frame Disk BW - MBps') )
with (t, abline(h=mean(sums$MBps),col='black',lty=2) )

## Disk I/O Sizes - Frame wide
par(mfrow = c(2,1)) 

rm.Inf <- function(x,var) {
  t<-x[!is.na(x[,var]),]   
  t <- t[t[,var] < 1e9,]  
  return (t)
}

t <- rm.Inf(d,'average.kbytes.per.read')
summary(t$average.kbytes.per.read)

hist(t$average.kbytes.per.read,col='red4',breaks=128,main=paste(serialnum,'Average kBytes/read'),xlab='' )
abline(v=mean(t$average.kbytes.per.read),col='black',lwd=3)


t<- rm.Inf(d,'average.kbytes.per.write')
hist(t$average.kbytes.per.write,col='blue4',breaks=128,main=paste(serialnum,'Average kBytes/write'),xlab='' )
abline(v=mean(t$average.kbytes.per.write),col='black',lwd=3)

