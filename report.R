
source ('cddttp.R')
source ('multiplot.R')
library(dplyr)
library(ggplot2)
library('XML')
library('data.table')
library('bit64')
library(grid)

systemReports <- function(fname) {
   sys <- sysread(fname)

   # 1,000,000 IOPS is unreasonable and bad data.
   threshold = 1e6
   bad <- count(filter(sys,ios.per.sec>threshold))
   if (bad > 0) {
     print(paste('warning:',bad,'extreme data points delete.'))
     sys <- sys %>% filter(!ios.per.sec>1e6) %>% arrange(TimeStamp)  
   }
   begin <- min(sys$TimeStamp)
   end <- max(sys$TimeStamp)
   # plot(sys$TimeStamp,sys$TimeStamp,typ='p',pch=19,pex=.1)
   
   dev.off()
   par(cex=0.4,mar=c(3,3,3,1),mfrow=c(2,1))
   
   
   # IOPS
   plot(sys$TimeStamp,sys$ios.per.sec,type='p',pch=19,cex.main=.8,cex=.5,col='darkblue',lwd='2',
        main=paste(serialnum,'IOPS',"\nstart:",begin,"   stop:",end),xlab='',ylab='',xaxt='n')
   grid(nx = 10, ny = 6, col = "lightgray", lty = "dotted", lwd = par("lwd"), equilogs = FALSE)
   axis.POSIXct(1, at=seq(begin,end,by="hour"), cex.axis=0.6,format="%a\n%R\n%D",col.ticks='darkblue') #label the x axis
   lines(supsmu(sys$TimeStamp,sys$ios.per.sec),lwd=4)
   abline(h=mean(sys$ios.per.sec),col='red3',lwd=2,lty=4)
   abline(h=quantile(sys$ios.per.sec,c(0.98)),col='red',lwd=1,lty=2)
   
   # BW
# Change from kBytes/Sec to MBytes/Sec
sys$Mbytes.transferred.per.sec = sys$Kbytes.transferred.per.sec/2^10
plot(sys$TimeStamp,sys$Mbytes.transferred.per.sec,type='p',pch=19,cex.main=.8,cex=.5,col='darkgreen',lwd='2',
     main=paste(serialnum,'MBytes/sec',"\nstart:",begin,"    stop:",end),xlab='',ylab='',xaxt='n')
grid(nx = 10, ny = 6, col = "lightgray", lty = "dotted", lwd = par("lwd"), equilogs = FALSE)
axis.POSIXct(1, at=seq(begin,end,by="hour"), cex.axis=0.6,format="%a\n%R\n%D") #label the x axis
lines(supsmu(sys$TimeStamp,sys$Mbytes.transferred.per.sec),lwd=4)
abline(h=mean(sys$Mbytes.transferred.per.sec),col='red3',lwd=2,lty=4)
abline(h=quantile(sys$Mbytes.transferred.per.sec,c(0.98)),col='red',lwd=1,lty=2)


}

devReports <- function (fname,sgfile,count=-1) {
  
  # File with storage group information in XML format
  # from symsg list -v --output XML
#   xdir <- '/media/cdd/Seagate Backup Plus Drive/HK192602527_VMAX'
#   xdir <- '/data/HK192602527'
#   sgfile <- paste(xdir,'sg_2527.xml',sep='/')
#   
#   # Input stats as parsed from TTP file
#   datad <- '/data/new'
#   fname <- paste(datad,"Devices",sep = '/')
  
  print('reading ttp data for devices')

  devs <- DevRead(fname,rows=count)
  
  print('reading Storage Group XML')
  stgroups <-parseSG(sgfile)
  serialnumber <- as.character(unique(stgroups$symid))
  
  print('joining storage group names into devices')
  devs <- merge(devs,stgroups,by='device.name')
  devs$LongLabel <- paste(devs$device.name,devs$sgname)
  # rm(stgroups)
  
  print('adding factors to devs')
  # Add a factor to each LUN, based on IO Size
  # Try dividing into quartiles.
  sz <- select(devs,TimeStamp,device.name,average.io.size.in.Kbytes)
  sz <- filter(sz,! is.na(average.io.size.in.Kbytes), average.io.size.in.Kbytes != Inf)
  cutpoints <- quantile(sz$average.io.size.in.Kbytes,seq(0,1,length=5),na.rm=TRUE)
  devs$sz <- cut(devs$average.io.size.in.Kbytes,cutpoints)
  
  print('dev reports ...')
  
  
  # TOP N by IOPS
  
  plots=9
  plotsperpage=9
  columns=3
  
  # x<-select(devs,total.ios.per.sec,HA.Kbytes.transferred.per.sec,average.io.size.in.Kbytes)
  # setnames(x,c('iops','kBps','kBytes'))
  # fit<- lm(iops ~kBps + kBytes,data=x)
  
  topNIOPS(devs,n=8)
  topNBW(devs,n=8)
  
  
  topLuns(devs,v='total.ios.per.sec',plots=plots,plotsperpage=plotsperpage, columns=columns)
  topLuns(devs,v='total.reads.per.sec',plots=plots,plotsperpage, columns)
  topLuns(devs,v='total.writes.per.sec',plots,plotsperpage, columns)
  topLuns(devs,v='Kbytes.read.per.sec',plots,plotsperpage, columns)
  topLuns(devs,v='Kbytes.written.per.sec',plots,plotsperpage, columns)
  # topLuns(devs,v='write.pending.count',plots,plotsperpage, columns)
  topLuns(devs,v='sampled.write.time.per.sec',plots,plotsperpage, columns)
  #topLuns(devs,v='rdf.copy.Kbytes.per.sec',plots,plotsperpage, columns)
  # topLuns(devs,v='average.read.size.in.Kbytes',plots,plotsperpage, columns)
  # topLuns(devs,v='average.write.size.in.Kbytes',plots,plotsperpage, columns)
  # topLuns(devs,v='average.io.size.in.Kbytes',plots,plotsperpage, columns)
  
  # read.time seems to always have outliers
  ndevs <- devs[ devs$sampled.read.time.per.sec < 1e6,]
  topLuns(ndevs,v='sampled.read.time.per.sec',plots,plotsperpage, columns)
  topLuns(ndevs,v='sampled.write.time.per.sec',plots,plotsperpage, columns)
  
  ## Just the stats
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
}

feReports <- function (fname) {
  
  fe <- feread(fname,nrows=-1)  
  feboxplot(fe)
  fedetails(fe)
#   # summarize front end stats in table
#   st<-fe %>% 
#     group_by(director.number)  %>%
#     summarize(
#       mean_wrt= mean(average.write.time.ms),
#       min_wrt= min(average.write.time.ms),
#       max_wrt= max(average.write.time.ms),
#       sd_wrt= sd(average.write.time.ms)
#     ) %>%
#     arrange(desc(mean_wrt))
#   
  # Density plots
  dirs <- getdirs(fe)
  
  ff <- select(fe,director.number,average.write.time.ms,average.read.time.ms)
  fr <- filter(ff,is.finite(average.read.time.ms))
  fw <- filter(ff,is.finite(average.write.time.ms))
  
  
  # Where we have data, plot the histogram of the read service time
  par(mfrow=c(4,4))
  # nf <- layout(mat = matrix(c(1,2),2,1, byrow=TRUE),  height = c(3,1))
  # par(mar=c(3.1, 3.1, 1.1, 2.1))
  for (dir in dirs) {
    d <- filter(fr, director.number == dir )
    # Reads
    x <- d$average.read.time.ms
    x <- x[is.finite(x)]
    if ( length(x)>0 ) {
      # Find everything about the 99th quantile and set = 99th quantile
      topvalue <- as.numeric(quantile(x,c(0.99)))
      x[x > topvalue] <- topvalue 
      n <-  topvalue
      breaks = 128
      
      xmin <- min(x)
      xmax <- max(x)
    
      main=paste(dir,'read svc time mSec')
      hist(x,breaks,xlim<-c(xmin,xmax),add=F,main=main,xlab='')
      abline(,v=mean(x),col='blue3',lwd=2)
    } # end if
  
    # Writes
    d <- filter(fw, director.number == dir )
    x <- d$average.write.time.ms
    # Find everything about the 99th quantile and set = 99th quantile
    topvalue <- as.numeric(quantile(x,c(0.99)))
    x[x > topvalue] <- topvalue 
    n <-  topvalue
    hist(x,main=paste(dir,'write svc time mSec'),xlab='mSec',breaks)
    abline(,v=mean(x),col='green3',lwd=2) 
  }
}

diskReports <- function (fname) {
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

}

serialnum <- "HK19570227"
dir <- "/data/HK192602527/output"
sgfile <- '/data/HK192602527/sg_2527.xml'

sysfile <- paste(dir,'System',sep='/')     # Devices
devfile <- paste(dir,'Devices',sep='/')     # Devices
fefile <- paste(dir,'Directors FE',sep='/') # Front End Directors
diskfile <- paste(dir,'Disks',sep='/')

systemReports(sysfile)

# devReports(devfile,sgfile,count=-1)
# feReports(fefile)
# diskReports(diskfile)