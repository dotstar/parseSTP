
sysread <- function(filename){
  
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

feread <- function(filename) {
  # open the Front End Directors file
  # and apply the derived calculations.

  fe<-read.table(filename,header=T,sep=",",stringsAsFactors=F)
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
          sub=paste('start time: ',from,'end time: ',to))

  boxplot(write~dir,data=t2,las=2,cex.axis=.4,
          horizontal=TRUE,
          ylab='Director FE',
          xlab='Service Time (mSec)',          
          col=c('green','blue','purple','red','orange'),
          main='Write Service Time by Front End Director',
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
  

  
  for (d in dirs) {
    cat ('processing graphs for director',d,'\n')
    
    # IOPS

    t1<-select(filter(times,director.number==d),TimeStamp,ios.per.sec)
    t1<- t1[complete.cases(t1),]
    x <- t1$TimeStamp
    y <- t1$ios.per.sec
    if (length(x) == 0 || length(y) ==0) {
      cat('detected empty data - skipping director',d)
      next
    }
    t <- paste('Front End ',d, 'IOPS')

    plot(x,y,ylab='I/O / Sec',cex.axis=0.75,col='firebrick4',main=t)
    lines(supsmu(x,y),col='chocolate4',lwd=3)
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

    # Read Service Time
    t1<-select(filter(times,director.number==d),TimeStamp,average.read.time.ms)
    t1 <-t1[complete.cases(t1),]
    t1 <- filter(t1,average.read.time.ms != Inf)
    x<-t1[[1]]
    y<-t1[[2]]
    plot(x,y,ylab='mSec',xlab='',col='gold',pch=19,cex.axis=0.75,main=(paste('Director:',d,'Read Service Time (mSec')))
    abline(h=mean(y),col='gold',lty=2)
    lines(supsmu(x,y),col='black',lwd=3)
    
    b<- 100
    hist(y,breaks=b,main=paste(d,'Read Service Time (mSec)'),ylab='Frequency',xlab='',col='gold')
    abline(v=mean(y),lty=2)

    # Write Service Time
    t1<-select(filter(times,director.number==d),TimeStamp,average.write.time.ms)
    t1 <-t1[complete.cases(t1),]
    t1 <- filter(t1,average.write.time.ms != Inf)
    x<-t1[[1]]
    y<-t1[[2]]
    plot(x,y,t=,ylab='mSec',xlab='',cex.axis=0.75,col='green4',pch=19,main=(paste('Director:',d,'Write mSec Service Time')))
    abline(h=mean(y),col='green4',lty=2)
    lines(supsmu(x,y),col='black',lwd=3)
    
    hist(y,breaks=b,main=paste(d,'Write Service Time (mSec)'),col='green4',ylab='Frequency',xlab='')        
    abline(v=mean(y),lty=2)
    # 8th plot - estmate queue lengths
    # Assume it is open systems for now.
    open <- c(0,5,10,20,40,80,160,320,640,1280)
    openmid <- as.vector(mode='numeric',open/2)
    
    # just work with this director
      d2 <- filter(df,director.number == d)
      q <- select(d2,average.queue.depth.range.0:average.queue.depth.range.9)
            
      addem <- function(v1,v2) { 
        # dot product of buckets to estimate the queue length
        # Remove NA from the calculation
        i <- complete.cases(as.vector(mode='numeric',v1)) 
        v1 <- v1[i]
        v2 <- v2[i]
        # Remove Inf values.
        i <- v1 != Inf
        v1 <- v1[i]
        v2 <- v2[i]
        sum(v1*v2) 
      }
      qlen <- apply(q,1,addem,openmid)
      plot(d2$TimeStamp,qlen,pch=19,col='purple',main=paste(d,'Estimate Queue Length'),xlab='',ylab='est queue length')
      hist(qlen,breaks=128,col='purple',main=paste(d,'Estimated Queue Length'))
      abline(v=mean(qlen),lty=2)
    
      # Fill the page with empty plots.
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    plot.new()
    
    } # End For
}

library(dplyr)
datad <- "~/parseSTP/data/output8"
fname <- paste(datad,"Directors FE",sep = '/')
fe <- feread(fname)

# pdf(file="Rplots.pdf",width=14,height=8.5)
# feboxplot(fe)
fedetails(fe)
# dev.off()
