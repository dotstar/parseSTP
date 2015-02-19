

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
sys <- sysread("/data/new/System")
serialnum = 'HK192602527'
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
par(cex=0.4,mar=c(4,4,8,1),mfrow=c(2,1))


# IOPS
plot(sys$TimeStamp,sys$ios.per.sec,type='p',pch=19,cex.main=.8,cex=.5,col='darkblue',lwd='2',
     main=paste(serialnum,'IOPS',"\nstart:",begin,"\nend:",end),xlab='',ylab='',xaxt='n')
grid(nx = 10, ny = 6, col = "lightgray", lty = "dotted", lwd = par("lwd"), equilogs = FALSE)
axis.POSIXct(1, at=seq(begin,end,by="hour"), cex.axis=0.6,format="%a\n%R\n%D",col.ticks='darkblue') #label the x axis
lines(supsmu(sys$TimeStamp,sys$ios.per.sec),lwd=4)
abline(h=mean(sys$ios.per.sec),col='red3',lwd=2,lty=4)
abline(h=quantile(sys$ios.per.sec,c(0.98)),col='red',lwd=1,lty=2)

# BW
# Change from kBytes/Sec to MBytes/Sec
sys$Mbytes.transferred.per.sec = sys$Kbytes.transferred.per.sec/2^10
plot(sys$TimeStamp,sys$Mbytes.transferred.per.sec,type='p',pch=19,cex.main=.8,cex=.5,col='darkgreen',lwd='2',
     main=paste(serialnum,'MBytes/sec',"\nstart:",begin,"\nend:",end),xlab='',ylab='',xaxt='n')
grid(nx = 10, ny = 6, col = "lightgray", lty = "dotted", lwd = par("lwd"), equilogs = FALSE)
axis.POSIXct(1, at=seq(begin,end,by="hour"), cex.axis=0.6,format="%a\n%R\n%D") #label the x axis
lines(supsmu(sys$TimeStamp,sys$Mbytes.transferred.per.sec),lwd=4)
abline(h=mean(sys$Mbytes.transferred.per.sec),col='red3',lwd=2,lty=4)
abline(h=quantile(sys$Mbytes.transferred.per.sec,c(0.98)),col='red',lwd=1,lty=2)


