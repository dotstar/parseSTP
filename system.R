
filename <- 'System'
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
sys <- sysread("System")

sys <- sys %>% filter(!ios.per.sec>1000000) %>% arrange(TimeStamp)

plot(sys$TimeStamp,sys$ios.per.sec,col='darkblue',typ='l')
lines(sys$TimeStamp,smooth(sys$ios.per.sec),typ='l')



plot(sys$TimeStamp,sys$TimeStamp,typ='l')
plot(sys$TimeStamp,sys$ios.per.sec,type='l',col='darkblue',xlab='IOPS',lwd='2')


lines(supsmu(sys$TimeStamp,sys$ios.per.sec),lwd=4)
abline(h=mean(sys$ios.per.sec),col='grey',lwd=2,lty=4)



