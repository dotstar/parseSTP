library(ggplot2)
fe<-read.csv('Ports FE')
colnames(fe) <- c('ts','port','iops','KBps',"avgsz")
fe<-fe[fe$iops>20,]
hist(fe$iops,breaks=100)

# Directors FE
# Average size is derived.  re-derive it.
fe$avgsz <- NULL  # Delete the old column
fe$avgsz <- fe$KBps / fe$iops
# Where are these negative sizes coming from ??
fe<-fe[fe$avgsz>0,]
hist(fe$avgsz,breaks=64,col='green')

# Ports FE
fp<-read.csv('Ports FE')
colnames(fp) <- c('ts','port','iops','KBps','avgsz')
# if IOPS is 0 there probably isn't anything connected to that port
fp<-fp[fp$iop>0,]
fp$avgsz <- NULL
fp$avgsz <- fp$KBps / fp$iops
fp<-fp[fp$avgsz>0,]
hist(fp$avgsz,breaks=64,col='purple')

# System
system <- read.csv('System')
hist(system$number.write.pending.tracks,breaks=64,col='red')
qplot(Timestamp,system$ios.per.sec)
m<-qplot(ios.per.sec,data=system,geom="histogram",binwidth=1000)
m + geom_histogram(aes(fill = ..count..))

devices<-read.csv('Devices')

devices$sampled.average.read.time..ms. <- NULL
devices$sampled.average.write.time..ms. <- NULL
devices$sampled.average.read.miss.time..ms. <- NULL
devices$sampled.average.WP.disconnect.time..ms. <- NULL
devices$sampled.average.RDF.write.time..ms. <- NULL
devices$average.DA.req.time..ms. <- NULL
devices$average.DA.disk.time..ms. <- NULL
devices$average.DA.task.time..ms. <- NULL
devices$total.reads.per.sec <- NULL
devices$total.read.hits.per.sec <- NULL
devices$total.read.misses.per.sec <- NULL
devices$total.writes.per.sec <- NULL
devices$total.ios.per.sec <- NULL
devices$total.hits.per.sec <- NULL
devices$total.misses.per.sec <- NULL
devices$total.write.misses.per.sec <- NULL
devices$total.seq.ios.per.sec <- NULL
devices$random.read.misses.per.sec <- NULL
devices$seq.read.misses.per.sec <- NULL
devices$random.write.misses.per.sec <- NULL
devices$seq.write.misses.per.sec <- NULL
devices$X.random.read.hit <- NULL
devices$X.random.read.miss <- NULL
devices$X.sequential.read <- NULL
devices$X.write <- NULL
devices$X.read <- NULL
devices$X.hit <- NULL
devices$X.miss <- NULL
devices$X.read.hit <- NULL
devices$X.write.hit <- NULL
devices$X.read.miss <- NULL
devices$X.write.miss <- NULL
devices$X.sequential.io <- NULL
devices$X.sequential.writes <- NULL
devices$HA.Kbytes.transferred.per.sec <- NULL
devices$average.read.size.in.Kbytes <- NULL
devices$average.write.size.in.Kbytes <- NULL
devices$average.io.size.in.Kbytes <- NULL
devices$DA.Kbytes.transferred.per.sec <- NULL
devices$X.read.hit.of.ios <- NULL
devices$X.read.miss.of.ios <- NULL
devices$X.write.hit.of.ios <- NULL
devices$X.write.miss.of.ios <- NULL
devices$device.capacity.in..MB. <- NULL

devices$total.reads.per.sec <- devices$random.reads.per.sec + devices$seq.reads.per.sec

devices$sampled.average.read.time..ms. <- devices$sampled.read.time.per.sec / devices$sampled.reads.per.sec
devices$sampled.average.write.time..ms. <- devices$sampled.write.time.per.sec / devices$sampled.writes.per.sec
devices$sampled.average.read.miss.time..ms. <- devices$sampled.read.miss.time.per.sec / devices$sampled.read.miss.waits.per.sec
devices$sampled.average.WP.disconnect.time..ms. <- devices$sampled.WP.disconnect.time.per.sec / devices$sampled.WP.disconnects.per.sec
devices$sampled.average.RDF.write.time..ms. <- devices$sampled.RDF.write.time.per.sec / devices$sampled.RDF.write.waits.per.sec
devices$average.DA.req.time..ms. <- devices$DA.req.time.per.sec / devices$DA.read.requests.per.sec
devices$average.DA.disk.time..ms. <- devices$DA.disk.time.per.sec / devices$DA.read.requests.per.sec
devices$average.DA.task.time..ms. <- devices$DA.task.time.per.sec / devices$DA.read.requests.per.sec
devices$total.reads.per.sec <- devices$random.reads.per.sec + devices$seq.reads.per.sec
devices$total.read.hits.per.sec <- devices$random.read.hits.per.sec + devices$seq.read.hits.per.sec
devices$total.read.misses.per.sec <- devices$total.reads.per.sec - devices$total.read.hits.per.sec
devices$total.writes.per.sec <- devices$random.writes.per.sec + devices$seq.writes.per.sec
devices$total.ios.per.sec <- devices$total.reads.per.sec + devices$total.writes.per.sec
devices$total.hits.per.sec <- devices$total.read.hits.per.sec + devices$random.write.hits.per.sec + devices$seq.write.hits.per.sec
devices$total.misses.per.sec <- devices$total.ios.per.sec - devices$total.hits.per.sec
devices$total.write.misses.per.sec <- devices$total.writes.per.sec - devices$random.write.hits.per.sec - devices$seq.write.hits.per.sec
devices$total.seq.ios.per.sec <- devices$seq.reads.per.sec + devices$seq.writes.per.sec
devices$random.read.misses.per.sec <- devices$random.reads.per.sec - devices$random.read.hits.per.sec
devices$seq.read.misses.per.sec <- devices$seq.reads.per.sec - devices$seq.read.hits.per.sec
devices$random.write.misses.per.sec <- devices$random.writes.per.sec - devices$random.write.hits.per.sec
devices$seq.write.misses.per.sec <- devices$seq.writes.per.sec - devices$seq.write.hits.per.sec
devices$X.random.read.hit <- 100 * devices$random.read.hits.per.sec / devices$total.ios.per.sec
devices$X.random.read.miss <- 100 * devices$random.read.misses.per.sec / devices$total.ios.per.sec
devices$X.sequential.read <- 100 * devices$seq.reads.per.sec / devices$total.ios.per.sec
devices$X.write <- 100 * (devices$total.writes.per.sec / devices$total.ios.per.sec)
devices$X.read <- 100 * (devices$total.reads.per.sec / devices$total.ios.per.sec)
devices$X.hit <- 100 * (devices$total.hits.per.sec / devices$total.ios.per.sec)
devices$X.miss <- 100 - devices$X.hit
devices$X.read.hit <- 100 * (devices$total.read.hits.per.sec / devices$total.reads.per.sec)
devices$X.write.hit <- 100 * ((devices$random.write.hits.per.sec + devices$seq.write.hits.per.sec) / devices$total.writes.per.sec)
devices$X.read.miss <- 100 * (devices$total.read.misses.per.sec / devices$total.reads.per.sec)
devices$X.write.miss <- 100 * (devices$total.write.misses.per.sec / devices$total.writes.per.sec)
devices$X.sequential.io <- 100 * (devices$total.seq.ios.per.sec / devices$total.ios.per.sec)
devices$X.sequential.writes <- 100*(devices$seq.writes.per.sec / devices$total.ios.per.sec)
devices$HA.Kbytes.transferred.per.sec <- devices$Kbytes.read.per.sec + devices$Kbytes.written.per.sec
devices$average.read.size.in.Kbytes <- devices$Kbytes.read.per.sec / devices$total.reads.per.sec
devices$average.write.size.in.Kbytes <- devices$Kbytes.written.per.sec / devices$total.writes.per.sec
devices$average.io.size.in.Kbytes <- devices$HA.Kbytes.transferred.per.sec / devices$total.ios.per.sec
devices$DA.Kbytes.transferred.per.sec <- devices$DA.Kbytes.read.per.sec + devices$DA.Kbytes.written.per.sec
devices$X.read.hit.of.ios <- 100 * (devices$total.read.hits.per.sec / devices$total.ios.per.sec)
devices$X.read.miss.of.ios <- 100 * (devices$total.read.misses.per.sec / devices$total.ios.per.sec)
devices$X.write.hit.of.ios <- 100 * ((devices$random.write.hits.per.sec + devices$seq.write.hits.per.sec) / devices$total.ios.per.sec)
devices$X.write.miss.of.ios <- 100 * (devices$total.write.misses.per.sec / devices$total.ios.per.sec)
devices$device.capacity.in.(MB) <- devices$device.capacity * devices$device.block.size / 1024 / 1024



# Response times > 0 and < 20,000 mSec
d<-devices$sampled.read.time.per.sec[devices$sampled.read.time.per.sec>0&devices$sampled.read.time.per.sec<20000]
hist(d,breaks=256)

# Which devices have a response time > 10 seconds?
longTimeDevs <- devices[devices$sampled.read.time.per.sec>10000,]
slowDevices <- sort(unique(longTimeDevs[longTimeDevs[longTimeDevs$device.name,2]]))


hist((devices$sampled.read.time.per.sec)[devices$sampled.read.time.per.sec<1000 & devices$sampled.read.time.per.sec>0 ],col='red',breaks=128)
hist((devices$sampled.write.time.per.sec)[devices$sampled.write.time.per.sec<1000 & devices$sampled.write.time.per.sec>0 ],col='blue',breaks=32)
