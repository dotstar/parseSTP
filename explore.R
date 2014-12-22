library(ggplot2)
fe<-read.csv('Ports FE')
colnames(fe) <- c('ts','port','iops','KBps',"avgsz")
fe<-fe[fe$iops>20,]
hist(fe$iops,breaks=100)

# Average size is derived.  re-derive it.
fe$avgsz <- NULL  # Delete the old column
fe$avgsz <- fe$KBps / fe$iops
hist(fe$avgsz,breaks=64,col='green')

devices<-read.csv('Devices')

hist((devices$sampled.read.time.per.sec)[devices$sampled.read.time.per.sec<1000 & devices$sampled.read.time.per.sec>0 ],col='red',breaks=128)
hist((devices$sampled.write.time.per.sec)[devices$sampled.write.time.per.sec<1000 & devices$sampled.write.time.per.sec>0 ],col='blue',breaks=32)
