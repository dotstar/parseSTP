#!/usr/bin/env python

import multiprocessing as mp

import time
import os


def f(name,sleeptime):
   print "PID:",os.getpid()
   print "running f"
   print 'hello', name
   print 'sleeping',sleeptime,'seconds'
   time.sleep(sleeptime)

   print 'goodbye', name
   return sleeptime




# Main

if __name__ == "__main__":

   # Put the names on the queue
   names = ('None', 'Bob', 'Ted', 'Carol', 'Alice', 'Fred', 'Wilma', 'Barney', 'Mrs Barney', 'Hawkeye', 'Trapper', 'BJ')


   results = []
   pool = mp.Pool(processes=20)
   for i in range(len(names)):
      print 'submitting job:',i
      r = pool.apply_async(f,args=(names[i],i))
      results.append(r)

   for r in results:
      print '\t',r.get()

   print "complete"