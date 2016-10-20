#!/usr/bin/env python
#
# Author: mtsai2@illinois.edu (Min-Hsuan Tsai)
# Beckman Institute, University of Illinois, 2010-2011
#
import sys

class Mapper_combine:
    def __call__(self, key, value):
       	
	style, ii, jj, simstr = value.split()
        
	yield [ii,jj], simstr


class Reducer_combine:
    def __call__(self, key, values):
        a,b = key
        if a == b:
            yield '', (a + ' ' + b + ' 1')
            return

        sim_delta = 0.0
        for v in values: #.split()
            sim_delta += float(v)

        yield '', (a +' ' +b + ' ' + str(sim_delta))

if __name__ == "__main__":
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper_combine, Reducer_combine)
    job.run()
