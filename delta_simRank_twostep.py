#!/usr/bin/env python
#
# Author: cao4@illinois.edu (Liangliang Cao)
#         mtsai2@illinois.edu (Min-Hsuan Tsai)
#         zhenli3@illinois.edu (Zhen Li)
# Beckman Institute, University of Illinois, 2010-2011
#
from simRank_twostep import *

ITER = 5;
THRESH = 1e-4

class Mapper_init:

    def __init__(self):
        # init adjacent matrix
        self.adj, self.len = get_adj()


    def __call__(self, key, value):
        # for each node i, distribute s(i,j) to all neighbors
        stype, ii, jj, simstr = value.split()

        #if ii == jj:
        #    yield [ii,jj], 1.0

        simij = float(simstr)

        for b in self.adj[jj]:
            yield [ii,b], simij


class Mapper_delta_simRank_step1:
    def __init__(self):
       self.adj, self.len = get_adj()

    def __call__(self, key, value):

        ii, jj,  deltastr = value.split()        
        deltaij = float(deltastr)

        # if delta is too small, no need to distribute
        if (ii == jj) or (deltaij < THRESH):
            return

        # distribute \delta 
        for b in self.adj[jj]:
            yield [ii,b], deltaij

class Mapper_delta_simRank_step2:
    def __init__(self):
       self.adj, self.len = get_adj()

    def __call__(self, key, value):

        ii, jj, simstr = value.split()
        deltaij = float(simstr)

        for a in self.adj[ii]:
            yield [a,jj], deltaij
        

class Reducer_delta_simRank:
    # different from Reducer_simRank_step2:
    #   if a==b, no yield
    
    def __init__(self):
        self.adj,self.len = get_adj()

    def __call__(self, key, values):

        a,b = key
        if a == b:            
            return;

        # accumulate
        sim_delta = 0.0
        for v in values:
            sim_delta += float(v)           

        # generate delta
        if sim_delta != 0:
            sim_delta *= C /self.len[a]/self.len[b]        
            yield 's', ( a +' ' +b + ' '  + str(sim_delta))


if __name__ == "__main__":
    import dumbo
    job = dumbo.Job()
   
    job.additer(Mapper_simRank_step1, Reducer_simRank_step1)
    job.additer(Mapper_simRank_step2, Reducer_delta_simRank)
    
    for it in range(ITER):
        job.additer(Mapper_delta_simRank_step1, Reducer_simRank_step1)
        job.additer(Mapper_simRank_step2, Reducer_delta_simRank)

    job.run()



