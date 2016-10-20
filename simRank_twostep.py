#!/usr/bin/env python
#
# Author: cao4@illinois.edu (Liangliang Cao)
#         mtsai2@illinois.edu (Min-Hsuan Tsai)
#         zhenli3@illinois.edu (Zhen Li)
# Beckman Institute, University of Illinois, 2010-2011
#
import sys

C = 0.4

def load_adj(fn_adj = 'adj.txt'):
    file = open(fn_adj, "r")
    adj = {}
    for ln in file:
        ele = ln.split()
        if len(ele) > 1:
            adj[ele[0]] = ele[1:]
        else:
            adj[ele[0]] = []
    file.close()

    file = open("inLen.txt", "r")
    leng = {}
    for ln in file:
        ele = ln.split()
        leng[ele[0]] = float(ele[1])
    file.close()

    return adj, leng
    
def get_adj():
    adj,len = load_adj("adj.txt")
    return adj, len
    
class Mapper_simRank_step1:
    def __init__(self):
        '''
        file = open("excludes.txt", "r")
        self.excludes = set(line.strip() for line in file)
        file.close()
        '''
        # init adjacent matrix
        self.adj, self.len = get_adj()


    def __call__(self, key, value):
        # for each node i, distribute s(i,j) to all neighbors

        ii, jj, simstr = value.split()

        #if ii == jj:
        #    yield [ii,jj], 1.0

        simij = float(simstr)

        for b in self.adj[jj]:
            yield [ii,b], simij

class Mapper_simRank_step2:

    def __init__(self):
        # init adjacent matrix
         self.adj, self.len = get_adj()

    def __call__(self, key, value):
        # for each node i, distribute s(i,j) to all neighbors

        ii, jj, simstr = value.split()


        simij = float(simstr)

        for a in self.adj[ii]:
            yield [a,jj], simij

class Reducer_simRank_step1:

    # for each node_pair (i,j), sum all the score together
    def __call__(self, key, values):

        a,b = key


        s = 0.0
        for v in values:
            s += float(v)

        yield 'sim', (a +' ' +b + ' ' + str(s))


class Reducer_simRank_step2:
    def __init__(self):
        # init adjacent matrix
         self.adj, self.len = get_adj()


    def __call__(self, key, values):

        a,b = key
        if a == b:
            yield 'sim', (a +' ' +b + ' ' + '1.0' )
            return;

        s = 0.0
        for v in values:
            s += float(v)


        sim_new = s/self.len[a]/self.len[b]*C;
        yield 'sim', (a +' ' +b + ' ' + str(sim_new))



if __name__ == "__main__":
    import dumbo
    job = dumbo.Job()
    for iter in range(3):
        job.additer(Mapper_simRank_step1, Reducer_simRank_step1)
        job.additer(Mapper_simRank_step2, Reducer_simRank_step2)
    job.run()



