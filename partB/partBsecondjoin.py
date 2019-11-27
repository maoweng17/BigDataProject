from mrjob.step import MRStep
from mrjob.job import MRJob
import re

import collections

def total(records):
    dct = collections.defaultdict(int)
    for cust_id, contrib in records:
        dct[cust_id] += contrib

    return dct.items()


class partB_secondjoin(MRJob):

    def steps(self):
           return [MRStep(mapper_init=self.mapper_join_init,
                           mapper=self.mapper_repl_join,
                           reducer=self.reducer_sum),
                   MRStep(mapper = self.mapper,
                           reducer=self.reducer_top)]

    sector_table = {}

    def mapper_join_init(self):
        with open("out_partBfirstjoin.txt") as f:
            for line in f:
                fields = line.split("\t")
                tx_hash = fields[0].replace('"','')
                vout = fields[1].replace('"','').strip()
                self.sector_table[(tx_hash,vout)] = ''


    def mapper_repl_join(self, _, line):
        fields = line.split(",")
        hash = fields[0]
        value = fields[1]
        n = fields[2]
        publicKey = fields[3]#.strip()
        if (hash,n) in self.sector_table:
            yield(publicKey,float(value))

    def reducer_sum(self, publicKey, value):
        yield('',(publicKey,sum(value)))

    def mapper(self, feature, value):
        yield('',value)

    def reducer_top(self,feature,pair):
        sorted_values = sorted(pair, reverse = True, key = lambda pair:pair[1])[:10]
        for value in sorted_values:
             yield (value[0],value[1])


if __name__ == '__main__':
    partB_secondjoin.JOBCONF = {'mapreduce.reduce.memory.mb': 4096 }
    partB_secondjoin.run()
