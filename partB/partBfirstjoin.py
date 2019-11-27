from mrjob.step import MRStep
from mrjob.job import MRJob
import re



class partBfirstjoin(MRJob):

    def steps(self):
           return [MRStep(mapper_init=self.mapper_join_init,
                           mapper=self.mapper_repl_join)]

    sector_table = {}
    def mapper_join_init(self):
        # load vout into a dictionary
        #with open("/homes/cmw30/Documents/BigData/coursework/input/vinSample.csv") as f:
        with open("out_partBfilter.txt") as f:
            for line in f:
                fields = line.split("\t")
                hash = fields[0].replace('"','')
                #value = fields[1]
                #n = fields[2]
                #publicKey = fields[1].strip()
                #if (publicKey =="{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"):
                self.sector_table[hash] = ''#[value,n]

    def mapper_repl_join(self, _, line):
        fields = line.split(",")
        if len(fields) == 3:
            txid = fields[0]
            tx_hash = fields[1]
            vout = fields[2]
            if txid in self.sector_table:
                yield(tx_hash,vout)


if __name__ == '__main__':
    partBfirstjoin.JOBCONF = {'mapreduce.reduce.memory.mb': 4096 }
    partBfirstjoin.run()
