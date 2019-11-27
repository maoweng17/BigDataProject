
from mrjob.job import MRJob
import re



class partBfilter(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        hash = fields[0]
        publicKey = fields[3].strip()
        if (publicKey =="{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"):
            yield(hash,publicKey)

if __name__ == '__main__':
    #partBfilter.JOBCONF = {'mapreduce.reduce.memory.mb': 4096 }
    partBfilter.run()
