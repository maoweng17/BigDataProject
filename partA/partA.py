"""
Create a bar plot showing the number of transactions which occurred every month
"""
#Transactions.csv
#[0]tx_hash, [1]blockhash, [2]time, [3]tx_in_count, [4]tx_out_count

from mrjob.job import MRJob
import re
import time


#This line declares the class Lab1, that extends the MRJob format.
class partA(MRJob):
# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if (len(fields)==5):
                time_epoch = int(fields[2])
                date = time.strftime("%Y-%m",time.gmtime(time_epoch))
                yield (date, 1)
        except:
            pass

    def combiner(self, date, counts):
        yield (date, sum(counts))

    def reducer(self, date, counts):
        yield (date, sum(counts))

if __name__ == '__main__':
    partA.run()
