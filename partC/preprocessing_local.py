'''
#Transactions.csv
[0]tx_hash,[1]blockhash,[2]time,[3]tx_in_count,[4]tx_out_count
#blocks.csv
[0]height,[1]hash,[2]time,[3]difficulty
#vout.csv
[0]hash,[1]value,[2]n,[3]publicKey
'''
# System Setting ==============================================================
isLocal = True
if isLocal:
    dir = "/Users/maoweng17/Documents/QMUL/BigDataProcessing/Lab/coursework/partC"
    trans_path = dir + "/input/transactionsSample.csv"
    vout_path = dir + "/input/voutSample.csv"
    vin_path = dir + "/input/vinSample.csv"
    price_path = dir + "/input/market_price.csv"
    #output_path = dir + "/output.csv"
else:
    dir = "hdfs://studoop.eecs.qmul.ac.uk/data/bitcoin"
    trans_path = dir + "transactions.csv"
    vout_path = dir + "/vout.csv"
    vin_path = dir + "/vin.csv"



# Start python ================================================================
import pyspark
import re
import time
import itertools
from itertools import chain
import collections


sc = pyspark.SparkContext()
# Get (total_bit, number of transaction ) per day =============================
# Get (tx_hash,time) from transactions.csv -------------------------------
linesTrans = sc.textFile(trans_path) \
               .map(lambda line:(line.split(',')[0],time.strftime("%Y-%m-%d",time.gmtime(int(line.split(',')[2])))))

# Get (hash,value) from vout.csv -----------------------------------------
linesVout = sc.textFile(vout_path) \
              .map(lambda line: (line.split(',')[0],float(line.split(',')[1])))
linesVout = linesVout.partitionBy(linesVout.getNumPartitions()) \
                     .persist()
# merge table and get results --------------------------------------------
merged = linesTrans.join(linesVout) \
                   .map(lambda x: x[1]) \
                   .groupByKey() \
                   .mapValues(lambda x: (sum(x),len(x))) # (total_btc, n_trans)

del linesTrans
del linesVout
# merge with price ============================================================
# (time,price,total_btc,n_trans)
linesPrice = sc.textFile(price_path) \
               .map(lambda line: (line.split(',')[0],float(line.split(',')[1]))) \
               .join(merged) \
               .map(lambda l: (l[0],l[1][0],l[1][1][0],l[1][1][1]))

linesPrice.saveAsTextFile("output")
