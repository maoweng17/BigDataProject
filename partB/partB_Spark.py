# System Setting ==============================================================
isLocal = False
if isLocal:
    dir = "/Users/maoweng17/Documents/QMUL/BigDataProcessing/Lab/coursework"
    vout_path = dir + "/input/voutSample.csv"
    vin_path = dir + "/input/vinSample.csv"
else:
    dir = "hdfs://studoop.eecs.qmul.ac.uk/data/bitcoin"
    vout_path = dir + "/vout.csv"
    vin_path = dir + "/vin.csv"



# Start python ================================================================
import pyspark
import re

sc = pyspark.SparkContext()

# Define functions that will be used------------------------------------------
def is_in_wallet(line):
    fields = line.split(',')
    publicKey = fields[3].strip()
    wallet = "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
    if (publicKey == wallet):
        return True
    else:
        return False

def is_receiver_trans(line):
    tx_id = line.split(',')[0]
    if tx_id in filtered_hash.value :
        return True
    else:
        return False
    filtered_hash.unpersist()

def is_sender_trans(line):
    fields = line.split(',')
    hash = fields[0]
    n = fields[2]
    if (hash,n) in first_join.value :
        return True
    else:
        return False
    first_join.unpersist()



# Initial Filtering: filter rows by wallet, get associated transaction ID------
linesVout = sc.textFile(vout_path) \
              .filter(is_in_wallet)
filtered_hash = linesVout.map(lambda line: line.split(',')[0]).collect()
filtered_hash = sc.broadcast(filtered_hash)

del linesVout


# First Join: filter rows by transaction(tx_id),get tx_hash & vout ------------
linesVin = sc.textFile(vin_path) \
             .filter(is_receiver_trans)
first_join = linesVin.map(lambda l: (l.split(',')[1], l.split(',')[2])).collect()
first_join = sc.broadcast(first_join)

del linesVin


# Second Join: filter rows by tx_hash & vout,get (wallet, value) --------------
linesVout = sc.textFile(vout_path) \
              .filter(is_sender_trans)
features = linesVout.map(lambda l: (l.split(',')[3].strip(),float(l.split(',')[1])))

del linesVout


# Top ten: sort by value(money), get top 10 wallet and value ------------------
top10 = features.reduceByKey(lambda a, b: a + b).takeOrdered(10, key = lambda x: -x[1])
for record in top10:
    print((record[0],record[1]))
