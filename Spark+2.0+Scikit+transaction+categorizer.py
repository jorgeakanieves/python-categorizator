import codecs
import json
import re
import pickle
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import cloud
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.classification import NaiveBayes 
from pyspark.mllib.regression import LabeledPoint

from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest, RandomForestModel
import logging
import json


conf = SparkConf().setAppName("CategorApp")

sc = SparkContext(conf=conf)
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger('=================>APP CATEGORIZER<================')

ssc = StreamingContext(sc, 3)

zkQuorum = "zookeeper:2181"
topic = "transactions"
dsTrans = KafkaUtils.createStream(ssc, zkQuorum, "categorizator-consumer", {topic: 1})

dsTransJson = dsTrans.map(lambda (k, v): json.loads(v))

# ## Load scikit-model
scikit_model = pickle.load(open("model.p","rb"))

# ## Predict
key = "categorization".decode('utf-8')
dsTransUpd = dsTransJson.map(lambda x: (dict(x.items() + dict({key:scikit_model.predict([x])[0].decode('utf-8')}).items() )))

dsTransJson.pprint(1)
dsTransUpd.pprint(1)

def saveToEs(rdd):
    logger.info('Saving data to elk...')	

    #for line in rdd.toLocalIterator() : print(line)

    #rddEs= rdd.map(lambda row: (None, row.asDict()))
    #rddEs.take(10)
    rddes= rdd.map(lambda row: (None, row))
    rddes.take(1)
    confEs = {"es.resource" : "transactions/transaction", "es.nodes" : "elasticsearch", "es.port" : "9200","es.mapping.id" : "id"}
    rddes.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=confEs)
    logger.info('Saved data to elk...')

dsTransUpd.foreachRDD(saveToEs)

ssc.start()
ssc.awaitTermination()

