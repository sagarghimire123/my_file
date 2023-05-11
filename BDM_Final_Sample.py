from pyspark import SparkContext
import itertools

if __name__=='__main__':
    sc = SparkContext()
    HDFS_INPUT = 'gs://bdma/data/weekly-patterns-nyc-2019-2020'
    header = sc.textFile(f'{HDFS_INPUT}/part-00000').first()
    sc.textFile(f'{HDFS_INPUT}/part-*') \
        .sample(False, 0.01) \
        .coalesce(1) \
        .mapPartitions(lambda x: itertools.chain([header], x)) \
        .saveAsTextFile('gs://big_data_my_bucket/weekly-patterns-nyc-2019-2020-sample')
