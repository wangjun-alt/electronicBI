# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# import traceback
# appname = "test"
# master = "spark://175.27.155.91:7078"
#
# try:
#     conf = SparkConf().setAppName(appname).setMaster(master)
#     spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
#     sc = spark.sparkContext
#     spark.sql('show databases').show()
#     sc.stop()
#     print('计算成功！')
# except:
#     traceback.print_exc()#返回出错信息
#     print('连接出错')

from utils.database import DataSource
spark = DataSource()
spark.sql('show databases').show()