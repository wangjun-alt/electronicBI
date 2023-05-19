from pyspark import SparkConf
from pyspark.sql import SparkSession
import traceback
import json

appname = "test"  # 任务名称
master = "spark://175.27.155.91:7078"  # 模式设置
try:
    conf = SparkConf().setAppName(appname).setMaster(master)  # spark资源配置
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    spark.sql("insert into table ods.contract_db values('6213501493','华润电力(江苏)销售有限公司2019年度电力交易服务合同','华润电力(江苏)销售有限公司','华电广东能源销售有限公司','广东电网公司','2022-04-25','江苏省泰州市姜堰区','广东省深圳市宝安区','2022-05-11','2023-05-23', '45242', '323', '331', '321', '421', '23', '324', '4111', '231', '321', '421', '231', '3421', '321', '1242')")







except:
    traceback.print_exc()  # 返回出错信息
    print('连接出错！')
