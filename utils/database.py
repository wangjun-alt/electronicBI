from pyspark import SparkConf
from pyspark.sql import SparkSession
import traceback


class DataSource:
    def __init__(self):
        self.appname = "ElectronicBI"
        self.master = "spark://175.27.155.91:7078"

    def sql(self, sql: str):
        try:
            conf = SparkConf().setAppName(self.appname).setMaster(self.master)
            spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
            return spark.sql(sql)
        except Exception:
            traceback.print_exc()  # 返回出错信息
            return ValueError("获取数据失败")
