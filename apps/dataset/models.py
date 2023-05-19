from django.db import models

# Create your models here.
class DataSet(models.Model):
    """数据集基本信息"""
    id = models.IntegerField(verbose_name="数据集id", primary_key=True, unique=True)
    data_name = models.CharField(max_length=100, verbose_name='数据集名称', null=True)
    data_descr = models.CharField(max_length=200, verbose_name='数据集描述', null=True)
    db_name = models.CharField(max_length=100, verbose_name='数据库名称', null=True)
    create_user = models.CharField(max_length=100, verbose_name='创建者', null=True)
    create_time = models.DateTimeField(verbose_name='创建时间', null=True)
    create_date = models.DateField(verbose_name='创建时间', null=True)

    def __str__(self):
        return str(self.id)

    class Meta:
        verbose_name = '数据集表'
        verbose_name_plural = verbose_name


class DataModel(models.Model):
    """数据表具体信息"""
    id = models.IntegerField(verbose_name="排序id", primary_key=True, unique=True)
    app_id = models.IntegerField(verbose_name="数据集id", unique=False)
    table_name = models.CharField(max_length=100, verbose_name='数据表名称', null=True)
    field_name = models.CharField(max_length=100, verbose_name='字段名称', null=True)
    data_type = models.BooleanField(default=True, verbose_name='字段类型')

    def field_type(self):
        if self.data_type:
            return '维度'
        else:
            return '指标'
    field_type.short_description = "字段类型"

    def __str__(self):
        return str(self.id)

    class Meta:
        verbose_name = '数据模型表'
        verbose_name_plural = verbose_name