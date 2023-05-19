from rest_framework import serializers

class DataSetSerializers(serializers.Serializer):
    """数据集基本信息"""
    id = serializers.IntegerField()
    data_name = serializers.CharField()
    data_descr = serializers.CharField()
    db_name = serializers.CharField()
    create_user = serializers.CharField()
    create_time = serializers.DateTimeField()
    create_date = serializers.DateField()