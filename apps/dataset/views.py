from datetime import datetime
import json
from django.http import JsonResponse
from rest_framework.views import APIView
from apps.dataset.models import DataSet, DataModel
from apps.dataset.serializers import DataSetSerializers
from utils.database import DataSource

spark = DataSource()

# Create your views here.
class DataSetsView(APIView):
    def get(self, request):
        dataset = DataSet.objects.all()
        dataset_json = DataSetSerializers(instance=dataset, many=True)
        response = JsonResponse({'code': 200, 'errmsg': 'ok', 'dataset': dataset_json.data})
        return response

class TableInfoView(APIView):
    def get(self, request):
        table_res = spark.sql('show tables in ods').collect()
        children = []
        for i in range(len(table_res)):
            children.append({
                'label': table_res[i]['tableName'],
                'value': table_res[i]['tableName'],
                'key': '1-{}'.format(str(i)),
            })
        table_data = [{
            'label': 'ods',
            'value': 'ods',
            'key': '1',
            'children': children
        }]
        response = JsonResponse({'code': 200, 'errmsg': 'ok', 'table_data': table_data})
        return response

    def post(self, request):
        data = json.loads(request.body.decode())
        dataset_id = data.get('dataset_id')
        try:
            table_data = DataModel.objects.filter(app_id=dataset_id).values()
            table_list = []
            for item in table_data:
                if {'value': item['table_name'], 'label': item['table_name']} not in table_list:
                    table_list.append(
                        {'value': item['table_name'], 'label': item['table_name']}
                    )
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'table_name': table_list})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据获取错误'})


class TableDataView(APIView):
    def post(self, request):
        # 1、接收数据
        data = json.loads(request.body.decode())
        table_name = data.get('v')
        if not table_name:
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'table_data': []})
            return response
        else:
            table_data = []
            try:
                for item in table_name:
                    df = spark.sql('select * from ods.{}'.format(item))
                    table_columns = []
                    for i in df.columns:
                        columns = {
                            'title': i,
                            'dataIndex': i,
                            'key': i,
                            'width': 150,
                        }
                        table_columns.append(columns)
                    pd = df.toPandas()
                    resjson = pd.to_json(orient='records', force_ascii=False)
                    table = {
                        'table_name': item,
                        'table_columns': table_columns,
                        'table_value': json.loads(resjson)
                    }
                    table_data.append(table)
                response = JsonResponse({'code': 200, 'errmsg': 'ok', 'table_data': table_data})
                return response
            except Exception:
                return JsonResponse({'code': 400, 'errmsg': '数据表数据获取错误'})


class TableBIView(APIView):
    def post(self, request):
        # 1、接收数据
        result = request.user
        username = result.get('username')
        data = json.loads(request.body.decode())
        dataset_name = data.get('dataset_name')
        dataset_desc = data.get('dataset_desc')
        table_selected = data.get('table_selected')
        if not table_selected:
            return JsonResponse({'code': 400, 'errmsg': '未选择数据源'})
        res = DataSet.objects.filter(data_name=dataset_name)
        if res.exists():
            return JsonResponse({'code': 400, 'errmsg': '数据集已存在'})
        try:
            last_dataset = DataSet.objects.last()
            data_id = last_dataset.id + 1
            DataSet.objects.create(id=data_id, data_name=dataset_name, data_descr=dataset_desc, create_user=username,
                                   create_date=datetime.today().date())
            for item in table_selected:
                df = spark.sql('select * from ods.{}'.format(item))
                type_items = df.dtypes
                for i in type_items:
                    last_datamodel = DataModel.objects.last()
                    if i[1] == 'string':
                        DataModel.objects.create(id=last_datamodel.id+1, app_id=data_id, table_name=item, field_name=i[0],
                                                 data_type=True)
                    else:
                        DataModel.objects.create(id=last_datamodel.id+1, app_id=data_id, table_name=item, field_name=i[0],
                                                 data_type=False)
            response = JsonResponse({'code': 200, 'errmsg': 'ok'})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据集创建失败'})


class DatasetDelView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        dataset_id = data.get('id')
        try:
            DataSet.objects.get(id=dataset_id).delete()
            DataModel.objects.filter(app_id=dataset_id).delete()
            response = JsonResponse({'code': 200, 'errmsg': 'ok'})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据集删除失败'})


class GetColumnsView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        table_name = data.get('name')
        try:
            dimension_data = DataModel.objects.filter(table_name=table_name, data_type=True).values()
            index_data = DataModel.objects.filter(table_name=table_name, data_type=False).values()
            dimension_li = []
            index_li = []
            dimension = []
            index = []
            for d in dimension_data:
                if d['field_name'] not in dimension_li:
                    dimension_li.append(d['field_name'])
            for i in index_data:
                if i['field_name'] not in index_li:
                    index_li.append(i['field_name'])
            for ditem in dimension_li:
                dimension.append(
                    {
                        'value': ditem
                    }
                )
            for iitem in index_li:
                index.append(
                    {'value': iitem}
                )
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'coordinate_data':{'dimension': dimension, 'index':index }})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据集数据获取失败'})

"""avagelist, maxlist, minlist, sumlist"""
class GetHeaderView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        table_header = data.get('header')
        table_name = data.get('table_name')
        avageli = data.get('avagelist')
        maxli = data.get('maxlist')
        minli = data.get('minlist')
        sumli = data.get('sumlist')
        table_columns = ",".join(table_header)
        if len(table_header) > 0:
            columns = []
            for item in table_header:
                columns.append({
                    'title': item,
                    'dataIndex': item,
                },)
            df = spark.sql('select %s from ods.%s' % (table_columns, table_name))
            pd = df.toPandas()
            if len(avageli) > 0:
                for avage in avageli:
                    avage_i = pd[avage].mean()
                    pd[avage] = avage_i
            if len(maxli) > 0:
                for max in maxli:
                    max_i = pd[max].var()
                    pd[max] = max_i
            if len(minli)>0:
                for min in minli:
                    min_i = pd[min].std()
                    pd[min] = min_i
            if len(sumli) > 0:
                for sum in sumli:
                    sum_i = pd[sum].sum()
                    pd[sum] = sum_i
            resjson = pd.to_json(orient='records', force_ascii=False)
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'columns': columns, 'table_data': json.loads(resjson)})
            return response
        else:
            return JsonResponse({'code': 400, 'errmsg': '未选择列名'})


class SetBarDataView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        bardim = data.get('bardim')
        index_list = data.get('index_list')
        table_name = data.get('table_name')
        avageli = data.get('avagelist')
        maxli = data.get('maxlist')
        minli = data.get('minlist')
        sumli = data.get('sumlist')
        if not bardim or not index_list or not table_name:
            return JsonResponse({'code': 400, 'errmsg': '请检查是否选择维度和指标及横轴字段','bardim_list': [], 'bar_data': []})
        table_index = ",".join(index_list)
        bardim_list = []
        bar_data = []
        try:
            df_dim = spark.sql('select %s from ods.%s' % (bardim, table_name))
            pd_dim = df_dim.toPandas()
            res_dim = pd_dim.values
            for item_dim in res_dim:
                bardim_list.append(item_dim[0])
            df_index = spark.sql('select %s from ods.%s' % (table_index, table_name))
            pd = df_index.toPandas()
            if len(avageli) > 0:
                for avage in avageli:
                    avage_i = pd[avage].mean()
                    pd[avage] = avage_i
            if len(maxli) > 0:
                for max in maxli:
                    max_i = pd[max].var()
                    pd[max] = max_i
            if len(minli)>0:
                for min in minli:
                    min_i = pd[min].std()
                    pd[min] = min_i
            if len(sumli) > 0:
                for sum in sumli:
                    sum_i = pd[sum].sum()
                    pd[sum] = sum_i
            for i in range(len(index_list)):
                index_data = []
                for item_index in pd.values:
                    index_data.append(item_index[i])
                bar_data.append({
                    'index_name': index_list[i],
                    'index_data': index_data
                })
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'bardim_list': bardim_list, 'bar_data': bar_data})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '请检查是否选择维度和指标及横轴字段', 'bardim_list': [], 'bar_data': []})


class SetPieDataView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        bardim = data.get('bardim')
        index = data.get('index')
        table_name = data.get('table_name')
        search_name = bardim + ',' + index
        pie_data = []
        try:
            df = spark.sql('select %s from ods.%s' % (search_name, table_name))
            pd = df.toPandas()
            for item in pd.values:
                pie_data.append({
                    'value': item[1],
                    'name': item[0]
                })
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'pie_data': pie_data})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据获取错误', 'pie_data': []})


class GetDatasetNum(APIView):
    def get(self, request):
        dataset = DataSet.objects.all()
        dataset_json = DataSetSerializers(instance=dataset, many=True)
        data = dataset_json.data
        response = JsonResponse({'code': 200, 'errmsg': 'ok', 'dataset_num': len(data)})
        return response