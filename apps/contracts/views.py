import json
from django.http import JsonResponse
from rest_framework.views import APIView
from utils.database import DataSource

spark = DataSource()

# Create your views here.
class SetContractDataView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        contract_id ='{0}{1}{0}'.format("\'", data.get('contract_id'))
        contract_name = '{0}{1}{0}'.format("\'", data.get('contract_name'))
        part_a = '{0}{1}{0}'.format("\'", data.get('part_a'))
        part_b = '{0}{1}{0}'.format("\'", data.get('part_b'))
        part_c = '{0}{1}{0}'.format("\'", data.get('part_c'))
        signing_time = '{0}{1}{0}'.format("\'", data.get('contract_date'))
        purchasing_loaction = '{0}{1}{0}'.format("\'", data.get('purchasing_loaction'))
        saleing_loaction = '{0}{1}{0}'.format("\'", data.get('saleing_loaction'))
        effective_time = '{0}{1}{0}'.format("\'", data.get('effective_time'))
        expiration_time = '{0}{1}{0}'.format("\'", data.get('expiration_time'))
        total_transaction_power = '{0}{1}{0}'.format("\'", str(data.get('total_transaction_power')))
        sale_price = '{0}{1}{0}'.format("\'", str(data.get('sale_price')))
        delivery_1_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_1_month')))
        delivery_2_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_2_month')))
        delivery_3_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_3_month')))
        delivery_4_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_4_month')))
        delivery_5_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_5_month')))
        delivery_6_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_6_month')))
        delivery_7_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_7_month')))
        delivery_8_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_8_month')))
        delivery_9_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_9_month')))
        delivery_10_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_10_month')))
        delivery_11_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_11_month')))
        delivery_12_month = '{0}{1}{0}'.format("\'", str(data.get('delivery_12_month')))
        delivery_year = '{0}{1}{0}'.format("\'", str(data.get('delivery_year')))
        values = contract_id + ',' + contract_name + ',' + part_a + ',' + part_b + ',' + part_c + ',' + signing_time\
                 + ',' + purchasing_loaction + ',' + saleing_loaction + ',' + effective_time + ',' + expiration_time + ',' + \
                 total_transaction_power + ',' + sale_price + ',' + delivery_1_month + ',' + delivery_2_month + ',' + delivery_3_month + ',' + \
                 delivery_4_month + ',' + delivery_5_month + ',' + delivery_6_month + ',' + delivery_7_month + ',' + delivery_8_month + ',' + delivery_9_month + ',' + \
                 delivery_10_month + ',' + delivery_11_month + ',' + delivery_12_month + ',' + delivery_year
        try:
            spark.sql("insert into table ods.contract_db values(%s)"%(values))
            return JsonResponse({'code': 200, 'errmsg': '数据插入成功'})
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据插入失败'})


class GetContractNumView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        contract_id = data.get("contract_id")
        try:
            df = spark.sql("select contract_id from ods.contract_db")
            pd = df.toPandas()
            contract_li = []
            print(pd.values)
            for i in pd.values:
                contract_li.append(i[0])
            if contract_id not in contract_li:
                return JsonResponse({'code': 200, 'errmsg': 'ok'})
            else:
                return JsonResponse({'code': 400, 'errmsg': '已经存在该合同编号'})
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '未知错误'})


class SearchContractView(APIView):
    def get(self, request):
        try:
            table_res = spark.sql('show tables in ods').collect()
            table_list = []
            for i in range(len(table_res)):
                table_list.append({
                    'label': table_res[i]['tableName'],
                    'value': table_res[i]['tableName']
                })
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'table_list': table_list})
            return response
        except Exception:
            response = JsonResponse({'code': 400, 'errmsg': 'error', 'table_list': []})
            return response


class GetTableDataView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        table_name = data.get("table_name")
        try:
            df = spark.sql('select * from ods.{}'.format(table_name))
            table_columns = []
            for i in df.columns:
                table_columns.append(i)
            pd = df.toPandas()
            resjson = pd.to_json(orient='records', force_ascii=False)
            table_data = json.loads(resjson)
            response = JsonResponse({'code': 200, 'errmsg': 'ok', 'table_columns': table_columns,'table_data': table_data})
            return response
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '数据表数据获取错误'})


class UploadContractView(APIView):
    def post(self, request):
        file = request.FILES.get('file')
        print(file)
        return JsonResponse({'code': 200, 'errmsg': 'ok'})