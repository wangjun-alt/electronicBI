import json
from django.http import JsonResponse
from rest_framework.views import APIView
from utils.database import DataSource

spark = DataSource()

# Create your views here.
class SetContractDataView(APIView):
    def post(self, request):
        data = json.loads(request.body.decode())
        contract_id = data.get('contract_id')
        contract_name = data.get('contract_name')
        part_a = data.get('part_a')
        part_b = data.get('part_b')
        part_c = data.get('part_c')
        signing_time = data.get('contract_date')
        purchasing_loaction = data.get('purchasing_loaction')
        saleing_loaction = data.get('saleing_loaction')
        effective_time = data.get('effective_time')
        expiration_time = data.get('expiration_time')
        total_transaction_power = data.get('total_transaction_power')
        sale_price = data.get('sale_price')
        delivery_1_month = data.get('delivery_1_month')
        delivery_2_month = data.get('delivery_2_month')
        delivery_3_month = data.get('delivery_3_month')
        delivery_4_month = data.get('delivery_4_month')
        delivery_5_month = data.get('delivery_5_month')
        delivery_6_month = data.get('delivery_6_month')
        delivery_7_month = data.get('delivery_7_month')
        delivery_8_month = data.get('delivery_8_month')
        delivery_9_month = data.get('delivery_9_month')
        delivery_10_month = data.get('delivery_10_month')
        delivery_11_month = data.get('delivery_11_month')
        delivery_12_month = data.get('delivery_12_month')
        delivery_year = data.get('delivery_year')
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
