from django.test import TestCase
from apps.dataset.models import DataSet

# Create your tests here.
res = DataSet.objects.last()
print(res)