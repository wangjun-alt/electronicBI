from django.db import models

# Create your models here.


class User(models.Model):
    """用户的基本个人信息"""
    username = models.CharField(max_length=100, verbose_name='用户名', primary_key=True, unique=True)
    password = models.CharField(max_length=100, verbose_name='密码')

    def __str__(self):
        return self.username

    class Meta:
        verbose_name = '个人信息表'
        verbose_name_plural = verbose_name
