from django.apps import AppConfig

class UsersConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.users'

    def ready(self):
        from utils.database import DataSource
        spark = DataSource()
        spark.sql('select * from ods.student1').show()

