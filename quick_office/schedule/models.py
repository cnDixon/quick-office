from django.db import models


# Create your models here.


class JobStatusCt(models.Model):
    date = models.DateField()
    status = models.CharField(max_length=10)
    ct = models.IntegerField()
