from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils import timezone

# Create your models here.


class JiebaTagWeight(models.Model):
    name = models.CharField(max_length=31, unique=True)
    description = models.CharField(max_length=127, null=True, blank=True)
    weight = models.FloatField(default=1.0)
    punish_factor = models.FloatField(
        default=0.0, validators=[MinValueValidator(0.0), MaxValueValidator(1.0)]
    )

    class Meta:
        verbose_name = 'Jieba Tag'
        verbose_name_plural = verbose_name

    def __str__(self):
        return '{}:jieba'.format(self.name)


class ChatRule(models.Model):
    rtype = models.CharField(max_length=31, verbose_name='type')
    keyword = models.CharField(max_length=1023)
    response = models.TextField(blank=True, null=True)

    def __str__(self):
        return '<{}>{}'.format(self.rtype, self.keyword[:20])

    class Meta:
        verbose_name = 'Chat Rule'
        verbose_name_plural = verbose_name


class ChatCache(models.Model):
    platform = models.CharField(max_length=31)
    uid = models.CharField(max_length=127, unique=True)
    idtype = models.CharField(max_length=31, blank=True, null=True)
    query = models.TextField()
    keyword = models.TextField(blank=True, null=True)
    reply = models.TextField()
    time = models.DateTimeField(default=timezone.now)

    def get_query(self):
        return self.query[:20]

    def get_reply(self):
        return self.reply[:20]


# class ChatTree(model.Model):
