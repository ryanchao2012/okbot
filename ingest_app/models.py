from django.utils.translation import ugettext_lazy as _
from django.utils import timezone

from django.db import models

# Create your models here.



class Post(models.Model):
    title = models.CharField(max_length=255)
    tag = models.CharField(max_length=63, default='', blank=True, null=True)
    spider = models.CharField(max_length=63)
    url = models.CharField(max_length=1023)
    author = models.CharField(max_length=63)
    push = models.TextField()
    publish_date = models.DateTimeField(default=timezone.now)
    last_update = models.DateTimeField(default=timezone.now)
    update_count = models.IntegerField(default=0)
    allow_update = models.BooleanField(default=True)
    class Meta:
        verbose_name = _('POST')
        verbose_name_plural = verbose_name

    def __str__(self):
        return '{}'.format(self.title[:20])
    


class Vocabulary(models.Model):
    word = models.CharField(max_length=255)
    tokenizer = models.CharField(max_length=255)
    tag = models.CharField(max_length=63, blank=True, null=True)
    post = models.ManyToManyField(Post, blank=True)
    doc_freq = models.IntegerField(default=0)
    excluded = models.BooleanField(default=False)

    class Meta:
        verbose_name = _('VOCABULARY')
        verbose_name_plural = verbose_name

    def __str__(self):
        return '{}'.format(self.word)



class Joblog(models.Model):
    name = models.CharField(max_length=255)
    start_time = models.DateTimeField(default=timezone.now)
    finish_time = models.DateTimeField(default=timezone.now, blank=True, null=True)
    status = models.CharField(max_length=255, default='running')

    def __str__(self):
        return '{}'.format(self.name)

    class Meta:
        verbose_name = _('JOB LOG')
        verbose_name_plural = verbose_name






    