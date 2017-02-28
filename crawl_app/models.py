from __future__ import unicode_literals
from django.utils.translation import ugettext_lazy as _
from django.db import models
from django.utils import timezone

# Create your models here.





class Blacklist(models.Model):
    BLIST_TYPE_CHOICES = (
        (0, _("-----")),
        (1, _("title")),
        (2, _("push")),
        (3, _("author")),
    )
    btype = models.IntegerField(choices=BLIST_TYPE_CHOICES, default=0, verbose_name=u'type')
    phrases = models.CharField(max_length=4096)

    def __str__(self):
        return '<{}>{}'.format(self.BLIST_TYPE_CHOICES[self.btype][1], self.phrases[:20])

    class Meta:
        verbose_name = _('BLACK LIST')
        verbose_name_plural = verbose_name

class Spider(models.Model):
    tag = models.CharField(max_length=256)
    entry = models.CharField(max_length=1024)
    start = models.IntegerField(default=-1)
    end = models.IntegerField(default=-1)
    newest = models.IntegerField(default=-1)
    status = models.CharField(max_length=256, default='debug')
    blacklist = models.ManyToManyField(Blacklist, blank=True)

    def __str__(self):
        return '{}'.format(self.tag)

    class Meta:
        verbose_name = _('SPIDER')
        verbose_name_plural = verbose_name


class Joblog(models.Model):
    name = models.CharField(max_length=256)
    start_time = models.DateTimeField(default=timezone.now)
    finish_time = models.DateTimeField(default=timezone.now, blank=True)
    status = models.CharField(max_length=256, default='running')

    def __str__(self):
        return '{}'.format(self.name)

