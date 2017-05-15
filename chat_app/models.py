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
    keyword = models.CharField(max_length=1023, blank=True, null=True)
    response = models.TextField(blank=True, null=True)

    def __str__(self):
        return '<{}>{}'.format(self.rtype, self.keyword[:20])

    class Meta:
        verbose_name = 'Chat Rule'
        verbose_name_plural = verbose_name


class ChatCache(models.Model):
    user = models.OneToOneField('ChatUser', on_delete=models.CASCADE)
    query = models.TextField()
    keyword = models.TextField(blank=True, null=True)
    reply = models.TextField()
    time = models.DateTimeField(default=timezone.now)
    repeat = models.IntegerField(default=0)
    post = models.TextField(null=True, blank=True)
    push_num = models.IntegerField(default=0, null=True, blank=True)
    tree_node = models.IntegerField(default=-1, null=True, blank=True)

    class Meta:
        verbose_name = 'Chat Cache'
        verbose_name_plural = verbose_name


class ChatUser(models.Model):
    platform = models.CharField(max_length=31)
    uid = models.CharField(max_length=127)
    idtype = models.CharField(max_length=31, blank=True, null=True)
    active = models.BooleanField(default=False)
    state = models.IntegerField(default=0)
    chat_count = models.IntegerField(default=0)

    class Meta:
        unique_together = ('platform', 'uid')
        verbose_name = 'Chat User'
        verbose_name_plural = verbose_name

    def __str__(self):
        return '<{}>{}'.format(self.platform, self.uid)


class ChatTree(models.Model):
    user = models.ForeignKey('ChatUser', on_delete=models.CASCADE)
    ancestor = models.IntegerField(default=-1, null=True, blank=True)
    successor = models.IntegerField(default=-1, null=True, blank=True)
    query = models.TextField()
    keyword = models.TextField(blank=True, null=True)
    reply = models.TextField()
    time = models.DateTimeField(default=timezone.now)
    post = models.TextField(null=True, blank=True)
    push_num = models.IntegerField(default=0, null=True, blank=True)

    class Meta:
        verbose_name = 'Chat Tree'
        verbose_name_plural = verbose_name

