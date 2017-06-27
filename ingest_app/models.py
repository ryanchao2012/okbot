from django.db import models
from django.utils import timezone

# Create your models here.




class PostImgur(models.Model):
    title = models.CharField(max_length=255)
    grammar = models.CharField(max_length=255, default='', blank=True, null=True)
    tokenized_jieba_tw = models.CharField(max_length=255, default='', blank=True, null=True)
    url = models.CharField(max_length=1023, unique=True)
    push = models.TextField()


class Post(models.Model):
    title = models.CharField(max_length=255)

    tokenized = models.CharField(max_length=255, default='', blank=True, null=True)
    grammar = models.CharField(max_length=255, default='', blank=True, null=True)
    tokenized_jieba_tw = models.CharField(max_length=255, default='', blank=True, null=True)
    grammar_jieba_tw = models.CharField(max_length=255, default='', blank=True, null=True)

    tag = models.CharField(max_length=63, default='', blank=True, null=True)
    spider = models.CharField(max_length=63)
    url = models.CharField(max_length=1023, unique=True)
    author = models.CharField(max_length=63)
    push = models.TextField()
    publish_date = models.DateTimeField(default=timezone.now)
    last_update = models.DateTimeField(default=timezone.now)
    update_count = models.IntegerField(default=0)
    allow_update = models.BooleanField(default=True)
    #entity = models.CharField(max_length=63, blank=True, null=True)
    #verb = models.CharField(max_length=63, blank=True, null=True)

    class Meta:
        verbose_name = 'POST'
        verbose_name_plural = verbose_name

    def __str__(self):
        return '<{}>{}'.format(self.spider, self.title[:20])


class Tokenized(models.Model):
    tokenized = models.CharField(max_length=255)
    tokenizer = models.CharField(max_length=31)
    # post = models.ForeignKey(Post)


class Grammar(models.Model):
    grammer = models.CharField(max_length=255)
    tokenizer = models.CharField(max_length=31)
    # post = models.ForeignKey(Post)


class Vocabulary(models.Model):
    name = models.CharField(max_length=255, unique=True)
    word = models.CharField(max_length=255)
    tokenizer = models.CharField(max_length=31)
    tag = models.CharField(max_length=31, blank=True, null=True)
    post = models.ManyToManyField(Post, blank=True)
    doc_freq = models.IntegerField(default=0)
    stopword = models.BooleanField(default=False)

    class Meta:
        verbose_name = 'VOCABULARY'
        verbose_name_plural = verbose_name

    # def get_posts(self):
    #     return "\n".join([p.title for p in self.post.all()])

    def __str__(self):
        return '{}'.format(self.word)


# class Grammar(models.Model):
#    name = models.CharField(max_length=1023, unique=True)
#    sent_tag = models.CharField(max_length=1023)
#    tokenizer = models.CharField(max_length=255)
#    doc_freq = models.IntegerField(default=0)
#    post = models.ManyToManyField(Post, blank=True)

#    def __str__(self):
#        return '{}'.format(self.sent_tag)

#    class Meta:
#        verbose_name = _('GRAMMAR')
#        verbose_name_plural = verbose_name

class Joblog(models.Model):
    name = models.CharField(max_length=255)
    start_time = models.DateTimeField(default=timezone.now)
    result = models.TextField()
    finish_time = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=31, default='running')

    def __str__(self):
        return '{}'.format(self.name)

    class Meta:
        verbose_name = 'JOB LOG'
        verbose_name_plural = verbose_name
