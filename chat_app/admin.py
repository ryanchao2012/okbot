from django.contrib import admin

from .models import (
    ChatCache, ChatRule, JiebaTagWeight
)
# Register your models here.


class JiebaTagWeightAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'weight', 'punish_factor')
    list_editable = ('description', 'weight', 'punish_factor')


class ChatCacheAdmin(admin.ModelAdmin):
    list_display = ('platform', 'uid', 'get_query', 'get_reply', 'time')


class ChatRuleAdmin(admin.ModelAdmin):
    list_display = ('rtype', 'keyword')
    list_editable = ('keyword',)


admin.site.register(JiebaTagWeight, JiebaTagWeightAdmin)
admin.site.register(ChatCache, ChatCacheAdmin)
admin.site.register(ChatRule, ChatRuleAdmin)
