from django.contrib import admin

from .models import (
    ChatCache, ChatTree, ChatUser, ChatRule, JiebaTagWeight
)
# Register your models here.


class JiebaTagWeightAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'weight', 'punish_factor')
    list_editable = ('description', 'weight', 'punish_factor')


class ChatUserAdmin(admin.ModelAdmin):
    list_display = ('uid', 'platform', 'idtype', 'active', 'chat_count', 'state')
    list_editable = ('active',)

    def has_add_permission(self, request):
        return False


class ChatCacheAdmin(admin.ModelAdmin):
    list_display = ('user', 'short_query', 'short_reply', 'push_num', 'time', 'repeat')

    def has_add_permission(self, request):
        return False

    def short_query(self, obj):
        if len(obj.query) > 20:
            return '{}...'.format(obj.query[:20])
        else:
            return obj.query

    def short_reply(self, obj):
        if len(obj.reply) > 20:
            return '{}...'.format(obj.reply[:20])
        else:
            return obj.reply

    short_query.short_description = 'query'
    short_reply.short_description = 'reply'


class ChatRuleAdmin(admin.ModelAdmin):
    list_display = ('rtype', 'keyword')
    list_editable = ('keyword',)


class ChatTreeAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'short_query', 'short_reply', 'ancestor', 'successor', 'time', 'push_num')

    def has_add_permission(self, request):
        return False

    def short_query(self, obj):
        if len(obj.query) > 20:
            return '{}...'.format(obj.query[:20])
        else:
            return obj.query

    def short_reply(self, obj):
        if len(obj.reply) > 20:
            return '{}...'.format(obj.reply[:20])
        else:
            return obj.reply

    short_query.short_description = 'query'
    short_reply.short_description = 'reply'



admin.site.register(JiebaTagWeight, JiebaTagWeightAdmin)
admin.site.register(ChatUser, ChatUserAdmin)
admin.site.register(ChatCache, ChatCacheAdmin)
admin.site.register(ChatRule, ChatRuleAdmin)
admin.site.register(ChatTree, ChatTreeAdmin)

