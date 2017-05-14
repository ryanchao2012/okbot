from django.contrib import admin

from .models import (
    ChatUser, ChatRule, JiebaTagWeight
)
# Register your models here.


class JiebaTagWeightAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'weight', 'punish_factor')
    list_editable = ('description', 'weight', 'punish_factor')


class ChatUserAdmin(admin.ModelAdmin):
    list_display = ('uid', 'platform', 'idtype', 'active', 'state')
    list_editable = ('active', 'state')

    def has_add_permission(self, request):
        return False


class ChatRuleAdmin(admin.ModelAdmin):
    list_display = ('rtype', 'keyword')
    list_editable = ('keyword',)


admin.site.register(JiebaTagWeight, JiebaTagWeightAdmin)
admin.site.register(ChatUser, ChatUserAdmin)
admin.site.register(ChatRule, ChatRuleAdmin)
