from django.contrib import admin

# Register your models here.
from .models import Spider, Blacklist, Joblog


class SpiderAdmin(admin.ModelAdmin):
    list_display = ('tag', 'offset', 'page', 'newest', 'start', 'end', 'status')
    # list_editable = ('start', 'end')
    readonly_fields = ('newest', 'status')


class BlacklistAdmin(admin.ModelAdmin):
    list_display = ('pk', 'btype', 'phrases')
    list_editable = ('btype', 'phrases')
    list_display_links = ('pk',)



class JoblogAdmin(admin.ModelAdmin):
    list_display = ('name', 'start_time', 'finish_time', 'status')
    readonly_fields = ('name', 'start_time', 'result', 'finish_time', 'status')
    def has_add_permission(self, request):
        return False

#    def has_change_permission(self, request):
#        return False

admin.site.register(Spider, SpiderAdmin)
admin.site.register(Blacklist, BlacklistAdmin)
admin.site.register(Joblog, JoblogAdmin)

