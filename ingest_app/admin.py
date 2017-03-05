from django.contrib import admin


from .models import Vocabulary, Post, Joblog
# Register your models here.



class PostAdmin(admin.ModelAdmin):
    list_display = ('title', 'tag', 'spider', 'last_update', 'update_count', 'allow_update')


class VocabularyAdmin(admin.ModelAdmin):
    list_display = ('word', 'doc_freq', 'tag', 'tokenizer', 'excluded')


class JoblogAdmin(admin.ModelAdmin):
    list_display = ('name', 'start_time', 'finish_time', 'status')
    readonly_fields = ('name', 'start_time', 'finish_time', 'status')
    def has_add_permission(self, request):
        return False


admin.site.register(Post, PostAdmin)
admin.site.register(Vocabulary, VocabularyAdmin)
admin.site.register(Joblog, JoblogAdmin)