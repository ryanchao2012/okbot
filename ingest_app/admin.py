from django.contrib import admin


from .models import Vocabulary, Post, Joblog#, Grammar
# Register your models here.



class PostAdmin(admin.ModelAdmin):
    list_display = ('title', 'tokenized', 'spider', 'last_update', 'allow_update')
    list_editable = ('allow_update',)
    search_fields = ('title',)

class VocabularyAdmin(admin.ModelAdmin):
    list_display = ('name', 'doc_freq', 'stopword')
    list_editable = ('stopword',)
    readonly_fields = ('post',)
    search_fields = ('name',)

#class GrammarAdmin(admin.ModelAdmin):
#    list_display = ('name', 'sent_tag', 'tokenizer', 'doc_freq')
#    readonly_fields = ('post',)
#    search_fields = ('name',)



class JoblogAdmin(admin.ModelAdmin):
    list_display = ('name', 'start_time', 'finish_time', 'status')
    readonly_fields = ('name', 'start_time', 'finish_time', 'status')
    def has_add_permission(self, request):
        return False


admin.site.register(Post, PostAdmin)
admin.site.register(Vocabulary, VocabularyAdmin)
admin.site.register(Joblog, JoblogAdmin)
#admin.site.register(Grammar, GrammarAdmin)



