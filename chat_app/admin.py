from django.contrib import admin
from .models import JiebaTagWeight
# Register your models here.


class JiebaTagWeightAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'weight', 'punish_factor')
    list_editable = ('description', 'weight', 'punish_factor')




admin.site.register(JiebaTagWeight, JiebaTagWeightAdmin)

