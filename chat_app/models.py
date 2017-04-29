from django.db import models
from django.core.validators import MaxValueValidator, MinValueValidator

# Create your models here.


class JiebaTagWeight(models.Model):
	name = models.CharField(max_length=31, unique=True)
	weight = models.FloatField(default=1.0)
	punish_factor = models.FloatField(default=0.0,
    	validators=[MinValueValidator(0.0), MaxValueValidator(1.0)]
    )

# class Rule