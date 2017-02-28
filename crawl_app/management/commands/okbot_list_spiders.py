
from django.core.management.base import BaseCommand, CommandError
from crawl_app.models import Spider
from pygments import highlight, lexers, formatters
import json


class Command(BaseCommand):
    help = 'list all spiders in db'

    def handle(self, *args, **options):
        fields = ('tag', 'entry', 'newest', 'start', 'end', 'status')
        
        spider = Spider.objects.all()
        for sp in spider:
            spiderinfo = {f: sp.__dict__[f] for f in fields}
            formatted_json = json.dumps(spiderinfo, indent = 4)
            colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
            print(colorful_json)

