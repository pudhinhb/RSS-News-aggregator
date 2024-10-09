from celery import Celery
from rss_feed_parser import process_feed

app = Celery('news_processor', broker='redis://localhost:6379/0')

# Register the task explicitly
app.conf.task_default_queue = 'celery'
app.tasks.register(process_feed)
