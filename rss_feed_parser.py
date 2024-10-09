import feedparser
import datetime
from celery import Celery
from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
import spacy
import logging

# Initialize Celery
app = Celery('news_processor', broker='redis://localhost:6379/0')

# Celery configuration update to handle retries for newer versions (6.0+)
app.conf.update(
    broker_connection_retry_on_startup=True,  # Retain retry behavior on startup
)

# Database setup
Base = declarative_base()

class NewsArticle(Base):
    __tablename__ = 'news_articles'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    content = Column(String)
    publication_date = Column(DateTime)
    source_url = Column(String, unique=True)
    category = Column(String)

engine = create_engine('sqlite:///news_articles.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# RSS feeds
RSS_FEEDS = [
    'http://rss.cnn.com/rss/cnn_topstories.rss',
    'http://qz.com/feed',
    'http://feeds.foxnews.com/foxnews/politics',
    'http://feeds.reuters.com/reuters/businessNews',
    'http://feeds.feedburner.com/NewshourWorld',
    'https://feeds.bbci.co.uk/news/world/asia/india/rss.xml'
]

# NLP model
nlp = spacy.load('en_core_web_sm')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_feed(url):
    """Parses an RSS feed and returns a list of articles."""
    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            logger.error(f"Error parsing feed {url}: {feed.bozo_exception}")
            return []

        logger.info(f"Feed entries: {feed.entries}")  # Log entire feed entries

        articles = []
        for entry in feed.entries:
            logger.info(f"Processing entry: {entry}")  # Log each entry
            
            published = entry.get('published_parsed', None)
            if published and len(published) >= 6:  # Ensure there are enough values to unpack
                publication_date = datetime.datetime(*published[:6])
            else:
                publication_date = datetime.datetime.now()  # Default to now if published is invalid

            article = {
                'title': entry.get('title', 'No Title'),
                'content': entry.get('summary', 'No Content'),
                'publication_date': publication_date,
                'source_url': entry.get('link', 'No URL')
            }
            articles.append(article)

        logger.info(f"Parsed {len(articles)} articles from {url}")
        return articles
    except Exception as e:
        logger.error(f"Failed to parse feed {url}: {str(e)}")
        return []

def classify_article(content):
    """Classifies an article into predefined categories."""
    try:
        doc = nlp(content)
        if any(token.text.lower() in ['protest', 'terrorism', 'riot', 'unrest'] for token in doc):
            return 'Terrorism / protest / political unrest / riot'
        elif any(token.text.lower() in ['earthquake', 'flood', 'hurricane', 'disaster'] for token in doc):
            return 'Natural Disasters'
        elif any(token.text.lower() in ['positive', 'uplifting', 'hope'] for token in doc):
            return 'Positive/Uplifting'
        else:
            return 'Others'
    except Exception as e:
        logger.error(f"Error classifying article: {str(e)}")
        return 'Others'

def store_article(article, category):
    """Stores an article in the database if it's not a duplicate."""
    session = Session()
    try:
        if not session.query(NewsArticle).filter_by(source_url=article['source_url']).first():
            news_article = NewsArticle(
                title=article['title'],
                content=article['content'],
                publication_date=article['publication_date'],
                source_url=article['source_url'],
                category=category
            )
            session.add(news_article)
            session.commit()
            logger.info(f"Stored article: {article['title']}")
        else:
            logger.info(f"Article already exists: {article['source_url']}")
    except Exception as e:
        logger.error(f"Error storing article: {str(e)}")
        session.rollback()
    finally:
        session.close()

@app.task(name='news_processor.process_feed')
def process_feed(feed_url):
    """Processes a feed: parses it, classifies articles, and stores them."""
    logger.info(f"Processing feed: {feed_url}")
    articles = parse_feed(feed_url)
    for article in articles:
        category = classify_article(article['content'])
        store_article(article, category)

if __name__ == "__main__":
    # Manually call process_feed without Celery for testing
    for feed in RSS_FEEDS:
        app.send_task('news_processor.process_feed', args=[feed])  # Correct task name for Celery queue
