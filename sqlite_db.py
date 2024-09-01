from sqlalchemy import create_engine, Column, Integer, DateTime, Float, String, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

def init_db():

    #Define db
    engine = create_engine('sqlite:///reddit_smp_sentiment.db')
    base = declarative_base()

    #Define yfinance_smp
    class YFinanceSMP(base):
        __tablename__ = 'y_finance_smp'
        id = Column(Integer, primary_key=True, autoincrement=True)
        date = Column(DateTime)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        adj_close = Column(Float)
        volume = Column(Integer)

    #Define reddit_source
    class RedditSource(base):
        __tablename__ = 'reddit_source'
        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime)
        date = Column(DateTime)
        title = Column(String)
        sentiment = Column(Float)

    #Define reddit_transform
    class RedditTransform(base):
        __tablename__ = 'reddit_transform'
        id = Column(Integer, primary_key=True, autoincrement=True)
        date = Column(DateTime)
        sentiment_agg = Column(Float)

    base.metadata.create_all(engine)

def delete_db(db_path):

    # Create an engine connected to the SQLite database
    engine = create_engine(f'sqlite:///{db_path}')

    # Dispose of the engine to close all connections
    engine.dispose()

    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Database '{db_path}' deleted successfully.")
    else:
        print(f"Database '{db_path}' does not exist.")

if __name__ == '__main__':


    # delete_db('reddit_smp_sentiment.db')
    init_db()

