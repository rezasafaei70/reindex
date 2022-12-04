from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('sqlite:///database/data.db', echo = True)

Session = sessionmaker(bind=engine)
Session = scoped_session(Session)

# create a Session
session = Session()

Base = declarative_base()
