from sqlalchemy import Integer, Column, String,TEXT

from db import Base


class Failure(Base):
    __tablename__ = 'failurs'
    id = Column(Integer, primary_key=True)
    start_time = Column(String)
    end_time = Column(String)
    index_name = Column(String)
    query = Column(TEXT)

