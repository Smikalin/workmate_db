from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

from config import SQLALCHEMY_DATABASE_URL


class PreBase:

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


BaseModel = declarative_base(cls=PreBase)

engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


def create_db():
    BaseModel.metadata.create_all(engine)
