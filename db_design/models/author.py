from sqlalchemy import (
    Column, Integer, String
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class Author(BaseModel):
    author_id = Column(Integer, primary_key=True)
    name_author = Column(String, nullable=False)

    books = relationship("Book", back_populates="author")
