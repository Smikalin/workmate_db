from sqlalchemy import (
    Column, Integer, String
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class Genre(BaseModel):

    genre_id = Column(Integer, primary_key=True)
    name_genre = Column(String, nullable=False)

    books = relationship('Book', back_populates='genre')
