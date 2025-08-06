from sqlalchemy import (
    Column, Integer, String, ForeignKey, Float
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class Book(BaseModel):
    book_id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    author_id = Column(Integer, ForeignKey('author.author_id'))
    genre_id = Column(Integer, ForeignKey('genre.genre_id'))
    price = Column(Float, nullable=False)
    amount = Column(Integer, nullable=False)

    author = relationship("Author", back_populates="books")
    genre = relationship("Genre", back_populates="books")
    buy_books = relationship("BuyBook", back_populates="book")