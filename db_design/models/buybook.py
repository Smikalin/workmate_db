from sqlalchemy import (
    Column, Integer, ForeignKey
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class BuyBook(BaseModel):

    buy_book_id = Column(Integer, primary_key=True)
    buy_id = Column(Integer, ForeignKey('buy.buy_id'))
    book_id = Column(Integer, ForeignKey('book.book_id'))
    amount = Column(Integer, nullable=False)

    buy = relationship("Buy", back_populates="buy_books")
    book = relationship("Book", back_populates="buy_books")
