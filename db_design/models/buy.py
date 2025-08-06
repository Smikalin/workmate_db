from sqlalchemy import (
    Column, Integer, String, ForeignKey
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class Buy(BaseModel):
    buy_id = Column(Integer, primary_key=True)
    buy_description = Column(String)
    client_id = Column(Integer, ForeignKey('client.client_id'))

    client = relationship("Client", back_populates="buys")
    buy_books = relationship("BuyBook", back_populates="buy")
    buy_steps = relationship("BuyStep", back_populates="buy")