from sqlalchemy import (
    Column, Integer, String, ForeignKey
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class Client(BaseModel):

    client_id = Column(Integer, primary_key=True)
    name_client = Column(String, nullable=False)
    city_id = Column(Integer, ForeignKey('city.city_id'))
    email = Column(String, nullable=False)

    city = relationship("City", back_populates="clients")
    buys = relationship("Buy", back_populates="client")
