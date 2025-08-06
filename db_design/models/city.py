from sqlalchemy import (
    Column, Integer, String
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class City(BaseModel):

    city_id = Column(Integer, primary_key=True)
    name_city = Column(String, nullable=False)
    days_delivery = Column(Integer, nullable=False)

    clients = relationship("Client", back_populates="city")\
