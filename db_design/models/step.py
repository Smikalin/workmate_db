from sqlalchemy import (
    Column, Integer, String
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class Step(BaseModel):

    step_id = Column(Integer, primary_key=True)
    name_step = Column(String, nullable=False)

    buy_steps = relationship("BuyStep", back_populates="step")