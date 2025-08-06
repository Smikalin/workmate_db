from sqlalchemy import (
    Column, Integer, Date, ForeignKey
)
from sqlalchemy.orm import relationship

from models.database import BaseModel


class BuyStep(BaseModel):

    buy_step_id = Column(Integer, primary_key=True)
    buy_id = Column(Integer, ForeignKey('buy.buy_id'))
    step_id = Column(Integer, ForeignKey('step.step_id'))
    date_step_beg = Column(Date)
    date_step_end = Column(Date)

    buy = relationship("Buy", back_populates="buy_steps")
    step = relationship("Step", back_populates="buy_steps")
