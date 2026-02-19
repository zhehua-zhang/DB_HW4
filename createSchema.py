# create_analytics_schema.py
from sqlalchemy import create_engine
from TargetSchema import Base

engine = create_engine("sqlite:///analytics.sqlite3", future=True)
Base.metadata.create_all(engine)
print("Target Schema created")
