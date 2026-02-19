from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Boolean, Float,
    ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class DimDate(Base):
    __tablename__ = "dim_date"
    date_key = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)

class DimFilm(Base):
    __tablename__ = "dim_film"
    film_key = Column(Integer, primary_key=True, autoincrement=True)
    film_id = Column(Integer, nullable=False, unique=True)
    title = Column(String(255), nullable=False)
    rating = Column(String(16), nullable=True)
    length = Column(Integer, nullable=True)
    language = Column(String(64), nullable=True)
    release_year = Column(Integer, nullable=True)
    last_update = Column(DateTime, nullable=True)

class DimActor(Base):
    __tablename__ = "dim_actor"
    actor_key = Column(Integer, primary_key=True, autoincrement=True)
    actor_id = Column(Integer, nullable=False, unique=True)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    last_update = Column(DateTime, nullable=True)

class DimCategory(Base):
    __tablename__ = "dim_category"
    category_key = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(Integer, nullable=False, unique=True)
    name = Column(String(64), nullable=False)
    last_update = Column(DateTime, nullable=True)

class DimStore(Base):
    __tablename__ = "dim_store"
    store_key = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, nullable=False, unique=True)
    city = Column(String(64), nullable=True)
    country = Column(String(64), nullable=True)
    last_update = Column(DateTime, nullable=True)

class DimCustomer(Base):
    __tablename__ = "dim_customer"
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, nullable=False, unique=True)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    active = Column(Boolean, nullable=False)
    city = Column(String(64), nullable=True)
    country = Column(String(64), nullable=True)
    last_update = Column(DateTime, nullable=True)

class BridgeFilmActor(Base):
    __tablename__ = "bridge_film_actor"
    film_key = Column(Integer, ForeignKey("dim_film.film_key"), primary_key=True)
    actor_key = Column(Integer, ForeignKey("dim_actor.actor_key"), primary_key=True)

class BridgeFilmCategory(Base):
    __tablename__ = "bridge_film_category"
    film_key = Column(Integer, ForeignKey("dim_film.film_key"), primary_key=True)
    category_key = Column(Integer, ForeignKey("dim_category.category_key"), primary_key=True)

class FactRental(Base):
    __tablename__ = "fact_rental"
    fact_rental_key = Column(Integer, primary_key=True, autoincrement=True)

    rental_id = Column(Integer, nullable=False, unique=True)  

    date_key_rented = Column(Integer, ForeignKey("dim_date.date_key"), nullable=False)
    date_key_returned = Column(Integer, ForeignKey("dim_date.date_key"), nullable=True)

    film_key = Column(Integer, ForeignKey("dim_film.film_key"), nullable=False)
    store_key = Column(Integer, ForeignKey("dim_store.store_key"), nullable=False)
    customer_key = Column(Integer, ForeignKey("dim_customer.customer_key"), nullable=False)

    staff_id = Column(Integer, nullable=True)  
    rental_duration_days = Column(Integer, nullable=True)

class FactPayment(Base):
    __tablename__ = "fact_payment"
    fact_payment_key = Column(Integer, primary_key=True, autoincrement=True)

    payment_id = Column(Integer, nullable=False, unique=True)

    date_key_paid = Column(Integer, ForeignKey("dim_date.date_key"), nullable=False)
    customer_key = Column(Integer, ForeignKey("dim_customer.customer_key"), nullable=False)
    store_key = Column(Integer, ForeignKey("dim_store.store_key"), nullable=False)

    staff_id = Column(Integer, nullable=True)
    amount = Column(Float, nullable=False)

class EtlState(Base):
    __tablename__ = "etl_state"
    name = Column(String(64), primary_key=True)   
    watermark = Column(DateTime, nullable=True)
