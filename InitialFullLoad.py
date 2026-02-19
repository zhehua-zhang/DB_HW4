from __future__ import annotations

from datetime import date, datetime
from typing import Dict, Iterable, List, Tuple, Optional

from sqlalchemy import create_engine, MetaData, Table, select, join, func, union_all, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from TargetSchema import (
    Base,
    DimDate, DimFilm, DimActor, DimCategory, DimStore, DimCustomer,
    BridgeFilmActor, BridgeFilmCategory,
    FactRental, FactPayment,
)

MYSQL_DSN = "mysql+pymysql://root:@127.0.0.1:3306/sakila"
SQLITE_DSN = "sqlite:///analytics.sqlite3"

BATCH = 2000



def yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day

def quarter(m: int) -> int:
    return (m - 1) // 3 + 1

def day_of_week_python(d: date) -> int:
    # Monday=0 .. Sunday=6
    return d.weekday()

def is_weekend(d: date) -> bool:
    return day_of_week_python(d) >= 5

def upsert_sqlite(session, model, unique_cols: List[str], rows: List[dict]):
    if not rows:
        return
    tbl = model.__table__
    stmt = sqlite_insert(tbl).values(rows)
    update_cols = {
        c.name: getattr(stmt.excluded, c.name)
        for c in tbl.c
        if c.name not in unique_cols
    }

    if not update_cols:
        stmt = stmt.on_conflict_do_nothing(index_elements=unique_cols)
    else:
        stmt = stmt.on_conflict_do_update(index_elements=unique_cols, set_=update_cols)

    session.execute(stmt)

def chunked(iterable: Iterable, size: int):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def reflect_sakila_tables(src_engine):
    md = MetaData()

    tables = {}
    for name in [
        "film", "language", "actor", "film_actor",
        "category", "film_category",
        "store", "staff", "customer",
        "address", "city", "country",
        "inventory", "rental", "payment",
    ]:
        tables[name] = Table(name, md, autoload_with=src_engine)
    return tables



#load
def load_dim_date(src_engine, tgt_session, t):
    rental = t["rental"]
    payment = t["payment"]

    pay_col = payment.c.payment_date 

    u = union_all(
        select(func.date(rental.c.rental_date).label("d")),
        select(func.date(rental.c.return_date).label("d")).where(rental.c.return_date.isnot(None)),
        select(func.date(pay_col).label("d")),
    ).subquery()

    q = select(u.c.d).where(u.c.d.isnot(None)).distinct()

    rows = []
    with src_engine.connect() as conn:
        for (dval,) in conn.execute(q):
            if isinstance(dval, str):
                d = datetime.fromisoformat(dval).date()
            elif isinstance(dval, datetime):
                d = dval.date()
            else:
                d = dval  # date

            rows.append({
                "date_key": yyyymmdd(d),
                "date": d,
                "year": d.year,
                "quarter": quarter(d.month),
                "month": d.month,
                "day_of_month": d.day,
                "day_of_week": day_of_week_python(d),
                "is_weekend": is_weekend(d),
            })

    for batch in chunked(rows, BATCH):
        upsert_sqlite(tgt_session, DimDate, ["date"], batch)

    tgt_session.commit()

def load_dim_film(src_engine, tgt_session, t):
    film = t["film"]
    language = t["language"]

    q = (
        select(
            film.c.film_id, film.c.title, film.c.rating, film.c.length,
            film.c.release_year, film.c.last_update,
            language.c.name.label("language"),
        )
        .select_from(film.join(language, film.c.language_id == language.c.language_id))
    )

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            buf.append({
                "film_id": r.film_id,
                "title": r.title,
                "rating": getattr(r, "rating", None),
                "length": getattr(r, "length", None),
                "release_year": getattr(r, "release_year", None),
                "language": r.language,
                "last_update": getattr(r, "last_update", None),
            })
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, DimFilm, ["film_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, DimFilm, ["film_id"], buf)
        tgt_session.commit()


def load_dim_actor(src_engine, tgt_session, t):
    actor = t["actor"]
    q = select(actor.c.actor_id, actor.c.first_name, actor.c.last_name, actor.c.last_update)

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            buf.append({
                "actor_id": r.actor_id,
                "first_name": r.first_name,
                "last_name": r.last_name,
                "last_update": getattr(r, "last_update", None),
            })
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, DimActor, ["actor_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, DimActor, ["actor_id"], buf)
        tgt_session.commit()


def load_dim_category(src_engine, tgt_session, t):
    category = t["category"]
    q = select(category.c.category_id, category.c.name, category.c.last_update)

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            buf.append({
                "category_id": r.category_id,
                "name": r.name,
                "last_update": getattr(r, "last_update", None),
            })
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, DimCategory, ["category_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, DimCategory, ["category_id"], buf)
        tgt_session.commit()


def load_dim_store(src_engine, tgt_session, t):
    store = t["store"]
    address = t["address"]
    city = t["city"]
    country = t["country"]

    q = (
        select(
            store.c.store_id,
            city.c.city.label("city"),
            country.c.country.label("country"),
            store.c.last_update,
        )
        .select_from(
            store.join(address, store.c.address_id == address.c.address_id)
                 .join(city, address.c.city_id == city.c.city_id)
                 .join(country, city.c.country_id == country.c.country_id)
        )
    )

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            buf.append({
                "store_id": r.store_id,
                "city": r.city,
                "country": r.country,
                "last_update": getattr(r, "last_update", None),
            })
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, DimStore, ["store_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, DimStore, ["store_id"], buf)
        tgt_session.commit()


def load_dim_customer(src_engine, tgt_session, t):
    customer = t["customer"]
    address = t["address"]
    city = t["city"]
    country = t["country"]

    q = (
        select(
            customer.c.customer_id,
            customer.c.first_name,
            customer.c.last_name,
            customer.c.active,
            city.c.city.label("city"),
            country.c.country.label("country"),
            customer.c.last_update,
        )
        .select_from(
            customer.join(address, customer.c.address_id == address.c.address_id)
                    .join(city, address.c.city_id == city.c.city_id)
                    .join(country, city.c.country_id == country.c.country_id)
        )
    )

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            buf.append({
                "customer_id": r.customer_id,
                "first_name": r.first_name,
                "last_name": r.last_name,
                "active": bool(r.active),
                "city": r.city,
                "country": r.country,
                "last_update": getattr(r, "last_update", None),
            })
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, DimCustomer, ["customer_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, DimCustomer, ["customer_id"], buf)
        tgt_session.commit()


def build_key_maps(tgt_session) -> Tuple[Dict[int, int], Dict[int, int], Dict[int, int], Dict[int, int], Dict[int, int]]:
    film_map = {fid: fkey for (fid, fkey) in tgt_session.execute(select(DimFilm.film_id, DimFilm.film_key)).all()}
    actor_map = {aid: akey for (aid, akey) in tgt_session.execute(select(DimActor.actor_id, DimActor.actor_key)).all()}
    cat_map  = {cid: ckey for (cid, ckey) in tgt_session.execute(select(DimCategory.category_id, DimCategory.category_key)).all()}
    store_map = {sid: skey for (sid, skey) in tgt_session.execute(select(DimStore.store_id, DimStore.store_key)).all()}
    cust_map = {cid: ckey for (cid, ckey) in tgt_session.execute(select(DimCustomer.customer_id, DimCustomer.customer_key)).all()}
    return film_map, actor_map, cat_map, store_map, cust_map


def load_bridges(src_engine, tgt_session, t, film_map, actor_map, cat_map):
    film_actor = t["film_actor"]
    film_category = t["film_category"]

    # bridge_film_actor
    q1 = select(film_actor.c.film_id, film_actor.c.actor_id)
    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q1):
            fk = film_map.get(r.film_id)
            ak = actor_map.get(r.actor_id)
            if fk is None or ak is None:
                continue
            buf.append({"film_key": fk, "actor_key": ak})
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, BridgeFilmActor, ["film_key", "actor_key"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, BridgeFilmActor, ["film_key", "actor_key"], buf)
        tgt_session.commit()

    # bridge_film_category
    q2 = select(film_category.c.film_id, film_category.c.category_id)
    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q2):
            fk = film_map.get(r.film_id)
            ck = cat_map.get(r.category_id)
            if fk is None or ck is None:
                continue
            buf.append({"film_key": fk, "category_key": ck})
            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, BridgeFilmCategory, ["film_key", "category_key"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, BridgeFilmCategory, ["film_key", "category_key"], buf)
        tgt_session.commit()


def load_fact_rental(src_engine, tgt_session, t, film_map, store_map, cust_map):
    rental = t["rental"]
    inventory = t["inventory"]
    staff = t["staff"]

    # rental -> inventory(film_id) ; rental -> staff(store_id)
    q = (
    select(
        rental.c.rental_id,
        rental.c.rental_date,
        rental.c.return_date,
        rental.c.customer_id,
        rental.c.staff_id,
        inventory.c.film_id,
        inventory.c.store_id,  
        )
        .select_from(
            rental.join(inventory, rental.c.inventory_id == inventory.c.inventory_id)
        )
    )

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            # dates -> date_key
            rd = r.rental_date.date() if isinstance(r.rental_date, datetime) else r.rental_date
            dd_rented = yyyymmdd(rd)

            if r.return_date is not None:
                ret = r.return_date.date() if isinstance(r.return_date, datetime) else r.return_date
                dd_returned = yyyymmdd(ret)
                duration = (ret - rd).days
            else:
                dd_returned = None
                duration = None

            fk = film_map.get(r.film_id)
            sk = store_map.get(r.store_id)
            ck = cust_map.get(r.customer_id)
            if fk is None or sk is None or ck is None:
                continue

            buf.append({
                "rental_id": r.rental_id,
                "date_key_rented": dd_rented,
                "date_key_returned": dd_returned,
                "film_key": fk,
                "store_key": sk,
                "customer_key": ck,
                "staff_id": r.staff_id,
                "rental_duration_days": duration,
            })

            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, FactRental, ["rental_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, FactRental, ["rental_id"], buf)
        tgt_session.commit()


def load_fact_payment(src_engine, tgt_session, t, store_map, cust_map):
    payment = t["payment"]
    staff = t["staff"]

    # payment -> staff(store_id)
    q = (
        select(
            payment.c.payment_id,
            payment.c.payment_date,
            payment.c.customer_id,
            payment.c.staff_id,
            payment.c.amount,
            staff.c.store_id,
        )
        .select_from(payment.join(staff, payment.c.staff_id == staff.c.staff_id))
    )

    buf = []
    with src_engine.connect() as conn:
        for r in conn.execute(q):
            pd = r.payment_date.date() if isinstance(r.payment_date, datetime) else r.payment_date
            dd_paid = yyyymmdd(pd)

            sk = store_map.get(r.store_id)
            ck = cust_map.get(r.customer_id)
            if sk is None or ck is None:
                continue

            buf.append({
                "payment_id": r.payment_id,
                "date_key_paid": dd_paid,
                "customer_key": ck,
                "store_key": sk,
                "staff_id": r.staff_id,
                "amount": float(r.amount),
            })

            if len(buf) >= BATCH:
                upsert_sqlite(tgt_session, FactPayment, ["payment_id"], buf)
                tgt_session.commit()
                buf = []
    if buf:
        upsert_sqlite(tgt_session, FactPayment, ["payment_id"], buf)
        tgt_session.commit()

def debug_counts(tgt_engine):
    with tgt_engine.connect() as c:
        for t in ["dim_film","dim_store","dim_customer","fact_rental","fact_payment"]:
            n = c.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one()
            print(f"[DEBUG] {t} rows =", n)

def main(src_dsn: str, tgt_dsn: str):
    src_engine = create_engine(src_dsn, future=True)
    tgt_engine = create_engine(tgt_dsn, future=True)

    # ensure target schema exists
    Base.metadata.create_all(tgt_engine)

    tables = reflect_sakila_tables(src_engine)
    TgtSession = sessionmaker(bind=tgt_engine, future=True)

    with TgtSession() as ts:
        print("1 Load dim_date")
        load_dim_date(src_engine, ts, tables)

        print("2 Load dimensions")
        load_dim_film(src_engine, ts, tables)
        load_dim_actor(src_engine, ts, tables)
        load_dim_category(src_engine, ts, tables)
        load_dim_store(src_engine, ts, tables)
        load_dim_customer(src_engine, ts, tables)

        print("3 Build key maps")
        film_map, actor_map, cat_map, store_map, cust_map = build_key_maps(ts)

        print("4 Load bridges")
        load_bridges(src_engine, ts, tables, film_map, actor_map, cat_map)

        print("5 Load facts")
        load_fact_rental(src_engine, ts, tables, film_map, store_map, cust_map)
        load_fact_payment(src_engine, ts, tables, store_map, cust_map)

    print("full load done.")
    debug_counts(tgt_engine)

if __name__ == "__main__":
    main()
