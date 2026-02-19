# incremental_load.py
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import (
    create_engine, MetaData, Table, select, func, union_all
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from etl_utils import create_sqlite_indexes, validate_after_sync
from TargetSchema import (
    Base,
    DimDate, DimFilm, DimActor, DimCategory, DimStore, DimCustomer,
    BridgeFilmActor, BridgeFilmCategory,
    FactRental, FactPayment,
    EtlState,
)

# -----------------------------
# Defaults (can be overridden by run_etll.py args)
# -----------------------------
DEFAULT_BATCH = 2000
DEFAULT_BACKFILL_DAYS = 30


# -----------------------------
# Helpers
# -----------------------------
def yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day

def quarter(m: int) -> int:
    return (m - 1) // 3 + 1

def day_of_week_python(d: date) -> int:
    # Monday=0 .. Sunday=6
    return d.weekday()

def is_weekend(d: date) -> bool:
    return day_of_week_python(d) >= 5

def chunked(iterable: Iterable[dict], size: int):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_sqlite(session, model, unique_cols: List[str], rows: List[dict]):
    """
    SQLite upsert:
      - if there are non-unique cols -> DO UPDATE
      - if table only has unique cols (bridge tables) -> DO NOTHING
    """
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

def src_iter(src_engine, stmt):
    with src_engine.connect() as conn:
        yield from conn.execute(stmt)


# -----------------------------
# ETL State (watermark)
# -----------------------------
def get_watermark(session, name: str) -> Optional[datetime]:
    row = session.get(EtlState, name)
    return row.watermark if row else None

def set_watermark(session, name: str, value: Optional[datetime]):
    row = session.get(EtlState, name)
    if row is None:
        session.add(EtlState(name=name, watermark=value))
    else:
        row.watermark = value


# -----------------------------
# Source table reflection
# -----------------------------
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


# -----------------------------
# Key maps
# -----------------------------
def build_key_maps(tgt_session) -> Tuple[Dict[int, int], Dict[int, int], Dict[int, int], Dict[int, int], Dict[int, int]]:
    film_map = {fid: fkey for (fid, fkey) in tgt_session.execute(select(DimFilm.film_id, DimFilm.film_key)).all()}
    actor_map = {aid: akey for (aid, akey) in tgt_session.execute(select(DimActor.actor_id, DimActor.actor_key)).all()}
    cat_map  = {cid: ckey for (cid, ckey) in tgt_session.execute(select(DimCategory.category_id, DimCategory.category_key)).all()}
    store_map = {sid: skey for (sid, skey) in tgt_session.execute(select(DimStore.store_id, DimStore.store_key)).all()}
    cust_map = {cid: ckey for (cid, ckey) in tgt_session.execute(select(DimCustomer.customer_id, DimCustomer.customer_key)).all()}
    return film_map, actor_map, cat_map, store_map, cust_map


# -----------------------------
# dim_date (safe to rerun full, small)
# -----------------------------
def ensure_dim_date(src_engine, tgt_session, t, batch: int):
    rental = t["rental"]
    payment = t["payment"]

    # If your column name differs, change this:
    pay_col = payment.c.payment_date

    u = union_all(
        select(func.date(rental.c.rental_date).label("d")),
        select(func.date(rental.c.return_date).label("d")).where(rental.c.return_date.isnot(None)),
        select(func.date(pay_col).label("d")),
    ).subquery()

    q = select(u.c.d).where(u.c.d.isnot(None)).distinct()

    out_rows = []
    for (dval,) in src_iter(src_engine, q):
        if dval is None:
            continue
        if isinstance(dval, str):
            d = datetime.fromisoformat(dval).date()
        elif isinstance(dval, datetime):
            d = dval.date()
        else:
            d = dval  # date

        out_rows.append({
            "date_key": yyyymmdd(d),
            "date": d,
            "year": d.year,
            "quarter": quarter(d.month),
            "month": d.month,
            "day_of_month": d.day,
            "day_of_week": day_of_week_python(d),
            "is_weekend": is_weekend(d),
        })

    for b in chunked(out_rows, batch):
        upsert_sqlite(tgt_session, DimDate, ["date"], b)
    tgt_session.commit()


# -----------------------------
# Incremental dims (watermark by last_update)
# -----------------------------
def incr_dim_film(src_engine, tgt_session, t, wm: Optional[datetime], batch: int) -> Optional[datetime]:
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
    if wm is not None:
        q = q.where(film.c.last_update > wm)

    buf: List[dict] = []
    max_ts = wm

    for r in src_iter(src_engine, q):
        ts = getattr(r, "last_update", None)
        if ts is not None and (max_ts is None or ts > max_ts):
            max_ts = ts

        buf.append({
            "film_id": r.film_id,
            "title": r.title,
            "rating": getattr(r, "rating", None),
            "length": getattr(r, "length", None),
            "release_year": getattr(r, "release_year", None),
            "language": r.language,
            "last_update": ts,
        })

        if len(buf) >= batch:
            upsert_sqlite(tgt_session, DimFilm, ["film_id"], buf)
            tgt_session.commit()
            buf = []

    if buf:
        upsert_sqlite(tgt_session, DimFilm, ["film_id"], buf)
        tgt_session.commit()

    return max_ts


def incr_dim_actor(src_engine, tgt_session, t, wm: Optional[datetime], batch: int) -> Optional[datetime]:
    actor = t["actor"]
    q = select(actor.c.actor_id, actor.c.first_name, actor.c.last_name, actor.c.last_update)
    if wm is not None:
        q = q.where(actor.c.last_update > wm)

    buf: List[dict] = []
    max_ts = wm

    for r in src_iter(src_engine, q):
        ts = getattr(r, "last_update", None)
        if ts is not None and (max_ts is None or ts > max_ts):
            max_ts = ts

        buf.append({
            "actor_id": r.actor_id,
            "first_name": r.first_name,
            "last_name": r.last_name,
            "last_update": ts,
        })

        if len(buf) >= batch:
            upsert_sqlite(tgt_session, DimActor, ["actor_id"], buf)
            tgt_session.commit()
            buf = []

    if buf:
        upsert_sqlite(tgt_session, DimActor, ["actor_id"], buf)
        tgt_session.commit()

    return max_ts


def incr_dim_category(src_engine, tgt_session, t, wm: Optional[datetime], batch: int) -> Optional[datetime]:
    category = t["category"]
    q = select(category.c.category_id, category.c.name, category.c.last_update)
    if wm is not None:
        q = q.where(category.c.last_update > wm)

    buf: List[dict] = []
    max_ts = wm

    for r in src_iter(src_engine, q):
        ts = getattr(r, "last_update", None)
        if ts is not None and (max_ts is None or ts > max_ts):
            max_ts = ts

        buf.append({
            "category_id": r.category_id,
            "name": r.name,
            "last_update": ts,
        })

        if len(buf) >= batch:
            upsert_sqlite(tgt_session, DimCategory, ["category_id"], buf)
            tgt_session.commit()
            buf = []

    if buf:
        upsert_sqlite(tgt_session, DimCategory, ["category_id"], buf)
        tgt_session.commit()

    return max_ts


def incr_dim_store(src_engine, tgt_session, t, wm: Optional[datetime], batch: int) -> Optional[datetime]:
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
    if wm is not None:
        q = q.where(store.c.last_update > wm)

    buf: List[dict] = []
    max_ts = wm

    for r in src_iter(src_engine, q):
        ts = getattr(r, "last_update", None)
        if ts is not None and (max_ts is None or ts > max_ts):
            max_ts = ts

        buf.append({
            "store_id": r.store_id,
            "city": r.city,
            "country": r.country,
            "last_update": ts,
        })

        if len(buf) >= batch:
            upsert_sqlite(tgt_session, DimStore, ["store_id"], buf)
            tgt_session.commit()
            buf = []

    if buf:
        upsert_sqlite(tgt_session, DimStore, ["store_id"], buf)
        tgt_session.commit()

    return max_ts


def incr_dim_customer(src_engine, tgt_session, t, wm: Optional[datetime], batch: int) -> Optional[datetime]:
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
    if wm is not None:
        q = q.where(customer.c.last_update > wm)

    buf: List[dict] = []
    max_ts = wm

    for r in src_iter(src_engine, q):
        ts = getattr(r, "last_update", None)
        if ts is not None and (max_ts is None or ts > max_ts):
            max_ts = ts

        buf.append({
            "customer_id": r.customer_id,
            "first_name": r.first_name,
            "last_name": r.last_name,
            "active": bool(r.active),
            "city": r.city,
            "country": r.country,
            "last_update": ts,
        })

        if len(buf) >= batch:
            upsert_sqlite(tgt_session, DimCustomer, ["customer_id"], buf)
            tgt_session.commit()
            buf = []

    if buf:
        upsert_sqlite(tgt_session, DimCustomer, ["customer_id"], buf)
        tgt_session.commit()

    return max_ts


# -----------------------------
# Bridges (rerunnable: insert all, conflict -> do nothing)
# -----------------------------
def rerun_bridges(src_engine, tgt_session, t, film_map, actor_map, cat_map, batch: int):
    film_actor = t["film_actor"]
    film_category = t["film_category"]

    # film_actor
    q1 = select(film_actor.c.film_id, film_actor.c.actor_id)
    buf: List[dict] = []
    for r in src_iter(src_engine, q1):
        fk = film_map.get(r.film_id)
        ak = actor_map.get(r.actor_id)
        if fk is None or ak is None:
            continue
        buf.append({"film_key": fk, "actor_key": ak})
        if len(buf) >= batch:
            upsert_sqlite(tgt_session, BridgeFilmActor, ["film_key", "actor_key"], buf)
            tgt_session.commit()
            buf = []
    if buf:
        upsert_sqlite(tgt_session, BridgeFilmActor, ["film_key", "actor_key"], buf)
        tgt_session.commit()

    # film_category
    q2 = select(film_category.c.film_id, film_category.c.category_id)
    buf = []
    for r in src_iter(src_engine, q2):
        fk = film_map.get(r.film_id)
        ck = cat_map.get(r.category_id)
        if fk is None or ck is None:
            continue
        buf.append({"film_key": fk, "category_key": ck})
        if len(buf) >= batch:
            upsert_sqlite(tgt_session, BridgeFilmCategory, ["film_key", "category_key"], buf)
            tgt_session.commit()
            buf = []
    if buf:
        upsert_sqlite(tgt_session, BridgeFilmCategory, ["film_key", "category_key"], buf)
        tgt_session.commit()


# -----------------------------
# Incremental facts
# -----------------------------
def incr_fact_payment(src_engine, tgt_session, t, store_map, cust_map, wm: Optional[datetime], batch: int) -> Optional[datetime]:
    payment = t["payment"]
    staff = t["staff"]

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
    if wm is not None:
        q = q.where(payment.c.payment_date > wm)

    buf: List[dict] = []
    max_ts = wm

    for r in src_iter(src_engine, q):
        ts = r.payment_date
        if ts is not None and (max_ts is None or ts > max_ts):
            max_ts = ts

        pd = ts.date() if isinstance(ts, datetime) else ts
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

        if len(buf) >= batch:
            upsert_sqlite(tgt_session, FactPayment, ["payment_id"], buf)
            tgt_session.commit()
            buf = []

    if buf:
        upsert_sqlite(tgt_session, FactPayment, ["payment_id"], buf)
        tgt_session.commit()

    return max_ts


def incr_fact_rental(
    src_engine,
    tgt_session,
    t,
    film_map,
    store_map,
    cust_map,
    wm: Optional[datetime],
    batch: int,
    backfill_days: int,
) -> Optional[datetime]:
    rental = t["rental"]
    inventory = t["inventory"]
    staff = t["staff"]

    base = (
        select(
            rental.c.rental_id,
            rental.c.rental_date,
            rental.c.return_date,
            rental.c.customer_id,
            rental.c.staff_id,
            inventory.c.film_id,
            staff.c.store_id,
        )
        .select_from(
            rental.join(inventory, rental.c.inventory_id == inventory.c.inventory_id)
                  .join(staff, rental.c.staff_id == staff.c.staff_id)
        )
    )

    q_new = base
    if wm is not None:
        q_new = q_new.where(rental.c.rental_date > wm)

    q_backfill = None
    if wm is not None and backfill_days > 0:
        q_backfill = base.where(rental.c.rental_date > (wm - timedelta(days=backfill_days)))

    max_ts = wm

    def process(q):
        nonlocal max_ts
        buf: List[dict] = []
        for r in src_iter(src_engine, q):
            rd_dt = r.rental_date
            if rd_dt is not None and (max_ts is None or rd_dt > max_ts):
                max_ts = rd_dt

            rd = rd_dt.date() if isinstance(rd_dt, datetime) else rd_dt
            dd_rented = yyyymmdd(rd)

            if r.return_date is not None:
                ret_dt = r.return_date
                ret = ret_dt.date() if isinstance(ret_dt, datetime) else ret_dt
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

            if len(buf) >= batch:
                upsert_sqlite(tgt_session, FactRental, ["rental_id"], buf)
                tgt_session.commit()
                buf = []

        if buf:
            upsert_sqlite(tgt_session, FactRental, ["rental_id"], buf)
            tgt_session.commit()

    process(q_new)
    if q_backfill is not None:
        process(q_backfill)

    return max_ts


# -----------------------------
# Main incremental job
# -----------------------------
def incremental_job(
    src_dsn: str,
    tgt_dsn: str,
    batch: int = DEFAULT_BATCH,
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
):
    src_engine = create_engine(src_dsn, future=True)
    tgt_engine = create_engine(tgt_dsn, future=True)

    # make sure schema exists (including etl_state)
    Base.metadata.create_all(tgt_engine)

    tables = reflect_sakila_tables(src_engine)
    TgtSession = sessionmaker(bind=tgt_engine, future=True)

    with TgtSession() as ts:
        dims_wm = get_watermark(ts, "dims")
        facts_wm = get_watermark(ts, "facts")

        # 0) dim_date is cheap; rerun to ensure new dates exist
        ensure_dim_date(src_engine, ts, tables, batch=batch)

        # 1) Incremental dims
        max_dim_ts = dims_wm
        for f in [
            incr_dim_film,
            incr_dim_actor,
            incr_dim_category,
            incr_dim_store,
            incr_dim_customer,
        ]:
            new_ts = f(src_engine, ts, tables, dims_wm, batch=batch)
            if new_ts is not None and (max_dim_ts is None or new_ts > max_dim_ts):
                max_dim_ts = new_ts

        set_watermark(ts, "dims", max_dim_ts)
        ts.commit()

        # 2) rebuild maps after dims update
        film_map, actor_map, cat_map, store_map, cust_map = build_key_maps(ts)

        # 3) bridges: rerunnable full insert (conflict -> do nothing)
        rerun_bridges(src_engine, ts, tables, film_map, actor_map, cat_map, batch=batch)

        # 4) facts incremental + backfill for rentals
        max_fact_ts = facts_wm

        new_rental_ts = incr_fact_rental(
            src_engine, ts, tables,
            film_map, store_map, cust_map,
            facts_wm,
            batch=batch,
            backfill_days=backfill_days,
        )
        if new_rental_ts is not None and (max_fact_ts is None or new_rental_ts > max_fact_ts):
            max_fact_ts = new_rental_ts

        new_payment_ts = incr_fact_payment(
            src_engine, ts, tables,
            store_map, cust_map,
            facts_wm,
            batch=batch,
        )
        if new_payment_ts is not None and (max_fact_ts is None or new_payment_ts > max_fact_ts):
            max_fact_ts = new_payment_ts

        set_watermark(ts, "facts", max_fact_ts)
        ts.commit()
    create_sqlite_indexes(tgt_engine)
    validate_after_sync(src_engine, tgt_engine)
    print(" completed.")


if __name__ == "__main__":
    incremental_job(
        src_dsn="mysql+pymysql://root:@127.0.0.1:3306/sakila",
        tgt_dsn="sqlite:///analytics.sqlite3",
    )
