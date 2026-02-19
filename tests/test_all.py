import os
import re
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# --- Config ---
RUN_ETL = Path(__file__).resolve().parents[1] / "run_etl.py"
MYSQL_DSN = os.environ.get("HW4_MYSQL_DSN")   # 必须设置
PY = os.environ.get("PYTHON", "python")       # 可选：指定 python 解释器

pytestmark = pytest.mark.skipif(
    not MYSQL_DSN,
    reason="Set env HW4_MYSQL_DSN to run integration tests, e.g. mysql+pymysql://root:pwd@127.0.0.1:3306/sakila",
)

def run_cli(tmp_path: Path, *args: str):
    """
    Run: python run_etl.py --src-dsn ... --tgt-dsn ... <cmd> [args]
    Returns (returncode, stdout+stderr)
    """
    tgt_sqlite = tmp_path / "analytics_test.sqlite3"
    cmd = [
        PY, str(RUN_ETL),
        "--src-dsn", MYSQL_DSN,
        "--tgt-dsn", f"sqlite:///{tgt_sqlite}",
        *args
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode, out, tgt_sqlite

def sqlite_engine(sqlite_path: Path):
    return create_engine(f"sqlite:///{sqlite_path}", future=True)

def mysql_engine():
    return create_engine(MYSQL_DSN, future=True)

def table_exists_sqlite(sqlite_path: Path, table: str) -> bool:
    eng = sqlite_engine(sqlite_path)
    with eng.connect() as c:
        r = c.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
        ), {"t": table}).fetchone()
    return r is not None

def count_sqlite(sqlite_path: Path, table: str) -> int:
    eng = sqlite_engine(sqlite_path)
    with eng.connect() as c:
        return int(c.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())

def count_mysql(table: str) -> int:
    eng = mysql_engine()
    with eng.connect() as c:
        return int(c.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())


# ---------------------------
# 1) Init command test
# ---------------------------
def test_init_creates_tables(tmp_path: Path):
    rc, out, sqlite_path = run_cli(tmp_path, "init")
    assert rc == 0, out
    # 至少验证关键表存在（按 rubric：dim_date + sync_state/etl_state）
    assert table_exists_sqlite(sqlite_path, "dim_date"), "dim_date missing"
    assert table_exists_sqlite(sqlite_path, "etl_state"), "etl_state missing"
    # 还可以加一个事实表/维表
    assert table_exists_sqlite(sqlite_path, "fact_payment"), "fact_payment missing"


# ---------------------------
# 2) Full-load command test
# ---------------------------
def test_full_load_loads_all_data(tmp_path: Path):
    # init then full-load
    rc, out, sqlite_path = run_cli(tmp_path, "init")
    assert rc == 0, out

    rc, out, sqlite_path = run_cli(tmp_path, "full-load")
    assert rc == 0, out

    # 核心验证：MySQL payment/rental 的行数 == SQLite fact_payment/fact_rental
    src_pay = count_mysql("payment")
    src_rent = count_mysql("rental")
    tgt_pay = count_sqlite(sqlite_path, "fact_payment")
    tgt_rent = count_sqlite(sqlite_path, "fact_rental")

    assert tgt_pay == src_pay, f"payment mismatch: src={src_pay}, tgt={tgt_pay}"
    assert tgt_rent == src_rent, f"rental mismatch: src={src_rent}, tgt={tgt_rent}"


# ---------------------------
# Helpers for incremental tests
# ---------------------------
def insert_test_payment():
    """
    Insert a new payment row into MySQL (Sakila).
    Returns payment_id and staff_id/store_id, customer_id, amount.
    Uses existing rental/customer/staff to satisfy FK.
    """
    eng = mysql_engine()
    with eng.begin() as c:
        # pick an existing rental to reuse customer_id and staff_id
        row = c.execute(text("""
            SELECT rental_id, customer_id, staff_id
            FROM rental
            ORDER BY rental_id DESC
            LIMIT 1
        """)).mappings().one()

        # payment_id is auto-increment in sakila; insert with NULL id
        amt = 9.99
        c.execute(text("""
            INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
            VALUES (:customer_id, :staff_id, :rental_id, :amount, NOW())
        """), {
            "customer_id": row["customer_id"],
            "staff_id": row["staff_id"],
            "rental_id": row["rental_id"],
            "amount": amt,
        })

        pid = c.execute(text("SELECT LAST_INSERT_ID()")).scalar_one()

    return int(pid), int(row["customer_id"]), int(row["staff_id"]), float(amt)

def cleanup_payment(payment_id: int):
    eng = mysql_engine()
    with eng.begin() as c:
        c.execute(text("DELETE FROM payment WHERE payment_id=:pid"), {"pid": payment_id})


def update_test_film_title():
    """
    Update an existing film title to force last_update change.
    Returns (film_id, old_title, new_title).
    """
    eng = mysql_engine()
    marker = "HW4_INC_UPDATE"
    with eng.begin() as c:
        film = c.execute(text("""
            SELECT film_id, title
            FROM film
            ORDER BY film_id ASC
            LIMIT 1
        """)).mappings().one()

        film_id = int(film["film_id"])
        old_title = film["title"]
        new_title = f"{old_title} {marker}"

        c.execute(text("""
            UPDATE film
            SET title=:t, last_update=NOW()
            WHERE film_id=:id
        """), {"t": new_title, "id": film_id})

    return film_id, old_title, new_title

def restore_film_title(film_id: int, old_title: str):
    eng = mysql_engine()
    with eng.begin() as c:
        c.execute(text("""
            UPDATE film
            SET title=:t, last_update=NOW()
            WHERE film_id=:id
        """), {"t": old_title, "id": film_id})


# ---------------------------
# 3) Incremental command (new data)
# ---------------------------
def test_incremental_picks_up_new_payment(tmp_path: Path):
    # init + full-load baseline
    rc, out, sqlite_path = run_cli(tmp_path, "init")
    assert rc == 0, out
    rc, out, sqlite_path = run_cli(tmp_path, "full-load")
    assert rc == 0, out

    # insert a new payment in MySQL
    pid, customer_id, staff_id, amt = insert_test_payment()
    try:
        # run incremental
        rc, out, sqlite_path = run_cli(tmp_path, "incremental")
        assert rc == 0, out

        # verify this payment_id exists in SQLite fact_payment
        eng = sqlite_engine(sqlite_path)
        with eng.connect() as c:
            got = c.execute(text("""
                SELECT COUNT(*) FROM fact_payment WHERE payment_id=:pid
            """), {"pid": pid}).scalar_one()
        assert int(got) == 1, f"new payment_id {pid} not found in SQLite"
    finally:
        cleanup_payment(pid)


# ---------------------------
# 4) Incremental command (updates)
# ---------------------------
def test_incremental_updates_existing_dim_row(tmp_path: Path):
    # baseline load
    rc, out, sqlite_path = run_cli(tmp_path, "init")
    assert rc == 0, out
    rc, out, sqlite_path = run_cli(tmp_path, "full-load")
    assert rc == 0, out

    # update film title in MySQL
    film_id, old_title, new_title = update_test_film_title()
    try:
        # run incremental to pick up last_update change
        rc, out, sqlite_path = run_cli(tmp_path, "incremental")
        assert rc == 0, out

        # verify dim_film updated in SQLite
        eng = sqlite_engine(sqlite_path)
        with eng.connect() as c:
            title = c.execute(text("""
                SELECT title FROM dim_film WHERE film_id=:id
            """), {"id": film_id}).scalar_one()
        assert title == new_title, f"dim_film not updated: expected '{new_title}', got '{title}'"
    finally:
        restore_film_title(film_id, old_title)


# ---------------------------
# 5) Validate command
# ---------------------------
def test_validate_command_passes(tmp_path: Path):
    rc, out, sqlite_path = run_cli(tmp_path, "init")
    assert rc == 0, out
    rc, out, sqlite_path = run_cli(tmp_path, "full-load")
    assert rc == 0, out

    rc, out, sqlite_path = run_cli(tmp_path, "validate", "--days", "30")
    assert rc == 0, out
