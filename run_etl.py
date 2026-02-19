# run_etll.py
import argparse
import sys
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


from TargetSchema import Base


try:
    from InitialFullLoad import main as full_load_job
except Exception:
    full_load_job = None

from incremental_load import incremental_job

try:
    from etl_utils import create_sqlite_indexes, validate_after_sync
except Exception:
    create_sqlite_indexes = None
    validate_after_sync = None




def cmd_init(src_dsn: str, tgt_dsn: str):
    """
    Init:
      - verify MySQL connection
      - create SQLite schema (ORM models)
      - ensure sync_state table exists (etl_state)
      - optionally create indexes
    """
    try:
        src_engine = create_engine(src_dsn, future=True)
        tgt_engine = create_engine(tgt_dsn, future=True)

        # connection
        with src_engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # create schema 
        Base.metadata.create_all(tgt_engine)


        # indexes (optional)
        if create_sqlite_indexes is not None:
            create_sqlite_indexes(tgt_engine)
            print("SQLite indexes created/verified.")
        else:
            print("create_sqlite_indexes() not found; skipping index creation.")

        print("Init completed.")
        return 0

    except SQLAlchemyError as e:
        print(f"Init failed (SQLAlchemyError): {e}")
        return 2
    except Exception as e:
        print(f"Init failed: {e}")
        return 2


def cmd_full_load(src_dsn: str, tgt_dsn: str):
    """
    Full-load:
      - run complete import from MySQL -> SQLite
    """
    if full_load_job is None:
        print("Full-load entry not found. Ensure InitialFullLoad.py exposes main().")
        return 2

    try:

        full_load_job(src_dsn, tgt_dsn)

        # indexes + validation
        tgt_engine = create_engine(tgt_dsn, future=True)
        src_engine = create_engine(src_dsn, future=True)

        if create_sqlite_indexes is not None:
            create_sqlite_indexes(tgt_engine)
            print("SQLite indexes created/verified.")

        if validate_after_sync is not None:
            validate_after_sync(src_engine, tgt_engine)
            print("Validation passed.")
        else:
            print("validate_after_sync() not found; skipping validation.")

        return 0

    except Exception as e:
        print(f"Full-load failed: {e}")
        return 2


def cmd_incremental(src_dsn: str, tgt_dsn: str, batch: int, backfill_days: int):
    """
    Incremental:
      - load only new/changed data since last watermark
    """
    try:
        incremental_job(
            src_dsn=src_dsn,
            tgt_dsn=tgt_dsn,
            batch=batch,
            backfill_days=backfill_days,
        )
        print("Incremental load completed.")

        tgt_engine = create_engine(tgt_dsn, future=True)
        src_engine = create_engine(src_dsn, future=True)

        if create_sqlite_indexes is not None:
            create_sqlite_indexes(tgt_engine)
            print("SQLite indexes created/verified.")

        if validate_after_sync is not None:
            validate_after_sync(src_engine, tgt_engine)
            print("Validation passed.")
        else:
            print("validate_after_sync() not found; skipping validation.")

        return 0

    except Exception as e:
        print(f"Incremental load failed: {e}")
        return 2


def cmd_validate(src_dsn: str, tgt_dsn: str, days: int):
    """
    Validate:
      - compare counts and totals over selected period (default last 30 days)
    """
    if validate_after_sync is None:
        print("validate_after_sync() not found. Implement it (counts/totals comparison).")
        return 2

    try:
        src_engine = create_engine(src_dsn, future=True)
        tgt_engine = create_engine(tgt_dsn, future=True)


        try:
            validate_after_sync(src_engine, tgt_engine, days=days)
        except TypeError:

            validate_after_sync(src_engine, tgt_engine)

        print(f"Validate completed (period: last {days} days).")
        return 0

    except Exception as e:
        print(f"Validate failed: {e}")
        return 2


def build_parser():
    p = argparse.ArgumentParser(prog="run_etll.py", description="Sakila -> SQLite analytics ETL CLI")
    p.add_argument("--src-dsn",default="mysql+pymysql://root:@127.0.0.1:3306/sakila")
    p.add_argument("--tgt-dsn", default="sqlite:///analytics.sqlite3")

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init")

    sub.add_parser("full-load")

    inc = sub.add_parser("incremental")
    inc.add_argument("--batch", type=int, default=2000)
    inc.add_argument("--backfill-days", type=int, default=30)

    val = sub.add_parser("validate")
    val.add_argument("--days", type=int, default=30, help="Compare over last N days (default 30).")

    return p


def main():
    args = build_parser().parse_args()

    if args.cmd == "init":
        rc = cmd_init(args.src_dsn, args.tgt_dsn)
    elif args.cmd == "full-load":
        rc = cmd_full_load(args.src_dsn, args.tgt_dsn)
    elif args.cmd == "incremental":
        rc = cmd_incremental(args.src_dsn, args.tgt_dsn, args.batch, args.backfill_days)
    elif args.cmd == "validate":
        rc = cmd_validate(args.src_dsn, args.tgt_dsn, args.days)
    else:
        print(f"Unknown command: {args.cmd}")
        rc = 2

    sys.exit(rc)


if __name__ == "__main__":
    main()
