from sqlalchemy import text

def create_sqlite_indexes(tgt_engine):
    ddl = [
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_film_film_id ON dim_film(film_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_actor_actor_id ON dim_actor(actor_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_category_category_id ON dim_category(category_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_store_store_id ON dim_store(store_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_customer_id ON dim_customer(customer_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_date_date ON dim_date(date);",

        
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_rental_rental_id ON fact_rental(rental_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_payment_payment_id ON fact_payment(payment_id);",

        "CREATE INDEX IF NOT EXISTS ix_fact_rental_film_key ON fact_rental(film_key);",
        "CREATE INDEX IF NOT EXISTS ix_fact_rental_store_key ON fact_rental(store_key);",
        "CREATE INDEX IF NOT EXISTS ix_fact_rental_customer_key ON fact_rental(customer_key);",
        "CREATE INDEX IF NOT EXISTS ix_fact_rental_date_rented ON fact_rental(date_key_rented);",
        "CREATE INDEX IF NOT EXISTS ix_fact_rental_date_returned ON fact_rental(date_key_returned);",

        "CREATE INDEX IF NOT EXISTS ix_fact_payment_store_key ON fact_payment(store_key);",
        "CREATE INDEX IF NOT EXISTS ix_fact_payment_customer_key ON fact_payment(customer_key);",
        "CREATE INDEX IF NOT EXISTS ix_fact_payment_date_paid ON fact_payment(date_key_paid);",
    ]

    with tgt_engine.begin() as conn:
        for s in ddl:
            conn.execute(text(s))

def validate_after_sync(src_engine, tgt_engine, warn_ratio=0.001, hard_ratio=0.01):
    def rel_err(a, b):
        if a == b == 0:
            return 0.0
        denom = max(1.0, float(abs(a)))
        return abs(float(a) - float(b)) / denom

    with src_engine.connect() as src, tgt_engine.connect() as tgt:

        src_rental_cnt = src.execute(text("SELECT COUNT(*) FROM rental")).scalar_one()
        tgt_rental_cnt = tgt.execute(text("SELECT COUNT(*) FROM fact_rental")).scalar_one()

        e = rel_err(src_rental_cnt, tgt_rental_cnt)
        if e > hard_ratio:
            raise RuntimeError(f"[ERROR] rental count mismatch: source={src_rental_cnt}, target={tgt_rental_cnt}, rel_err={e:.4%}")
        elif e > warn_ratio:
            print(f"[WARN] rental count mismatch: source={src_rental_cnt}, target={tgt_rental_cnt}, rel_err={e:.4%}")


        src_pay_cnt = src.execute(text("SELECT COUNT(*) FROM payment")).scalar_one()
        tgt_pay_cnt = tgt.execute(text("SELECT COUNT(*) FROM fact_payment")).scalar_one()

        e = rel_err(src_pay_cnt, tgt_pay_cnt)
        if e > hard_ratio:
            raise RuntimeError(f"[ERROR] payment count mismatch: source={src_pay_cnt}, target={tgt_pay_cnt}, rel_err={e:.4%}")
        elif e > warn_ratio:
            print(f"[WARN] payment count mismatch: source={src_pay_cnt}, target={tgt_pay_cnt}, rel_err={e:.4%}")


        src_rows = src.execute(text("""
            SELECT s.store_id AS store_id,
                   COUNT(*) AS cnt,
                   ROUND(SUM(p.amount), 2) AS total_amount
            FROM payment p
            JOIN staff s ON p.staff_id = s.staff_id
            GROUP BY s.store_id
            ORDER BY s.store_id
        """)).all()

        tgt_rows = tgt.execute(text("""
            SELECT ds.store_id AS store_id,
                   COUNT(*) AS cnt,
                   ROUND(SUM(fp.amount), 2) AS total_amount
            FROM fact_payment fp
            JOIN dim_store ds ON fp.store_key = ds.store_key
            GROUP BY ds.store_id
            ORDER BY ds.store_id
        """)).all()

        src_map = {r.store_id: (r.cnt, float(r.total_amount or 0.0)) for r in src_rows}
        tgt_map = {r.store_id: (r.cnt, float(r.total_amount or 0.0)) for r in tgt_rows}

        all_stores = sorted(set(src_map.keys()) | set(tgt_map.keys()))
        for sid in all_stores:
            sc, sa = src_map.get(sid, (0, 0.0))
            tc, ta = tgt_map.get(sid, (0, 0.0))

            ec = rel_err(sc, tc)
            ea = rel_err(sa, ta)

            if ec > hard_ratio or ea > hard_ratio:
                raise RuntimeError(
                    f"[ERROR] store {sid} payment mismatch: "
                    f"cnt source={sc}, target={tc}, rel_err={ec:.4%}; "
                    f"amt source={sa:.2f}, target={ta:.2f}, rel_err={ea:.4%}"
                )
            elif ec > warn_ratio or ea > warn_ratio:
                print(
                    f"[WARN] store {sid} payment mismatch: "
                    f"cnt source={sc}, target={tc}, rel_err={ec:.4%}; "
                    f"amt source={sa:.2f}, target={ta:.2f}, rel_err={ea:.4%}"
                )
