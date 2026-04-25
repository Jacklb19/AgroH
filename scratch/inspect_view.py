from load.db import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    res = conn.execute(text("SELECT definition FROM pg_views WHERE viewname = 'v_dashboard_agro'")).scalar()
    print(res)
