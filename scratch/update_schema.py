import sys; sys.path.insert(0, ".")
from load.db import get_engine, init_schema

engine = get_engine()
init_schema(engine)
print("Schema actualizado con las nuevas vistas para Power BI.")
