import pandas as pd
from load.db import get_engine
from sqlalchemy import text

def audit_nulls():
    engine = get_engine()
    tables = [
        "dim_municipio", "dim_cultivo", "dim_estacion_ideam", "dim_central_abastos",
        "fact_produccion_agricola", "fact_clima_mensual", "fact_precios_mayoristas",
        "fact_aptitud_suelo", "fact_censo_agropecuario"
    ]
    
    report = []
    
    with engine.connect() as conn:
        for table in tables:
            try:
                # Obtener conteo total
                total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                if total == 0:
                    report.append({"tabla": table, "columna": "TODAS", "nulos": 0, "pct": 0, "total": 0})
                    continue
                
                # Obtener columnas
                res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"))
                columns = [row[0] for row in res]
                
                for col in columns:
                    null_count = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")).scalar()
                    report.append({
                        "tabla": table,
                        "columna": col,
                        "nulos": null_count,
                        "total": total,
                        "pct": round((null_count / total) * 100, 2)
                    })
            except Exception as e:
                print(f"Error auditing {table}: {e}")
                
    df = pd.DataFrame(report)
    df_nulos = df[df["nulos"] > 0].sort_values(by="pct", ascending=False)
    print(df_nulos.to_string(index=False))

if __name__ == "__main__":
    audit_nulls()
