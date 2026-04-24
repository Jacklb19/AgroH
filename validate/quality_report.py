import pandas as pd
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

CHECKS = [
    {
        "nombre": "municipios_con_cobertura_climatica",
        "sql": """
            SELECT
                COUNT(DISTINCT m.id_municipio) FILTER (WHERE e.id_municipio IS NOT NULL)::FLOAT
                / COUNT(DISTINCT m.id_municipio) * 100 AS pct
            FROM dim_municipio m
            LEFT JOIN dim_estacion_ideam e ON e.id_municipio = m.id_municipio
        """,
        "umbral_min": 80,
        "mensaje": "% municipios con estación climática asignada",
    },
    {
        "nombre": "registros_sin_municipio",
        "sql": "SELECT COUNT(*) FILTER (WHERE id_municipio IS NULL)::FLOAT / COUNT(*) * 100 AS pct FROM fact_produccion_agricola",
        "umbral_max": 0,
        "mensaje": "% registros de producción sin id_municipio",
    },
    {
        "nombre": "municipios_rendimiento_nulo",
        "sql": "SELECT COUNT(*) FILTER (WHERE rendimiento_t_ha IS NULL OR rendimiento_t_ha = 0)::FLOAT / COUNT(*) * 100 AS pct FROM fact_produccion_agricola",
        "umbral_max": 5,
        "mensaje": "% registros de producción con rendimiento 0 o NULL",
    },
    {
        "nombre": "modelos_activos_duplicados",
        "sql": "SELECT COUNT(*) AS pct FROM model_version WHERE activo = TRUE",
        "umbral_max": 1,
        "mensaje": "Número de modelos activos en producción (debe ser ≤ 1)",
    },
]

def run_quality_report(engine) -> pd.DataFrame:
    resultados = []
    with engine.connect() as conn:
        for check in CHECKS:
            try:
                row = conn.execute(text(check["sql"])).fetchone()
                valor = float(row[0]) if row and row[0] is not None else None
                umbral_min = check.get("umbral_min")
                umbral_max = check.get("umbral_max")
                if valor is None:
                    estado = "SIN_DATOS"
                elif umbral_min is not None and valor < umbral_min:
                    estado = "ALERTA"
                elif umbral_max is not None and valor > umbral_max:
                    estado = "ALERTA"
                else:
                    estado = "OK"
                resultados.append({
                    "indicador": check["nombre"],
                    "descripcion": check["mensaje"],
                    "valor": valor,
                    "estado": estado,
                })
                if estado == "ALERTA":
                    logger.warning(f"CALIDAD ALERTA — {check['mensaje']}: {valor:.2f}")
                else:
                    logger.info(f"CALIDAD OK — {check['mensaje']}: {valor:.2f}")
            except Exception as e:
                logger.error(f"Error en check {check['nombre']}: {e}")
    return pd.DataFrame(resultados)
