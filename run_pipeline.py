"""
run_pipeline.py — Orquestador principal del ETL AgroIA Colombia
Ejecutar manualmente:   python run_pipeline.py --once
Ejecutar con scheduler: python run_pipeline.py
"""
import argparse
import logging
import sys
from datetime import datetime
from config.settings import LOGS_DIR

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "etl_run.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")


def run_etl():
    logger.info("=" * 60)
    logger.info(f"INICIO ETL — {datetime.now().isoformat()}")
    logger.info("=" * 60)

    from extract.extract_divipola    import extract_divipola
    from extract.extract_produccion  import extract_produccion
    from extract.extract_ideam_pdf   import extract_all_boletines

    from clean.clean_municipios      import agregar_id_municipio
    from clean.clean_clima           import agregar_clima_mensual, join_espacial_estacion_municipio

    from load.db                     import get_engine, init_schema
    from load.load_dimensions        import (
        load_dim_region_natural, load_dim_tiempo,
        load_dim_municipio, load_dim_cultivo,
        load_dim_estacion_ideam
    )
    from load.load_facts             import load_all_facts
    from validate.quality_report     import run_quality_report

    engine = get_engine()

    # ── Paso 1: Schema ──────────────────────────
    logger.info("Paso 1: Inicializando schema...")
    init_schema(engine)

    # ── Paso 2: Extracción ──────────────────────
    logger.info("Paso 2: Extrayendo fuentes...")
    df_divipola    = extract_divipola()
    df_produccion  = extract_produccion()
    df_boletines   = extract_all_boletines()

    # ── Paso 3: Dimensiones ─────────────────────
    logger.info("Paso 3: Cargando dimensiones...")
    load_dim_region_natural(engine)
    load_dim_tiempo(engine)
    load_dim_municipio(engine, df_divipola, df_region_map=None)
    
    df_cultivos = df_produccion[["cultivo", "grupo_de_cultivo", "ciclo_de_cultivo"]].drop_duplicates()
    df_cultivos = df_cultivos.rename(columns={
        "cultivo": "nombre_cultivo",
        "grupo_de_cultivo": "familia_botanica",
        "ciclo_de_cultivo": "tipo_ciclo"
    })
    df_cultivos["nombre_normalizado"] = df_cultivos["nombre_cultivo"].astype(str).str.upper().str.strip()
    
    # El CHECK de la BD exige 'transitorio' o 'permanente' (minúsculas)
    df_cultivos["tipo_ciclo"] = df_cultivos["tipo_ciclo"].astype(str).str.lower()
    df_cultivos.loc[~df_cultivos["tipo_ciclo"].isin(['transitorio','permanente']), "tipo_ciclo"] = None
    import numpy as np
    df_cultivos = df_cultivos.replace({np.nan: None, "nan": None, "None": None})
    
    load_dim_cultivo(engine, df_cultivos)

    # ── Paso 4: Limpieza y normalización ────────
    logger.info("Paso 4: Limpiando y normalizando...")
    df_produccion = agregar_id_municipio(df_produccion, col_nombre="municipio")
    nulos = df_produccion["id_municipio"].isna().sum()
    if nulos > 0:
        logger.warning(f"{nulos} registros de producción sin municipio resuelto")
        df_produccion = df_produccion.dropna(subset=["id_municipio"])

    # ── Paso 5: Hechos ──────────────────────────
    logger.info("Paso 5: Cargando hechos históricos...")
    load_all_facts(engine, df_produccion, df_boletines)

    # ── Paso 6: Validación de calidad ───────────
    logger.info("Paso 6: Validando calidad de datos...")
    reporte = run_quality_report(engine)
    alertas = reporte[reporte["estado"] == "ALERTA"]
    if not alertas.empty:
        logger.warning(f"ALERTAS DE CALIDAD:\n{alertas.to_string(index=False)}")
    else:
        logger.info("Todos los indicadores de calidad en estado OK")

    logger.info(f"FIN ETL — {datetime.now().isoformat()}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="Ejecutar el pipeline una sola vez y salir")
    args = parser.parse_args()

    if args.once:
        run_etl()
    else:
        from apscheduler.schedulers.blocking import BlockingScheduler
        scheduler = BlockingScheduler(timezone="America/Bogota")
        # Ejecutar todos los lunes a las 2 AM (hora Colombia)
        scheduler.add_job(run_etl, "cron", day_of_week="mon", hour=2, minute=0)
        logger.info("Scheduler activo — pipeline programado los lunes a las 02:00 (Bogotá)")
        logger.info("Ctrl+C para detener")
        try:
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("Scheduler detenido manualmente")
