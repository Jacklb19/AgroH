"""
run_pipeline.py — Orquestador principal del ETL AgroIA Colombia
Ejecutar manualmente:   python run_pipeline.py --once
Ejecutar con scheduler: python run_pipeline.py
"""
import argparse
import logging
import sys
import pandas as pd
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


def run_core_etl(engine=None):
    logger.info("=" * 60)
    logger.info(f"INICIO ETL CORE — {datetime.now().isoformat()}")
    logger.info("=" * 60)

    from extract.extract_divipola    import extract_divipola
    from extract.extract_produccion  import extract_produccion

    from clean.clean_municipios      import (
        agregar_id_municipio,
        asignar_estaciones_a_municipios,
        build_region_map_from_divipola,
    )
    from clean.clean_clima           import unificar_clima_mensual

    from load.db                     import get_engine, init_schema
    from load.load_dimensions        import (
        load_dim_region_natural, load_dim_tiempo,
        load_dim_municipio, load_dim_cultivo,
        load_dim_estacion_ideam
    )
    from load.load_facts             import load_all_facts, load_fact_clima_mensual
    from validate.quality_report     import run_quality_report

    if engine is None:
        engine = get_engine()

    # ── Paso 1: Schema ──────────────────────────
    logger.info("Paso 1: Inicializando schema...")
    init_schema(engine)

    # ── Paso 2: Extracción ──────────────────────
    logger.info("Paso 2: Extrayendo fuentes...")
    df_divipola    = extract_divipola()
    df_produccion  = extract_produccion()
    
    # Normalizar nombres de columnas para el nuevo dataset (Base 2019-2024)
    rename_map = {
        "a_o": "anio",
        "rea_sembrada": "area_sembrada_ha",
        "rea_cosechada": "area_cosechada_ha",
        "producci_n": "produccion_total_ton",
        "rendimiento": "rendimiento_t_ha",
        "grupo_cultivo": "grupo_de_cultivo",
        "ciclo_del_cultivo": "ciclo_de_cultivo",
        "c_digo_dane_municipio": "id_municipio"
    }
    df_produccion = df_produccion.rename(columns=rename_map)
    
    # Asegurar tipos numéricos (vienen como strings en el JSON)
    num_cols = ["anio", "area_sembrada_ha", "area_cosechada_ha", "produccion_total_ton", "rendimiento_t_ha"]
    for col in num_cols:
        if col in df_produccion.columns:
            df_produccion[col] = pd.to_numeric(df_produccion[col], errors="coerce")
    
    # Rellenar ceros en lugar de NaNs para áreas y producción
    df_produccion[num_cols[1:]] = df_produccion[num_cols[1:]].fillna(0)

    from extract.extract_ideam_estaciones import extract_estaciones
    df_estaciones = extract_estaciones()

    from extract.extract_ideam_clima import extract_all_clima
    df_precip_mensual, df_combinado_mensual = extract_all_clima()

    # ── Paso 3: Dimensiones ─────────────────────
    logger.info("Paso 3: Cargando dimensiones...")
    load_dim_region_natural(engine)
    load_dim_tiempo(engine)
    df_region_map = build_region_map_from_divipola(df_divipola)
    load_dim_municipio(engine, df_divipola, df_region_map=df_region_map)
    
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
    
    # Si ya tenemos id_municipio (del código DANE), aseguramos 5 dígitos
    if "id_municipio" in df_produccion.columns:
        df_produccion["id_municipio"] = df_produccion["id_municipio"].astype(str).str.zfill(5)
    else:
        df_produccion = agregar_id_municipio(df_produccion, col_nombre="municipio")
        
    df_estaciones = asignar_estaciones_a_municipios(df_estaciones, df_divipola, fallback_col="municipio")
    
    nulos = df_produccion["id_municipio"].isna().sum()
    if nulos > 0:
        logger.warning(f"{nulos} registros de producción sin municipio resuelto")
        
    # Cargar dimensión de estaciones (requiere id_municipio)
    from load.load_dimensions import load_dim_estacion_ideam
    df_estaciones_dim = df_estaciones.rename(columns={
        "codigo": "id_estacion",
        "nombre": "nombre_estacion",
        "categoria": "tipo_estacion",
        "latitud": "latitud",
        "longitud": "longitud",
        "altitud": "altitud_msnm"
    })
    # Asegurar tipos correctos para la base de datos
    df_estaciones_dim["latitud"] = pd.to_numeric(df_estaciones_dim["latitud"], errors="coerce")
    df_estaciones_dim["longitud"] = pd.to_numeric(df_estaciones_dim["longitud"], errors="coerce")
    df_estaciones_dim["altitud_msnm"] = pd.to_numeric(df_estaciones_dim["altitud_msnm"], errors="coerce")
    df_estaciones_dim["estado_activa"] = df_estaciones_dim["estado"].astype(str).str.upper() == "ACTIVA"
    
    # Solo tomamos las columnas del esquema
    cols_estacion = ["id_estacion", "nombre_estacion", "tipo_estacion", "latitud", "longitud", "altitud_msnm", "id_municipio", "estado_activa"]
    df_estaciones_dim = df_estaciones_dim[cols_estacion].dropna(subset=["id_estacion"])
    df_estaciones_dim = df_estaciones_dim.replace({np.nan: None})
    load_dim_estacion_ideam(engine, df_estaciones_dim)
    df_produccion = df_produccion.dropna(subset=["id_municipio"])

    # ── Paso 5: Hechos de producción ─────────────
    logger.info("Paso 5: Cargando hechos de producción...")
    load_all_facts(engine, df_produccion, pd.DataFrame())

    # ── Paso 6: Hechos climáticos ────────────────
    logger.info("Paso 6: Unificando clima a granularidad mensual...")
    df_clima_mensual = unificar_clima_mensual(df_precip_mensual, df_combinado_mensual)
    if not df_clima_mensual.empty:
        load_fact_clima_mensual(engine, df_clima_mensual)
    else:
        logger.info("Sin datos climáticos por ahora — se poblarán en futuras ejecuciones")

    # ── Paso 7: Validación de calidad ───────────
    logger.info("Paso 7: Validando calidad de datos...")
    reporte = run_quality_report(engine)
    alertas = reporte[reporte["estado"] == "ALERTA"]
    if not alertas.empty:
        logger.warning(f"ALERTAS DE CALIDAD:\n{alertas.to_string(index=False)}")
    else:
        logger.info("Todos los indicadores de calidad en estado OK")

    logger.info(f"FIN ETL CORE — {datetime.now().isoformat()}")
    logger.info("=" * 60)
    return {
        "df_divipola": df_divipola,
        "df_produccion": df_produccion,
        "df_estaciones": df_estaciones,
        "df_clima_mensual": df_clima_mensual,
        "quality_report": reporte,
    }


def run_extended_etl(engine=None):
    logger.info("INICIO ETL EXTENDIDO — %s", datetime.now().isoformat())

    from load.db import get_engine
    from extract.extract_ideam_pdf import extract_all_boletines
    from extract.extract_sipsa import extract_sipsa
    from extract.extract_sipra import extract_sipra
    from clean.clean_precios import normalizar_precios_sipsa, construir_dim_centrales
    from clean.clean_suelo import resumir_aptitud_suelo_por_municipio, load_censo_agropecuario_local
    from load.load_dimensions import load_dim_central_abastos
    from load.load_facts import (
        load_fact_alerta_enso,
        load_fact_precios_mayoristas,
        load_fact_aptitud_suelo,
        load_fact_censo_agropecuario,
    )
    from extract.extract_divipola import extract_divipola

    if engine is None:
        engine = get_engine()

    df_boletines = extract_all_boletines()
    if not df_boletines.empty:
        load_fact_alerta_enso(engine, df_boletines)
    else:
        logger.info("ENSO: sin boletines configurados o detectados")

    df_sipsa_raw = extract_sipsa()
    df_precios = normalizar_precios_sipsa(df_sipsa_raw)
    if not df_precios.empty:
        df_centrales = construir_dim_centrales(df_precios)
        if not df_centrales.empty:
            load_dim_central_abastos(engine, df_centrales)
        load_fact_precios_mayoristas(engine, df_precios)
    else:
        logger.info("SIPSA: sin archivos manuales configurados")

    df_divipola = extract_divipola()
    df_sipra = extract_sipra()
    df_suelo = resumir_aptitud_suelo_por_municipio(df_sipra, df_divipola)
    if not df_suelo.empty:
        load_fact_aptitud_suelo(engine, df_suelo)
    else:
        logger.info("SIPRA: sin capas suficientes para generar fact_aptitud_suelo")

    df_censo = load_censo_agropecuario_local()
    if not df_censo.empty:
        load_fact_censo_agropecuario(engine, df_censo)
    else:
        logger.info("CNA: sin archivo manual configurado")

    logger.info("FIN ETL EXTENDIDO — %s", datetime.now().isoformat())


def run_etl(mode: str = "all"):
    from load.db import get_engine

    engine = get_engine()
    result = None
    if mode in {"core", "all"}:
        result = run_core_etl(engine=engine)
    if mode in {"extended", "all"}:
        run_extended_etl(engine=engine)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="Ejecutar el pipeline una sola vez y salir")
    parser.add_argument(
        "--mode",
        choices=["core", "extended", "all"],
        default="all",
        help="Selecciona qué parte del pipeline ejecutar",
    )
    args = parser.parse_args()

    if args.once:
        run_etl(mode=args.mode)
    else:
        from apscheduler.schedulers.blocking import BlockingScheduler
        scheduler = BlockingScheduler(timezone="America/Bogota")
        # Ejecutar todos los lunes a las 2 AM (hora Colombia)
        scheduler.add_job(run_etl, "cron", day_of_week="mon", hour=2, minute=0, kwargs={"mode": "core"})
        logger.info("Scheduler activo — pipeline CORE programado los lunes a las 02:00 (Bogotá)")
        logger.info("Ctrl+C para detener")
        try:
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("Scheduler detenido manualmente")
