import pandas as pd
import logging
import unicodedata

logger = logging.getLogger(__name__)


def _normalizar_region(region: str) -> str:
    if not isinstance(region, str):
        return ""
    region = unicodedata.normalize("NFD", region.strip().lower())
    region = "".join(c for c in region if unicodedata.category(c) != "Mn")
    return region

def load_all_facts(engine, df_produccion: pd.DataFrame, df_boletines: pd.DataFrame):
    """
    Carga los hechos históricos en la base de datos.
    """
    from .db import upsert
    logger.info("load_all_facts: Cargando fact_produccion_agricola...")
    
    # 1. Recuperar id_cultivo y id_tiempo de la base de datos
    dim_cultivo_db = pd.read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)
    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio FROM dim_tiempo WHERE mes = 12", engine)
    
    # 2. Unir df_produccion con dim_cultivo y dim_tiempo
    df_produccion["nombre_normalizado"] = df_produccion["cultivo"].astype(str).str.upper().str.strip()
    df_produccion["anio"] = df_produccion["a_o"].astype(int)
    
    df_merged = df_produccion.merge(dim_cultivo_db, on="nombre_normalizado", how="inner")
    df_merged = df_merged.merge(dim_tiempo_db, on="anio", how="inner")
    
    # 3. Preparar el DataFrame para la tabla de hechos
    fact_cols = {
        "id_municipio": df_merged["id_municipio"],
        "id_cultivo": df_merged["id_cultivo"],
        "id_tiempo": df_merged["id_tiempo"],
        "area_sembrada_ha": df_merged["rea_sembrada_ha"],
        "area_cosechada_ha": df_merged["rea_cosechada_ha"],
        "produccion_total_ton": df_merged["producci_n_t"],
        "rendimiento_t_ha": df_merged["rendimiento_t_ha"],
        "fuente_origen": "MinAgricultura A04/A05"
    }
    df_fact = pd.DataFrame(fact_cols)
    
    # Limpiar nulos y asegurar tipos correctos
    df_fact = df_fact.dropna(subset=["id_municipio", "id_cultivo", "id_tiempo"])
    for col in ["area_sembrada_ha", "area_cosechada_ha", "produccion_total_ton", "rendimiento_t_ha"]:
        df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce').fillna(0)
        
    # Agrupar por llaves primarias en caso de duplicados en la fuente
    df_fact = df_fact.groupby(["id_municipio", "id_cultivo", "id_tiempo", "fuente_origen"]).sum().reset_index()

    # 4. Upsert a fact_produccion_agricola
    upsert(engine, "fact_produccion_agricola", df_fact, ["id_municipio", "id_cultivo", "id_tiempo"])
    logger.info(f"fact_produccion_agricola: {len(df_fact)} registros cargados")


def load_fact_clima_mensual(engine, df_clima_mensual: pd.DataFrame):
    """
    Carga fact_clima_mensual cruzando con dim_tiempo y dim_estacion_ideam.
    df_clima_mensual debe tener: id_estacion, anio, mes, precipitacion_mm, etc.
    """
    from .db import upsert

    if df_clima_mensual.empty:
        logger.warning("Sin datos climáticos mensuales para cargar")
        return

    logger.info(f"load_fact_clima_mensual: procesando {len(df_clima_mensual)} registros...")

    # 1. Recuperar id_tiempo (fecha = primer día del mes)
    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)

    # 2. Recuperar estaciones con su id_municipio
    dim_est_db = pd.read_sql(
        "SELECT id_estacion, id_municipio FROM dim_estacion_ideam WHERE id_municipio IS NOT NULL",
        engine
    )

    # 3. Join con dim_tiempo
    df_clima_mensual["anio"] = df_clima_mensual["anio"].astype(int)
    df_clima_mensual["mes"] = df_clima_mensual["mes"].astype(int)
    df = df_clima_mensual.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")

    # 4. Join con dim_estacion_ideam para obtener id_municipio
    df["id_estacion"] = df["id_estacion"].astype(str)
    dim_est_db["id_estacion"] = dim_est_db["id_estacion"].astype(str)
    df = df.merge(dim_est_db, on="id_estacion", how="inner")

    # 5. Seleccionar columnas del schema
    cols_fact = [
        "id_estacion", "id_municipio", "id_tiempo",
        "precipitacion_mm", "temperatura_media_c", "temperatura_max_c",
        "temperatura_min_c", "humedad_relativa_pct", "brillo_solar_horas_dia"
    ]
    # Asegurar que existen todas las columnas (pueden faltar si no hubo datos de ese sensor)
    for c in cols_fact:
        if c not in df.columns:
            df[c] = None

    df_fact = df[cols_fact].copy()
    df_fact = df_fact.dropna(subset=["id_estacion", "id_municipio", "id_tiempo"])

    import numpy as np
    df_fact = df_fact.replace({np.nan: None})

    if df_fact.empty:
        logger.warning("Tras filtrar, no quedan registros de clima para cargar")
        return

    logger.info(f"  Insertando {len(df_fact)} registros en fact_clima_mensual...")
    upsert(engine, "fact_clima_mensual", df_fact, ["id_estacion", "id_tiempo"])
    logger.info(f"fact_clima_mensual: {len(df_fact)} registros cargados")


def load_fact_alerta_enso(engine, df_boletines: pd.DataFrame):
    """Carga alertas ENSO trimestrales, distribuyéndolas en los tres meses del trimestre."""
    from .db import upsert

    if df_boletines.empty:
        logger.info("Sin boletines ENSO para cargar")
        return

    df = df_boletines.copy()
    df = df[df["anio"].notna() & df["trimestre"].notna() & df["region"].notna()]
    if df.empty:
        logger.warning("Los boletines ENSO no contienen anio/trimestre/region válidos")
        return

    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["trimestre_num"] = (
        df["trimestre"].astype(str).str.extract(r"(\d+)")[0].astype("Int64")
    )
    df = df.dropna(subset=["anio", "trimestre_num"])
    if df.empty:
        logger.warning("No se pudo inferir el trimestre de los boletines ENSO")
        return

    dim_region_db = pd.read_sql(
        "SELECT id_region, nombre_region FROM dim_region_natural",
        engine,
    )
    dim_region_db["region_norm"] = dim_region_db["nombre_region"].apply(_normalizar_region)
    df["region_norm"] = df["region"].apply(_normalizar_region)
    df = df.merge(dim_region_db[["id_region", "region_norm"]], on="region_norm", how="left")
    df = df.dropna(subset=["id_region"])

    expanded_rows = []
    for row in df.to_dict(orient="records"):
        trimestre_num = int(row["trimestre_num"])
        meses = range((trimestre_num - 1) * 3 + 1, (trimestre_num - 1) * 3 + 4)
        for mes in meses:
            expanded_rows.append({
                "anio": int(row["anio"]),
                "mes": mes,
                "id_region": int(row["id_region"]),
                "fase_enso": row.get("fase_enso"),
                "indice_spi": row.get("indice_spi"),
                "anomalia_precipitacion_pct": row.get("anomalia_precipitacion_pct"),
                "probabilidad_deficit_hidrico": row.get("probabilidad_deficit_hidrico"),
                "probabilidad_exceso_hidrico": row.get("probabilidad_exceso_hidrico"),
            })

    df_expanded = pd.DataFrame(expanded_rows)
    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)
    df_expanded = df_expanded.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")
    if df_expanded.empty:
        logger.warning("No hay periodos válidos en dim_tiempo para cargar ENSO")
        return

    cols = [
        "id_tiempo",
        "id_region",
        "fase_enso",
        "indice_spi",
        "anomalia_precipitacion_pct",
        "probabilidad_deficit_hidrico",
        "probabilidad_exceso_hidrico",
    ]
    df_fact = df_expanded[cols].drop_duplicates(subset=["id_tiempo", "id_region"])
    upsert(engine, "fact_alerta_enso", df_fact, ["id_tiempo", "id_region"])
    logger.info("fact_alerta_enso: %s registros cargados", len(df_fact))


def load_fact_precios_mayoristas(engine, df_precios: pd.DataFrame):
    """Carga precios mayoristas mensuales cuando ya existe un DataFrame normalizado."""
    from .db import upsert

    if df_precios.empty:
        logger.info("Sin precios mayoristas para cargar")
        return

    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)
    dim_cultivo_db = pd.read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)
    dim_central_db = pd.read_sql(
        "SELECT id_central, nombre_central, ciudad FROM dim_central_abastos",
        engine,
    )

    df = df_precios.copy()
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    df["nombre_normalizado"] = df["producto"].astype(str).str.upper().str.strip()
    df["nombre_central"] = df["nombre_central"].astype(str).str.strip()
    df["ciudad"] = df["ciudad"].astype(str).str.strip()

    df = df.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")
    df = df.merge(dim_cultivo_db, on="nombre_normalizado", how="inner")
    df = df.merge(dim_central_db, on=["nombre_central", "ciudad"], how="inner")
    if df.empty:
        logger.warning("No fue posible mapear precios a tiempo/cultivo/central")
        return

    cols = [
        "id_central",
        "id_cultivo",
        "id_tiempo",
        "precio_min_cop_kg",
        "precio_max_cop_kg",
        "precio_promedio_cop_kg",
        "volumen_abastecimiento_ton",
    ]
    df_fact = df[cols].copy()
    upsert(engine, "fact_precios_mayoristas", df_fact, ["id_central", "id_cultivo", "id_tiempo"])
    logger.info("fact_precios_mayoristas: %s registros cargados", len(df_fact))


def load_fact_aptitud_suelo(engine, df_suelo: pd.DataFrame):
    from .db import upsert

    if df_suelo.empty:
        logger.info("Sin aptitud de suelo para cargar")
        return

    dim_cultivo_db = pd.read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)
    df = df_suelo.copy()
    if "producto" in df.columns:
        df["nombre_normalizado"] = df["producto"].astype(str).str.upper().str.strip()
        df = df.merge(dim_cultivo_db, on="nombre_normalizado", how="left")
    if "id_cultivo" not in df.columns:
        df["id_cultivo"] = None

    cols = [
        "id_municipio",
        "id_cultivo",
        "clase_aptitud",
        "tipo_suelo",
        "textura_suelo",
        "pendiente_dominante",
        "drenaje",
        "limitante_principal",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = None

    df_fact = df[cols].dropna(subset=["id_municipio"]).drop_duplicates(
        subset=["id_municipio", "id_cultivo"],
        keep="first",
    )
    upsert(engine, "fact_aptitud_suelo", df_fact, ["id_municipio", "id_cultivo"])
    logger.info("fact_aptitud_suelo: %s registros cargados", len(df_fact))


def load_fact_censo_agropecuario(engine, df_censo: pd.DataFrame):
    from .db import upsert

    if df_censo.empty:
        logger.info("Sin censo agropecuario para cargar")
        return

    df = df_censo.copy()
    df["anio_censo"] = pd.to_numeric(df.get("anio_censo", 2014), errors="coerce").fillna(2014).astype(int)
    cols = [
        "id_municipio",
        "anio_censo",
        "upa_promedio_ha",
        "pct_tenencia_propia",
        "pct_tenencia_arrendada",
        "pct_acceso_riego",
        "pct_asistencia_tecnica",
        "area_cultivos_permanentes_ha",
        "area_cultivos_transitorios_ha",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = None

    df_fact = df[cols].dropna(subset=["id_municipio"])
    upsert(engine, "fact_censo_agropecuario", df_fact, ["id_municipio", "anio_censo"])
    logger.info("fact_censo_agropecuario: %s registros cargados", len(df_fact))


def load_fact_precios_insumos(engine, df_insumos: pd.DataFrame):
    """
    Carga fact_precios_insumos desde el DataFrame normalizado de insumos A07.
    df_insumos debe tener: anio, mes, tipo_insumo, nombre_insumo,
                           precio_cop_unidad, unidad_medida, region.
    """
    from .db import upsert

    if df_insumos.empty:
        logger.info("Sin datos de insumos para cargar")
        return

    # Unir con dim_tiempo
    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)
    df = df_insumos.copy()
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["mes"]  = pd.to_numeric(df["mes"],  errors="coerce").astype("Int64")
    df = df.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")

    if df.empty:
        logger.warning("Insumos: no se pudo mapear ningún registro a dim_tiempo")
        return

    # Unir con dim_region_natural si hay columna region
    if "region" in df.columns and df["region"].notna().any():
        dim_region_db = pd.read_sql(
            "SELECT id_region, nombre_region FROM dim_region_natural", engine
        )
        # Normalización básica para el join
        import unicodedata

        def _norm(s):
            if not isinstance(s, str):
                return ""
            s = unicodedata.normalize("NFD", s.strip().lower())
            return "".join(c for c in s if unicodedata.category(c) != "Mn")

        dim_region_db["region_norm"] = dim_region_db["nombre_region"].apply(_norm)
        df["region_norm"] = df["region"].apply(_norm)
        df = df.merge(dim_region_db[["id_region", "region_norm"]], on="region_norm", how="left")
    else:
        df["id_region"] = None

    cols = [
        "id_tiempo",
        "id_region",
        "tipo_insumo",
        "nombre_insumo",
        "precio_cop_unidad",
        "unidad_medida",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df_fact = (
        df[cols]
        .dropna(subset=["id_tiempo", "tipo_insumo", "nombre_insumo"])
        .drop_duplicates(subset=["id_tiempo", "tipo_insumo", "nombre_insumo"])
    )
    # NaN in nullable integer column → must be Python None for psycopg2
    df_fact = df_fact.copy()
    df_fact["id_region"] = df_fact["id_region"].apply(lambda x: None if pd.isna(x) else x)
    upsert(engine, "fact_precios_insumos", df_fact, ["id_tiempo", "tipo_insumo", "nombre_insumo"])
    logger.info("fact_precios_insumos: %s registros cargados", len(df_fact))


def load_raw_precios_mayoristas(engine, df_sipsa_raw: pd.DataFrame):
    """
    Persiste los microdatos SIPSA en granularidad ORIGINAL (diaria/semanal)
    en la tabla raw_precios_mayoristas, antes de la agregación mensual.

    df_sipsa_raw debe tener las mismas columnas que genera normalizar_precios_sipsa()
    más la columna fecha_registro con la fecha exacta del precio.
    """
    from .db import upsert

    if df_sipsa_raw.empty:
        logger.info("Sin microdatos SIPSA raw para cargar")
        return

    df = df_sipsa_raw.copy()

    # Resolver id_central desde dim_central_abastos
    if "nombre_central" in df.columns and "ciudad" in df.columns:
        dim_central_db = pd.read_sql(
            "SELECT id_central, nombre_central, ciudad FROM dim_central_abastos", engine
        )
        df["nombre_central"] = df["nombre_central"].astype(str).str.strip()
        df["ciudad"]         = df["ciudad"].astype(str).str.strip()
        df = df.merge(dim_central_db, on=["nombre_central", "ciudad"], how="left")

    # Resolver id_cultivo desde dim_cultivo
    if "producto" in df.columns:
        dim_cultivo_db = pd.read_sql(
            "SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine
        )
        df["nombre_normalizado"] = df["producto"].astype(str).str.upper().str.strip()
        df = df.merge(dim_cultivo_db, on="nombre_normalizado", how="left")

    # Asegurar fecha_registro como DATE
    if "fecha_registro" not in df.columns and "fecha" in df.columns:
        df["fecha_registro"] = pd.to_datetime(df["fecha"], errors="coerce")
    else:
        df["fecha_registro"] = pd.to_datetime(df.get("fecha_registro"), errors="coerce")

    df = df.dropna(subset=["fecha_registro"])
    df["fecha_registro"] = df["fecha_registro"].dt.date

    cols = [
        "id_central",
        "id_cultivo",
        "fecha_registro",
        "precio_min_cop_kg",
        "precio_max_cop_kg",
        "precio_promedio_cop_kg",
        "volumen_abastecimiento_ton",
        "unidad_empaque",
    ]
    # Alias tolerantes
    if "volumen_abastecimiento_ton" not in df.columns and "volumen_ton" in df.columns:
        df["volumen_abastecimiento_ton"] = df["volumen_ton"]

    for c in cols:
        if c not in df.columns:
            df[c] = None

    df_raw = df[cols].dropna(subset=["fecha_registro"])
    if df_raw.empty:
        logger.warning("raw_precios_mayoristas: sin registros válidos para insertar")
        return

    # NaN in nullable integer/float columns → None for psycopg2
    df_raw = df_raw.copy()
    for _col in ["id_central", "id_cultivo", "precio_min_cop_kg", "precio_max_cop_kg",
                 "precio_promedio_cop_kg", "volumen_abastecimiento_ton"]:
        if _col in df_raw.columns:
            df_raw[_col] = df_raw[_col].apply(lambda x: None if pd.isna(x) else x)

    # Esta tabla no tiene UNIQUE constraint; se hace INSERT simple (no upsert)
    with engine.begin() as conn:
        from sqlalchemy import text
        col_str = ", ".join(cols)
        ph_str  = ", ".join([f":{c}" for c in cols])
        conn.execute(
            text(f"INSERT INTO raw_precios_mayoristas ({col_str}) VALUES ({ph_str})"),
            df_raw.to_dict(orient="records"),
        )
    logger.info("raw_precios_mayoristas: %s microdatos insertados", len(df_raw))
