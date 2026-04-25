-- ══════════════════════════════════════════════
--  AgroIA Colombia — DDL completo Star Schema
--  Motor: PostgreSQL (Supabase)
-- ══════════════════════════════════════════════

-- ── CAPA 1: DIMENSIONES ────────────────────────

CREATE TABLE IF NOT EXISTS dim_region_natural (
    id_region     SERIAL PRIMARY KEY,
    nombre_region VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_municipio (
    id_municipio        CHAR(5) PRIMARY KEY,        -- código DIVIPOLA 5 dígitos
    nombre_municipio    VARCHAR(100) NOT NULL,
    id_departamento     CHAR(2) NOT NULL,
    nombre_departamento VARCHAR(100) NOT NULL,
    id_region           INT REFERENCES dim_region_natural(id_region),
    latitud_centroide   DOUBLE PRECISION,
    longitud_centroide  DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo   SERIAL PRIMARY KEY,
    fecha       DATE NOT NULL UNIQUE,               -- primer día del mes
    anio        SMALLINT NOT NULL,
    mes         SMALLINT NOT NULL,
    trimestre   SMALLINT NOT NULL,
    semestre    CHAR(1) NOT NULL CHECK (semestre IN ('A','B')),
    nombre_mes  VARCHAR(20) NOT NULL,
    es_anio_nino BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_cultivo (
    id_cultivo        SERIAL PRIMARY KEY,
    nombre_cultivo    VARCHAR(100) NOT NULL,
    nombre_normalizado VARCHAR(100) NOT NULL UNIQUE,
    tipo_ciclo        VARCHAR(20) CHECK (tipo_ciclo IN ('transitorio','permanente')),
    familia_botanica  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_estacion_ideam (
    id_estacion     VARCHAR(20) PRIMARY KEY,
    nombre_estacion VARCHAR(150),
    tipo_estacion   VARCHAR(50),
    latitud         DOUBLE PRECISION,
    longitud        DOUBLE PRECISION,
    altitud_msnm    DOUBLE PRECISION,
    id_municipio    CHAR(5) REFERENCES dim_municipio(id_municipio),
    estado_activa   BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_central_abastos (
    id_central   SERIAL PRIMARY KEY,
    nombre_central VARCHAR(150) NOT NULL,
    ciudad         VARCHAR(100) NOT NULL,
    id_municipio   CHAR(5) REFERENCES dim_municipio(id_municipio),
    UNIQUE (nombre_central, ciudad)
);

-- ── CAPA 2: HECHOS HISTÓRICOS ─────────────────

CREATE TABLE IF NOT EXISTS fact_produccion_agricola (
    id               SERIAL PRIMARY KEY,
    id_municipio     CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    id_cultivo       INT     NOT NULL REFERENCES dim_cultivo(id_cultivo),
    id_tiempo        INT     NOT NULL REFERENCES dim_tiempo(id_tiempo),
    area_sembrada_ha       DOUBLE PRECISION,
    area_cosechada_ha      DOUBLE PRECISION,
    produccion_total_ton   DOUBLE PRECISION,
    rendimiento_t_ha       DOUBLE PRECISION,
    fuente_origen          VARCHAR(50),
    UNIQUE (id_municipio, id_cultivo, id_tiempo)
);

CREATE TABLE IF NOT EXISTS fact_clima_mensual (
    id               SERIAL PRIMARY KEY,
    id_estacion      VARCHAR(20) NOT NULL REFERENCES dim_estacion_ideam(id_estacion),
    id_municipio     CHAR(5)     NOT NULL REFERENCES dim_municipio(id_municipio),
    id_tiempo        INT         NOT NULL REFERENCES dim_tiempo(id_tiempo),
    precipitacion_mm         DOUBLE PRECISION,
    UNIQUE (id_estacion, id_tiempo)
);

CREATE TABLE IF NOT EXISTS fact_precios_mayoristas (
    id               SERIAL PRIMARY KEY,
    id_central       INT  NOT NULL REFERENCES dim_central_abastos(id_central),
    id_cultivo       INT  NOT NULL REFERENCES dim_cultivo(id_cultivo),
    id_tiempo        INT  NOT NULL REFERENCES dim_tiempo(id_tiempo),
    precio_min_cop_kg        DOUBLE PRECISION,
    precio_max_cop_kg        DOUBLE PRECISION,
    precio_promedio_cop_kg   DOUBLE PRECISION,
    volumen_abastecimiento_ton DOUBLE PRECISION,
    UNIQUE (id_central, id_cultivo, id_tiempo)
);

CREATE TABLE IF NOT EXISTS fact_aptitud_suelo (
    id               SERIAL PRIMARY KEY,
    id_municipio     CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    id_cultivo       INT REFERENCES dim_cultivo(id_cultivo),
    clase_aptitud    VARCHAR(20) CHECK (clase_aptitud IN ('alta','moderada','marginal','no_apta')),
    UNIQUE (id_municipio, id_cultivo)
);

CREATE TABLE IF NOT EXISTS fact_censo_agropecuario (
    id               SERIAL PRIMARY KEY,
    id_municipio     CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    anio_censo       SMALLINT NOT NULL,
    area_cultivos_permanentes_ha     DOUBLE PRECISION,
    area_cultivos_transitorios_ha    DOUBLE PRECISION,
    UNIQUE (id_municipio, anio_censo)
);

CREATE TABLE IF NOT EXISTS fact_alerta_enso (
    id               SERIAL PRIMARY KEY,
    id_tiempo        INT  NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_region        INT  NOT NULL REFERENCES dim_region_natural(id_region),
    fase_enso        VARCHAR(20), -- El Niño, La Niña, Neutro
    indice_spi       DOUBLE PRECISION,
    anomalia_precipitacion_pct DOUBLE PRECISION,
    probabilidad_deficit_hidrico DOUBLE PRECISION,
    probabilidad_exceso_hidrico  DOUBLE PRECISION,
    UNIQUE (id_tiempo, id_region)
);

CREATE TABLE IF NOT EXISTS fact_precios_insumos (
    id               SERIAL PRIMARY KEY,
    id_tiempo        INT  NOT NULL REFERENCES dim_tiempo(id_tiempo),
    tipo_insumo      VARCHAR(50),
    nombre_insumo    VARCHAR(100),
    precio_cop_unidad DOUBLE PRECISION,
    unidad_medida    VARCHAR(20),
    id_region        INT REFERENCES dim_region_natural(id_region),
    UNIQUE (id_tiempo, tipo_insumo, nombre_insumo)
);

-- ── CAPA 3: MODELO IA ─────────────────────────

CREATE TABLE IF NOT EXISTS model_version (
    id_version        SERIAL PRIMARY KEY,
    nombre_modelo     VARCHAR(100) NOT NULL,
    fecha_entrenamiento TIMESTAMPTZ DEFAULT NOW(),
    metricas_json     JSONB,
    activo            BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS pred_rendimiento (
    id               SERIAL PRIMARY KEY,
    id_municipio     CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    id_cultivo       INT     NOT NULL REFERENCES dim_cultivo(id_cultivo),
    id_tiempo        INT     NOT NULL REFERENCES dim_tiempo(id_tiempo),
    rendimiento_predicho_t_ha      DOUBLE PRECISION,
    intervalo_confianza_inferior   DOUBLE PRECISION,
    intervalo_confianza_superior   DOUBLE PRECISION,
    id_version       INT REFERENCES model_version(id_version)
);

CREATE TABLE IF NOT EXISTS pred_alerta_climatica (
    id               SERIAL PRIMARY KEY,
    id_municipio     CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    id_tiempo        INT     NOT NULL REFERENCES dim_tiempo(id_tiempo),
    nivel_riesgo     VARCHAR(10) CHECK (nivel_riesgo IN ('BAJO','MEDIO','ALTO')),
    tipo_evento      VARCHAR(30),
    score_probabilidad DOUBLE PRECISION,
    descripcion_generada TEXT,
    activa           BOOLEAN DEFAULT TRUE,
    id_version       INT REFERENCES model_version(id_version)
);

-- ── CAPA 4: VISTAS DASHBOARD ───────────────────

DROP VIEW IF EXISTS v_dashboard_agro CASCADE;

CREATE OR REPLACE VIEW v_dashboard_agro AS
WITH clima_anual AS (
    SELECT fc.id_municipio,
        tc.anio,
        avg(fc.precipitacion_mm) AS precipitacion_mm
    FROM fact_clima_mensual fc
    JOIN dim_tiempo tc ON tc.id_tiempo = fc.id_tiempo
    GROUP BY fc.id_municipio, tc.anio
)
SELECT m.id_municipio AS codigo_divipola,
    m.nombre_municipio,
    m.nombre_departamento,
    m.latitud_centroide,
    m.longitud_centroide,
    c.nombre_cultivo,
    t.anio,
    fp.area_sembrada_ha,
    fp.area_cosechada_ha,
    fp.produccion_total_ton,
    fp.rendimiento_t_ha,
    ca.precipitacion_mm
FROM fact_produccion_agricola fp
JOIN dim_municipio m ON m.id_municipio = fp.id_municipio
JOIN dim_cultivo c ON c.id_cultivo = fp.id_cultivo
JOIN dim_tiempo t ON t.id_tiempo = fp.id_tiempo
LEFT JOIN clima_anual ca ON ca.id_municipio = fp.id_municipio AND ca.anio = t.anio;
